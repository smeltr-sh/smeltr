# Data paths

Every path a record takes through a worker, from the poll to the commit. Five stages carry it ([runtime stages](architecture/runtime-stages.md)): the source, which receives; its reader, which tracks each batch and commits the oldest run of batches every destination has acknowledged; the transform pool, which decodes, routes, runs pipelines and encodes; egress, one thread per destination behind a bounded queue; and the destination, which sends. Eight things can happen to a record on the way, and each one commits differently.

| path | where the records go | what they carry | the commit waits for |
|---|---|---|---|
| success | the destinations the route's deliver rules pick | the pipeline's record, in the format each destination writes | every destination's acknowledgment |
| unmatched | by the `Unmatched` answer, the group's for a record no route took and the route's over it for one no rule took: blocked by default, dropped, or delivered to every destination named | in each destination's format, the record object as it arrived, or the pipeline's record for one no rule took | under block, a new config; otherwise every destination's acknowledgment, the ones named included |
| pipeline exception | by the answer to each kind, the route's over the group's: tagged and placed by the deliver rules, delivered, blocked, or dropped | the pipeline's record, with its tag field, in each destination's format | under block, a new config; otherwise every destination's acknowledgment |
| unprocessable | by the `Unprocessable` answer, once a pipeline panics on the batch: blocked by default, dropped, or delivered to every destination named | in each destination's format, the record object as it arrived | under block, a new config; under deliver, the acknowledgment of every destination named, and of every target a refusal reaches |
| over budget | one record that will not fit alone meets the `Unprocessable` answer; under block the whole batch is held, otherwise the rest of the batch goes on | in each destination's format, the record object as it arrived | under block, a new config; otherwise every destination's acknowledgment |
| dropped | nowhere: a `Drop` step took the record | | every destination's acknowledgment, for the rest of the batch |
| refused | by the `Refused` answer, the destination's over the group's: kept and sent again, delivered to its targets, or dropped | the record, in the format each target writes | under block, the destination taking the refused records; under deliver, every target's acknowledgment; under drop, the acknowledgment that carries the refusal |
| unreachable | retried by egress until it is delivered | the same delivery unit again | holds until it is delivered |

Function names below are the code's own (`runtime.rs`, `egress.rs`, `commit.rs`).

## Success

The tracker records a batch when it is polled, waiting on every destination the source can reach. It commits the oldest run of batches every one of them has acknowledged, so a batch that finishes early waits for its elders.

```mermaid
sequenceDiagram
  autonumber
  participant S as Source
  participant R as Reader and commit tracker
  participant H as Handoff queue, one per source
  participant T as Transform thread
  participant Q as Egress queue, one per destination
  participant E as Egress thread, one per destination
  participant D as Destination
  R->>S: poll_batch
  S-->>R: records
  R->>R: track the batch id and offsets, waiting on every reachable destination
  R->>H: push the batch
  H->>T: take the batch
  T->>T: decode: each payload into its record
  T->>T: route_masks: which routes match each row
  T->>T: per route, run the pipeline, encode, apply the deliver rules
  T->>T: keep each destination's records for the turn
  T->>Q: once every route has run, deliver hands each destination its records as one unit
  Q->>E: take the unit
  E->>D: deliver the unit
  D-->>E: acknowledged
  E-->>T: Done for the batch, with any refusals
  T-->>R: Acked for the batch and the destination
  R->>R: every destination acked? retire the oldest run
  R->>S: commit the watermark
```

- A destination the batch has nothing for answers at once from `deliver` and is acknowledged in the same turn.
- Nothing leaves before the turn ends clean. A block, a memory refusal or a panic drops the turn's records with the turn, and the batch is held or run again whole.
- The handoff queue is bounded in bytes, and one batch larger than the bound goes in alone; each egress queue is bounded in bytes too. An Elasticsearch destination's queue holds up to its whole `bufferBytes`, as much as its pacing asks; a Kafka destination's up to half, and the producer's own queue the other half. A full egress queue refuses the unit at once, and the batch waits on its lane (Unreachable, below).
- Egress keeps each unit until it is acknowledged. The reader holds offsets, and at most one polled batch the handoff queue had no room for.
- A partition the group moves to another member commits nothing from the batches read before the move, however they finish: the source reports it revoked and the tracker forgets it. The new owner reads from the last committed offset and may already be past them, so a late commit would move it back. A record the source held from that partition is dropped.
- A commit the source refuses is counted as `smeltr_commits_dropped_total` and reading goes on: the batches are already retired, the next watermark for the same partition covers their offsets, and if none lands before the worker stops they replay from the last commit that did.

## Unmatched

No route took the record. Under block, the batch stops before any pipeline runs and is kept until a new config is deployed. Under deliver, each unmatched destination is one of the batch's destinations, so the commit waits for its acknowledgment like any other.

A record its route took and no delivery rule did meets the same answer after its pipeline, the route's over the group's, and counts in `smeltr_rows_unmatched_total` under cause `rule`, as a record no route took counts under cause `route`. Under block, the batch is held as for a marked record (Pipeline exception). Under deliver, it goes as the pipeline left it, placed like any delivered record. Under drop, it is counted under reason `unmatched`.

```mermaid
flowchart TD
  A["Transform has routed the batch"] --> B{"Did any route take this record?"}
  B -- yes --> P["Pipeline path, as in Success"]
  B -- no --> D{"The Unmatched answer"}
  D -- block --> Q["Stop before any pipeline runs. Block the source, keep the batch"]
  Q --> R["A redeploy re-reads it from the last commit"]
  D -- drop --> C["Count smeltr_rows_unmatched_total, cause route"]
  C --> X["Drop. Count smeltr_rows_dropped_total, reason unmatched"]
  D -- deliver --> C2["Count smeltr_rows_unmatched_total, cause route"]
  C2 --> F["place_unmatched: to every destination named, in its own format, no pipeline run"]
  F --> G["Handed over with the rest of the batch's records, one unit per destination"]
  G --> H["Each destination acknowledges"]
  H --> K["Commit once every destination has acknowledged"]
  X --> K2["The commit goes on without it"]
```

- A block here, or one on a marked record (Pipeline exception), holds the batch: the pool passes the source over and never re-runs it, since the same records meet the same routes and fail the same way. A destination's own block is re-run instead, because the destination can recover by itself.
- A destination that writes the record gets the record object, the document a pipeline with no steps would write: `{"_raw": ...}` for a record decoded from text, `{}` for one whose source's `encoding` is `none`, a typed record's fields otherwise. One that writes a field as text gets that field's text, the decoded text for `_raw` and base64 for a `bytes` field. One that writes bytes (`format: "bytes"`) gets one `bytes` field of each record, `__source.bytes` unless it names another: the payload as the source read it, before decoding. A text destination naming a field the record lacks refuses the record.
- No pipeline ran, so there is no `__destination.id`: a destination that reads it sends the document with no `_id`, and Elasticsearch generates one.

## Pipeline exception

A record's exceptions are its exceptions, each of a kind in one category. The `exceptions` setting, the route's over the group's, gives each kind an answer, and the record takes the strongest of its exceptions' answers, after the pipeline's last step and before any deliver rule runs.

```mermaid
flowchart TD
  A["The route's pipeline runs"] --> B{"The record's strongest answer"}
  B -- "ignore, or no mark" --> N["The deliver rules pick destinations"]
  B -- tag --> T["Set the tag field. The record is placed like any other"]
  T --> N
  B -- drop --> D["Count smeltr_rows_dropped_total, reason exception"]
  B -- deliver --> V["Set the tag field. Send it to each destination named, in its own format, in place of the deliver rules"]
  V --> X
  B -- block --> Q["Encode nothing. Block the source, keep the batch"]
  Q --> R["A redeploy re-reads it from the last commit"]
  N --> X["Hand over, acknowledge and commit as in Success"]
  D --> X2["The commit goes on without it"]
```

- The tag field is one column past the plan's own (`Plan::tag_slot`), a variant holding the route, the pipeline and each exception's step, kind and message (`output::tag_type`), set on every row with an exception not ignored and null on every other. A deliver rule reads it by name, and on an untagged row reads what the row holds there, so `smeltr.exception == null` takes a healthy row whose record has no such field, and the encoder writes it beside the pipeline's fields, so a dotted name nests with them. Where a dynamic field claims the same name, or a parent of it, the tag wins on every row it is set on.
- A pipeline run through `call` is inlined; it has no answer of its own, so the route's holds.

## Unprocessable

A pipeline panicked on the batch. The same records would meet the same pipelines again, so there is no second attempt: the `Unprocessable` answer takes it. Under deliver, `Abandoned` narrows the batch's waiting list to the destinations named, so it commits once they acknowledge it. A target one of them refuses a record into is added back as it is handed the record (Refused, below).

```mermaid
flowchart TD
  A["Transform runs the batch under catch_unwind"] --> B{"Panicked?"}
  B -- no --> OK["Hand-off, as in Success"]
  B -- yes --> C["Drop every record this attempt produced. Count smeltr_batches_poisoned_total"]
  C --> F{"The Unprocessable answer"}
  F -- block --> Q["Block the source, keep the batch"]
  Q --> R["A redeploy re-reads it from the last commit"]
  F -- drop --> G["Count smeltr_rows_dropped_total, reason unprocessable. Report Abandoned, waiting on nothing"]
  G --> K2["The commit moves past it in poll order"]
  F -- deliver --> H["Report Abandoned, waiting on the destinations named"]
  H --> I["divert_unprocessable: to every destination named, in its own format, handed over under the batch id"]
  I --> J{"Every queue took its unit?"}
  J -- no --> W["Park the batch, marked unprocessable. Its re-run skips the pipelines and sends it again"]
  W --> I
  J -- yes --> L["Each destination acknowledges"]
  L --> K["Commit once every one of them has acknowledged"]
```

- A batch parked as unprocessable is not run again: `step` sends it straight on.
- A destination named here answers for what it refuses with its own `Refused` answer, so its targets take the records and the commit waits for them too.
- Over budget is the neighboring path. A range the memory budget refuses is halved and run again. A single record that still will not fit meets the `Unprocessable` answer by `divert_over_budget`: under block, the whole batch is held; under deliver, the record is handed over with this batch's records, so that commit waits for it.

## Refused

The destination looked and said no. Its `Refused` answer, the destination's over the group's, answers: block, the default, keeps the refused records in egress and sends them again until the destination takes them; deliver sends them on to every destination it names, its targets; drop lets them go. Every refusal is counted whatever the answer.

```mermaid
flowchart TD
  A["The egress thread delivers a unit"] --> B["The destination refuses some records. Elasticsearch names each refused item; the retryable ones go again on their own"]
  B --> C{"The Refused answer"}
  C -- "block, the default" --> K["Egress keeps the refused records and reports them held, not done"]
  K --> K1["The transform thread counts smeltr_rows_rejected_total and reports the block. The reader blocks the source"]
  K1 --> K2["Egress sends the records again, 1 s after the refusal, then twice as long each time, at most a minute apart"]
  K2 --> K3{"Taken?"}
  K3 -- no --> K1
  K3 -- "yes: egress reports Done" --> E["The reader hears the batch acknowledged for this destination"]
  C -- "drop or deliver" --> D["Egress reports Done, with each refusal and the document it refused"]
  D --> T["The transform thread that handed over the batch counts smeltr_rows_rejected_total"]
  T -- "drop: count smeltr_rows_dropped_total, reason rejected" --> E
  T -- deliver --> Q{"Every target's queue has room?"}
  Q -- no --> Q2["Keep the records on the transform thread and hold this destination's acknowledgment back. Offer them again on the next collect, to the targets still left"]
  Q2 --> Q
  Q -- yes --> Q3["Hand the records to each target under the same batch id, and report that the batch now waits on each"]
  Q3 --> E
  Q3 --> R["The target delivers them"]
  R --> R2{"The target refuses one too?"}
  R2 -- "yes: the target's own Refused answer answers, from the top. Dropped there, the record is counted smeltr_rows_lost_total" --> C
  R2 -- no --> S["The target acknowledges the batch"]
  E --> F["Commit once every destination the batch waits on has acknowledged, the targets included"]
  S --> F
```

- Block is kept in egress because the fix for a refusal is usually the destination's own -- a mapping, a limit -- and nothing tells the worker it is done. Waiting for a redeploy would wait for a config change nobody needs to make. The kept records count against the destination's queue until they are delivered.
- Targets are the transform thread's to fill: it holds a handle on every destination its source reaches, targets included, and the report that the batch waits on a target goes out before this destination's acknowledgment, on the same channel, so the reader never sees the batch done while a target still has its records.
- A target is sent the record in the format it writes, kept beside the record from when it was sent, keyed on the target's own identity, which is worked out when the record is sent, while its fields are still there. A target a delivery rule already sent the record to is not sent it again. Chains that meet -- two destinations that refuse into the same target -- deliver the record there once from each: at least once, as everywhere. The record's placement -- the `__destination.topic`, `.index` or `.prefix` its destination reads -- stays behind: it routes within the destination it was computed for.
- A target can owe one batch more than one unit -- its own records, and records each destination that refuses into it salvaged. The reader hears it done with the batch only once every one has come back.
- A destination can refuse a record as it is handed the batch, before any acknowledgment. `deliver` answers with the refusal and its record, and the destination's `Refused` answer answers exactly as above. An `EgressHandle` refuses nothing itself, so in a deployment every refusal comes with the acknowledgment.

## Unreachable

The destination did not answer. Nothing commits past the batch until it is delivered.

```mermaid
flowchart TD
  A["Egress delivers a unit"] --> B{"Error or timeout"}
  B --> C["Take back every unacknowledged unit. Retry with backoff. Mark the destination stalled"]
  C --> A
  C --> D["The egress queue fills"]
  D --> E["No room. The batch is parked on its lane with the records its pipelines made. The pool serves the other lanes"]
  E --> F{"Waited past the destination's stall time?"}
  F -- no --> E
  F -- yes --> G["Report Block. The reader pauses the source's partitions, those the group assigns later included, and keeps polling empty, so it stays in the group"]
  G --> H["No commit past the batch. Lag grows"]
  H --> I["Room returns. The parked records are handed to the destinations still owed them, without running the pipelines again"]
  I --> K["The tracker empties. The source is released"]
```

- A stalled destination stops only the sources that feed it.
- Room is asked for before records are handed over, since a full queue drops what it refuses. When room is taken between the asking and the handing, the records are gone, and the batch runs again from the start once there is room; destinations that already took it get duplicates: at-least-once.
- A reader whose batches stop coming back reaches its in-flight limit, or its lane's handoff queue fills, and it takes no more work. Past `MEMBERSHIP_AFTER` of that it keeps its source's membership in place of polling: `keep_membership` polls with every partition paused, so the group's rebalances do not wait on it and it is not evicted.
- A failed credential and throttling count as unreachable: they get fixed, then retried.
