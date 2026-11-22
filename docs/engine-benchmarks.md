# Engine benchmarks

`just engine-speed` times smeltr beside Logstash, Vector and Apache NiFi doing the same work on the same machine. Each
engine runs in a container with the same cores and the same memory. It takes the same records, either pushed to it
over HTTP or pulled from a Kafka topic, and delivers what its pipeline makes the same way. Before an engine is timed,
every document it makes of the first 20,000 records is compared with what the pipeline must make; after it is timed,
the records it delivered are counted against the records it took. A run is one engine doing one pipeline over one
transport on some number of cores. A run that fails either test is reported as failed, not timed.

```mermaid
flowchart LR
  subgraph harness [the other cores]
    sender
    sink
    t1[(input topic)]
    t2[(output topic)]
  end
  subgraph container [the engine's cores and memory]
    e1[engine]
    e2[engine]
  end
  sender -- HTTP --> e1 -- HTTP --> sink
  t1 -- Kafka --> e2 -- Kafka --> t2
```

## Engines

| Engine | Image | License |
|---|---|---|
| smeltr | the `dist` binary on Ubuntu 24.04 | - |
| Logstash | `docker.elastic.co/logstash/logstash-oss:9.5.6` | Apache-2.0 |
| Vector | `timberio/vector:0.62.0-debian` | MPL-2.0 |
| NiFi | `apache/nifi:2.12.0` | Apache-2.0 |

NiFi has two entries. `nifi` maps records in a processor of its own, packaged as a NAR, as a NiFi shop that normalizes
its data builds one. `nifi-groovy` maps them in a Groovy script, as stock NiFi does with no code of its own. They
differ only in the mapping, and passthrough has none, so `nifi-groovy` runs `json_app` alone.

Each engine is set up for throughput with its documented settings only, at the values its documentation advises, or
where it advises none, at the value past which more stopped adding speed. Its pipelines are in `tests/engines/`, under
its name, and its transports and settings in `tests/engines/mod.rs`.

- **smeltr** runs its defaults, as one process holding its leader and a worker, except that its worker's threads are
  limited to the hyperthreads it has.
- **Logstash** runs a worker for each hyperthread, batches of 1,000 and no ordering. Its heap is half the container's
  memory.
- **Vector** runs a worker thread for each hyperthread.
- **NiFi** runs a flow the harness builds through its REST API: the transport's consumer, the mapping, and the
  transport's producer. The consumer runs a task for each hyperthread over Kafka and one over HTTP, the mapping a task
  for each hyperthread, the producer two for each hyperthread over Kafka and eight for each over HTTP, past which more
  stopped adding speed, and the thread pool has four threads for each hyperthread, the most NiFi's administration guide
  advises. The mapping's reader is given the record schema rather than inferring one. A delivery that fails goes back to
  the producer, as the other engines retry it. Content archiving, which keeps sent content for replay, is off: with it
  on, NiFi stops taking records while the disk is more than half full. Its heap is half the container's memory.

## Transports

- **HTTP** is the receiver case. The harness's sender posts 2,000 records a request, 256 requests at once, to each
  engine's HTTP listener. Every engine answers a request once it holds its records, before it delivers them: Logstash
  and NiFi cannot answer later, and smeltr and Vector answer on receipt unless told otherwise. A busy answer is
  retried after the time it names, or after a wait that doubles up to a second. Each engine posts what it makes to the
  harness's sink, which runs a thread for each of the harness's hyperthreads.
- **Kafka** is the pull case. One Apache Kafka 4.4.0 broker serves every run, on the harness's cores, with its log on
  tmpfs so the disk sets no limit. Before the first run the harness writes three runs' worth of records, spread evenly,
  to an input topic with a partition for each hyperthread the engine may have, and at least 16. Each run consumes it
  from the start under a group of its own and produces to a topic of its own, which the harness counts by its offsets.
  Each engine uses its own Kafka client, with a consumer thread for each hyperthread where it has them, the producer
  batching as the client batches by default, every write acknowledged by the broker, and no transactions. Logstash's
  producer is told to batch as librdkafka does, up to a megabyte waiting up to 5 ms, since by default it waits for no
  batch to fill.

## Pipelines

**passthrough** forwards each record as it came, and every engine sends the bytes it read with nothing of its own
added. Smeltr's destination writes each record's text, Vector encodes its events as text, and NiFi moves the bytes it
took. Logstash over Kafka writes each event's message. Over HTTP, Logstash posts many events to a request only as a
JSON array of them, so it reads each record as JSON and removes the fields its input adds before it posts them.

**json_app** is the `json_app` reference pipeline: parse the JSON, drop the debug lines, compute `event.duration` in
nanoseconds, move the application's fields to their ECS names, and leave out the rest. Each engine maps a record so:

| Engine | Pipeline |
|---|---|
| smeltr | five steps, in `tests/reference/json_app.rs`: a JSON parser typing `ts`, `level`, `attempt` and `latency_ms`; an `if` dropping debug; an `eval` computing `event.duration` and copying the fields that move; a `rename` of the typed fields; an `eval` setting the fields left out to null |
| Logstash | `tests/engines/logstash/json_app.conf`: `json`, a conditional `drop`, `date`, one `ruby` line for `event.duration`, which no core filter can compute, and a `mutate` renaming and removing |
| Vector | `tests/engines/vector/json_app.vrl`, one `remap` |
| NiFi | `tests/engines/nifi/nar/`, a Java processor reading and writing records through NiFi's JSON reader and writer |
| NiFi Groovy | `tests/engines/nifi/json_app.groovy`, a ScriptedTransformRecord, with the reader's schema in `json_app.avsc` |

## Checking a run

Each engine is checked once for each pipeline and transport, on the fewest cores a run gives it. The check sends it
the first 20,000 records and compares the documents that arrive with those the pipeline must make, as a multiset: no
document missing, none extra, none different. A passthrough document must be the record itself. A `json_app`
document must be the reference's, its `@timestamp` the same instant written in UTC to any precision.

Each timed run is counted when it ends. Over HTTP the sender stops and the sink must receive, record for record, what
the engine accepted. Over Kafka the engine reads the whole input topic and the output topic must hold every record it
keeps. More is a duplicate and less is a loss, and either fails the run.

## What a run runs

- **Threads.** A row is measured in hardware threads, set by `ENGINE_THREADS`. The engine's container gets them as
  whole physical cores, the first ones, so the count is a multiple of a core's threads. The harness runs on the other
  cores, so it never shares a core with the engine.
- **Memory.** Every container gets the same limit, 4 GB unless `ENGINE_MEMORY_GB` says otherwise.
- **Disk.** A run fails when less than 5 GB of the disk is free, before or while it runs: NiFi keeps its queues on the
  disk and slows as it fills.
- **The clock** starts once the engine has delivered a tenth of the run's records and run for 20 seconds, so the JVM
  engines have compiled their hot paths. Under Kafka, the clock starts sooner for an engine fast enough that waiting
  the full 20 seconds would leave too few records in the input topic to time. A run then times 2,000,000 records,
  or the records per run `ENGINE_ROWS` names.
- **Order.** Each of `ENGINE_ROUNDS` rounds, three by default, does every run. Each round starts with a different engine
  and runs the core counts in the reverse of the order the round before used, so a host that slows as it warms costs
  every engine alike.

## Differences that remain

The engines are built differently, and some of the difference cannot be set away. The runs do not show these:

- **NiFi keeps its data on disk.** Its flowfile, content and provenance repositories are written to the container's
  disk, so what it accepts survives a restart. The other engines hold what is in flight in memory. This can cost NiFi
  speed, and its page cache counts toward its container's memory and its peak.
- **NiFi's HTTP listener takes requests without credentials.** ListenHTTP authenticates only TLS client
  certificates. The other engines check a password or a token on every request, which favours NiFi a little.
- **Kafka offsets are committed differently.** Smeltr, and Vector with its sink acknowledging, commit an offset once
  its records are delivered. NiFi commits once they are in its repositories, and Logstash commits on its client's
  interval, whatever became of them, which favours Logstash a little.
- **NiFi's passthrough moves blobs.** A Kafka message or a request body passes through NiFi as one piece of content,
  while the other engines read it as records, one event each. This favours NiFi in passthrough.
- **Smeltr's container runs its leader as well as its worker**, as a standalone smeltr does, and the cores and memory
  counted include both. This costs smeltr a little.

## Reading a run

- **records/s** is input records a second, the median of the rounds. **Spread** is the fastest round minus the
  slowest, divided by the median.
- **records/s per core used** divides by the cores the container used, as its cgroup counts them.
- **Peak memory** is the most the container held, page cache included, as its cgroup counts it.
- **Broker cores** is what the Kafka broker used. Near the number of cores the harness has, the broker set the run's
  speed.
- **Harness busy** is how busy the harness's cores were, the broker included. Near 100%, the harness, not the engine,
  set the run's speed.

## Results

RESULTS
