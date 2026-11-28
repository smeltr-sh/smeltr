# Connector benchmarks

`just connector-speed` times each source and each destination alone, through the shipped worker on one core, over a
network link with the latency and loss of a path between cloud regions, and again over no link at all.
A row is one connector, and every other part of its path is a harness end that takes whatever the worker gives it, so
the row says what that connector sustains, and what held it there.

```mermaid
flowchart LR
  subgraph source
    svc1[(service)] -- link --> w1[worker] --> sink
  end
  subgraph receiver
    sender --> relay -- link --> w2[worker] --> sink2[sink]
  end
  subgraph destination
    sender2[sender] --> w3[worker] -- link --> svc2[(service)]
  end
```

## What a row runs

- **The worker** is the `smeltr` binary built with the `dist` profile, a leader with its worker inside, deployed
  through its repository as an administrator deploys. Its one pipeline passes records through unchanged.
- **Cores.** The worker runs as on a host of one core: all its threads on the first core, with that core's
  hyperthread sibling left idle, and one transform thread (`SMELTR_WORKER_THREAD_LIMIT=1`). The harness and every
  container share the remaining cores. `CONNECTOR_THREADS=1,2,4` gives the worker that many cores, and a transform
  thread for each. With `CONNECTOR_PIN=thread`, each transform thread runs alone on its core and the worker's other
  threads share the harness's cores. With `CONNECTOR_PIN=off`, nothing is pinned and everything shares every core.
- **A source row** seeds its service before the worker starts, then reads it into a sink on this host. Object
  sources read files of 5,000 lines, gzipped when `CONNECTOR_GZIP` is set.
- **A receiver row** sends from this host through a relay container. The relay's link is shaped, so the traffic
  crosses the link once each way, and the worker delivers to a sink on this host.
- **A destination row** sends to an HTTP receiver on this host, which answers each request once the destination has
  its records, and writes to the destination's service. Destinations that write files close one every 5 seconds, the
  least they allow.
- **The corpus** is the `json_app` reference pipeline's lines, about 330 bytes each.
- **The clock.** The first tenth of a row's records is the warm-up and is not timed. A row moves 1,000,000 records
  unless `CONNECTOR_ROWS` says otherwise.
- **Order.** Each row runs on the shaped link and on the bare one, then again in the opposite order. A host that
  slows as it warms costs both links alike.

## The link

`bench/network/network.env` is the link, and it is the one every live benchmark uses:

- 20 ms each way, so a 40 ms round trip;
- 2 ms of jitter;
- 0.05% loss each way;
- 1 Gbit/s.

`bench/bench-network.sh` puts it on both ends of each container's veth with `tc netem`. Segmentation offload is
off on both ends, so a loss drops one packet and not a 64 KiB segment. A kernel without `sch_netem` refuses the run
rather than measuring a perfect network.

## Reading a row

- **rows/s and MB/s** are what the row moved between the end of its warm-up and its last record. MB counts the
  records' bytes as the harness sent or received them.
- **Transform** is the share of a core the transform thread used. **Busiest other thread** is the worker's busiest
  thread besides it, by name. **Peer** is the share of a core the service's containers used, as their cgroups count it.
  That includes the service's own health checks.
- **Rest** is how busy the harness's cores were, and **Cores** how busy the worker's were.
- **Bound by** names the one of Transform, Busiest other thread or Peer that reached nine tenths of a core. Failing
  those it says `host` when Rest was nine tenths busy, and `cores` when Cores was. `waits` means none did: the row
  waited on round trips, polls or acknowledgements, which is the link's share of the cost.
- **A file destination** answers a file's records when it closes the file, so the end of a row waits up to 5 seconds
  after the last record. That makes its rate a floor, closer to the truth the more records the row moves.
- **A peer that saturates** measures the emulator, not the connector. The services are single-node test fixtures,
  so a `peer` row says how fast the fixture goes, not how fast the connector can. LocalStack serves S3 from about
  one core (its S3 rows peaked near 130% of one on a 12-core host), so `s3 list source on seaweedfs` reads the same
  files from SeaweedFS, which serves S3 on every core.

## Not covered

- **The S2S receiver.** Only a universal forwarder speaks S2S, and the one in `tests/live/compose.live.yaml` clones to three test
  receivers.
- **The HTTP GET and Okta sources.** They read one page per poll, so a rate measures the poll interval.
- **The Prometheus remote write destination.** It takes metric records, not log lines.
- **The metrics and logs sources.** They cross no network.

## Results

Run on an Intel Core i7-8650U, 4 cores and 8 threads, Linux Mint 22.3.

| Connector | Link | rows/s | MB/s | Transform | Busiest other thread | Peer | Bound by |
|---|---|---:|---:|---:|---|---:|---|
