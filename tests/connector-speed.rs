//! Each source and each destination timed alone through a deployed worker, over the link `bench/network` describes
//! and over none: `just connector-speed`. A source row reads what the harness seeded into the source's service and
//! delivers it to a sink on this host; a destination row takes what the harness sends a receiver on this host and
//! writes it to the destination's service. Only the leg under test crosses the shaped link, and the harness's own
//! ends take whatever the worker gives them, so a row measures the worker and the link, and the peer when it is the
//! one that saturates.
//!
//! ```mermaid
//! flowchart LR
//!   subgraph source row
//!     svc1[(service)] -- shaped link --> w1[worker] -- loopback --> sink
//!   end
//!   subgraph destination row
//!     sender -- loopback --> w2[worker] -- shaped link --> svc2[(service)]
//!   end
//! ```
//!
//! The worker is the shipped binary, a leader with its worker inside, deployed through its repository as an
//! administrator deploys. It runs one transform thread, alone on the first core with that core's sibling left idle;
//! everything else, the harness and the services included, shares the cores that remain.
#![cfg(target_os = "linux")]

mod live;
mod panos;
mod reference;

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// One source or destination, as a row of the table.
struct Case {
    name: &'static str,
    /// The `tests/live/compose.live.yaml` services whose CPU is the peer's.
    services: &'static [&'static str],
    leg: Leg,
}

enum Leg {
    /// Seeds `rows` records of the corpus where the source reads them, under a name unique to this run, and returns
    /// the source's declaration. `None` when its service is not configured.
    Source(fn(run: &str, rows: u64) -> Option<Value>),
    /// The destination's declaration, writing under a name unique to this run. `None` when its service is not
    /// configured.
    Destination(fn(run: &str) -> Option<Value>),
    /// A receiver on this host, and how the harness sends it the corpus across the link.
    Receiver(Receiver, Sending),
}

/// How the harness sends a receiver the corpus.
enum Sending {
    Requests(Push),
    /// Syslog lines on connections held open.
    Syslog,
}

const CASES: &[Case] = &[
    Case {
        name: "kafka source",
        services: &["kafka"],
        leg: Leg::Source(kafka::source),
    },
    Case {
        name: "event hubs source",
        services: &["event-hubs", "azurite"],
        leg: Leg::Source(event_hubs::source),
    },
    Case {
        name: "s3 sqs source",
        services: &["localstack"],
        leg: Leg::Source(s3::sqs_source),
    },
    Case {
        name: "s3 list source",
        services: &["localstack"],
        leg: Leg::Source(s3::list_source),
    },
    Case {
        name: "gcs list source",
        services: &["fake-gcs"],
        leg: Leg::Source(gcs::list_source),
    },
    Case {
        name: "gcs pubsub source",
        services: &["fake-gcs", "pubsub"],
        leg: Leg::Source(gcs::pubsub_source),
    },
    Case {
        name: "azure blob list source",
        services: &["azurite"],
        leg: Leg::Source(azure::list_source),
    },
    Case {
        name: "azure blob queue source",
        services: &["azurite"],
        leg: Leg::Source(azure::queue_source),
    },
    Case {
        name: "sftp source",
        services: &["sftp"],
        leg: Leg::Source(sftp::source),
    },
    Case {
        name: "postgres source",
        services: &["postgres"],
        leg: Leg::Source(sql::postgres),
    },
    Case {
        name: "mysql source",
        services: &["mysql"],
        leg: Leg::Source(sql::mysql),
    },
    Case {
        name: "mariadb source",
        services: &["mariadb"],
        leg: Leg::Source(sql::mariadb),
    },
    Case {
        name: "http receiver",
        services: &[],
        leg: Leg::Receiver(receivers::http, Sending::Requests(receivers::HTTP)),
    },
    Case {
        name: "hec receiver",
        services: &[],
        leg: Leg::Receiver(receivers::hec, Sending::Requests(receivers::HEC)),
    },
    Case {
        name: "elastic receiver",
        services: &[],
        leg: Leg::Receiver(receivers::elastic, Sending::Requests(receivers::ELASTIC)),
    },
    Case {
        name: "otlp receiver",
        services: &[],
        leg: Leg::Receiver(receivers::otlp, Sending::Requests(receivers::OTLP)),
    },
    Case {
        name: "prometheus receiver",
        services: &[],
        leg: Leg::Receiver(receivers::prometheus, Sending::Requests(receivers::PROMETHEUS)),
    },
    Case {
        name: "syslog receiver",
        services: &[],
        leg: Leg::Receiver(receivers::syslog, Sending::Syslog),
    },
    Case {
        name: "kafka destination",
        services: &["kafka"],
        leg: Leg::Destination(kafka::destination),
    },
    Case {
        name: "event hubs destination",
        services: &["event-hubs", "azurite"],
        leg: Leg::Destination(event_hubs::destination),
    },
    Case {
        name: "elasticsearch destination",
        services: &["elasticsearch"],
        leg: Leg::Destination(elastic::destination),
    },
    Case {
        name: "splunk hec destination",
        services: &["splunk"],
        leg: Leg::Destination(splunk::hec),
    },
    Case {
        name: "http destination",
        services: &["splunk"],
        leg: Leg::Destination(splunk::http),
    },
    Case {
        name: "s3 destination",
        services: &["localstack"],
        leg: Leg::Destination(s3::destination),
    },
    Case {
        name: "gcs destination",
        services: &["fake-gcs"],
        leg: Leg::Destination(gcs::destination),
    },
    Case {
        name: "azure blob destination",
        services: &["azurite"],
        leg: Leg::Destination(azure::destination),
    },
    Case {
        name: "sftp destination",
        services: &["sftp"],
        leg: Leg::Destination(sftp::destination),
    },
    Case {
        name: "iceberg destination",
        services: &["iceberg-rest", "localstack"],
        leg: Leg::Destination(s3::iceberg),
    },
    Case {
        name: "delta destination",
        services: &["localstack"],
        leg: Leg::Destination(s3::delta),
    },
];

/// Records each row moves, from `CONNECTOR_ROWS`.
fn rows() -> u64 {
    std::env::var("CONNECTOR_ROWS").map_or(1_000_000, |r| r.parse().expect("CONNECTOR_ROWS is a number"))
}

/// The records a row moves before its clock starts: the worker's start, its first connection and its heap's growth
/// are not what it sustains.
fn warm(rows: u64) -> u64 {
    rows / 10
}

/// How long a row may move nothing before it is given up.
const PATIENCE: Duration = Duration::from_secs(120);

/// Record `i` of the corpus: one line of a service's JSON log.
fn line(i: u64) -> String {
    (reference::json_app::PIPELINE.line)(i)
}

#[test]
#[ignore = "run by `just connector-speed`"]
fn speed() {
    let cores = Cores::of_this_host();
    cores.confine(0);
    let only = std::env::var("CONNECTOR").ok();
    let cases: Vec<&Case> = CASES
        .iter()
        .filter(|c| only.as_deref().is_none_or(|o| c.name.contains(o)))
        .collect();
    let passes: &[Link] = match std::env::var("CONNECTOR_LINK").as_deref() {
        Ok("bare") => &[Link::Bare],
        Ok(other) if other != "both" => panic!("CONNECTOR_LINK is bare or both, not {other}"),
        _ => &[Link::Shaped, Link::Bare],
    };
    // Bare again however the run ends: the live tests share these services.
    let _bare = BareAfter;
    let _pinned = Pinned::off(&cores);
    let rows = rows();
    let mut table: Vec<(&str, Link, Option<Measured>)> = Vec::new();
    // Each order once, so a host that slows as it warms costs both links alike.
    let forward: Vec<Link> = passes.to_vec();
    let backward: Vec<Link> = passes.iter().rev().copied().collect();
    for (round, order) in [forward, backward].into_iter().enumerate() {
        for case in &cases {
            for &link in &order {
                link.apply();
                let run = format!(
                    "cs-{}-{}-{round}-{}",
                    case.name.replace(' ', "-"),
                    link.label(),
                    std::process::id()
                );
                let m = match case.leg {
                    Leg::Source(seed) => seed(&run, rows).map(|decl| time_source(&run, decl, rows, case, &cores)),
                    Leg::Destination(declare) => {
                        declare(&run).map(|decl| time_destination(&run, decl, rows, case, &cores))
                    }
                    Leg::Receiver(declare, ref sending) => {
                        Some(time_receiver(&run, declare, sending, rows, link, &cores))
                    }
                };
                if let Some(m) = &m {
                    println!("{:<24} {:<6} {}", case.name, link.label(), m.row());
                }
                table.push((case.name, link, m));
            }
        }
    }
    println!("\n{}", markdown(&table));
}

/// Every live container kept off the transform core, each given back the CPUs it had when dropped: the live tests
/// share these services.
struct Pinned(Vec<(String, String)>);

impl Pinned {
    fn off(cores: &Cores) -> Pinned {
        let ids: Vec<String> = compose(&["ps", "-q"]).split_whitespace().map(String::from).collect();
        let had = ids.into_iter().map(|id| {
            let out = Command::new("docker")
                .args(["inspect", "-f", "{{.HostConfig.CpusetCpus}}", &id])
                .output()
                .expect("docker runs");
            (id, String::from_utf8(out.stdout).unwrap().trim().to_string())
        });
        let pinned = Pinned(had.collect());
        for (id, _) in &pinned.0 {
            assert!(
                cpuset(id, &cores.rest_list()),
                "could not keep container {id} off the transform core"
            );
        }
        pinned
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        // Docker reads an empty set as every CPU, so one the container never had is given back as all of them.
        let all = std::fs::read_to_string("/sys/devices/system/cpu/online").expect("online CPUs");
        for (id, cpus) in &self.0 {
            if !cpuset(id, if cpus.is_empty() { all.trim() } else { cpus }) {
                eprintln!("could not give container {id} back its CPUs");
            }
        }
    }
}

/// Puts container `id` on `cpus`; whether it took.
fn cpuset(id: &str, cpus: &str) -> bool {
    Command::new("docker")
        .args(["update", "--cpuset-cpus", cpus, id])
        .stdout(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Takes the link away when dropped.
struct BareAfter;

impl Drop for BareAfter {
    fn drop(&mut self) {
        let _ = Command::new(repo().join("bench/bench-network.sh"))
            .args(["off", "tests/live/compose.live.yaml"])
            .current_dir(repo())
            .status();
    }
}

/// What a row cost, between the end of its warm-up and its last record.
struct Measured {
    rows: u64,
    bytes: u64,
    wall: Duration,
    /// CPU seconds of the transform thread.
    transform: f64,
    /// The worker's busiest other thread, and its CPU seconds.
    busiest: (String, f64),
    /// CPU seconds of the peer's processes.
    peer: f64,
}

impl Measured {
    fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.wall.as_secs_f64()
    }

    fn mb_per_sec(&self) -> f64 {
        self.bytes as f64 / self.wall.as_secs_f64() / 1e6
    }

    /// A share of one core, for each of the transform thread, the busiest other thread and the peer.
    fn shares(&self) -> (f64, f64, f64) {
        let w = self.wall.as_secs_f64();
        (self.transform / w, self.busiest.1 / w, self.peer / w)
    }

    /// What saturated: a thread of the worker, or the peer, at nine tenths of a core or more. Neither is `waits`: the
    /// row waits on round trips, polls or acknowledgements.
    fn bound(&self) -> String {
        let (t, b, p) = self.shares();
        if t >= 0.9 {
            "transform".into()
        } else if b >= 0.9 {
            self.busiest.0.clone()
        } else if p >= 0.9 {
            "peer".into()
        } else {
            "waits".into()
        }
    }

    fn row(&self) -> String {
        let (t, b, p) = self.shares();
        format!(
            "{:>9.0} rows/s {:>7.1} MB/s  transform {:>4.0}%  {} {:>4.0}%  peer {:>4.0}%  bound: {}",
            self.rows_per_sec(),
            self.mb_per_sec(),
            t * 100.0,
            self.busiest.0,
            b * 100.0,
            p * 100.0,
            self.bound()
        )
    }
}

fn markdown(table: &[(&str, Link, Option<Measured>)]) -> String {
    let mut out = String::from(
        "| Connector | Link | rows/s | MB/s | Transform | Busiest other thread | Peer | Bound by |\n\
         |---|---|---:|---:|---:|---|---:|---|\n",
    );
    for (name, link, m) in table {
        match m {
            Some(m) => {
                let (t, b, p) = m.shares();
                out.push_str(&format!(
                    "| {name} | {} | {:.0} | {:.1} | {:.0}% | {} {:.0}% | {:.0}% | {} |\n",
                    link.label(),
                    m.rows_per_sec(),
                    m.mb_per_sec(),
                    t * 100.0,
                    m.busiest.0,
                    b * 100.0,
                    p * 100.0,
                    m.bound()
                ));
            }
            None => out.push_str(&format!(
                "| {name} | {} | skipped: no service | | | | | |\n",
                link.label()
            )),
        }
    }
    out
}

/// Whether the services sit behind `bench/network`'s link or none.
#[derive(Clone, Copy, PartialEq)]
enum Link {
    Shaped,
    Bare,
}

impl Link {
    fn label(self) -> &'static str {
        match self {
            Link::Shaped => "shaped",
            Link::Bare => "bare",
        }
    }

    fn apply(self) {
        let state = match self {
            Link::Shaped => "on",
            Link::Bare => "off",
        };
        shape(&[state, "tests/live/compose.live.yaml"]);
    }
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// `docker compose -f tests/live/compose.live.yaml ARGS`, and what it printed.
fn compose(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(["compose", "-f", "tests/live/compose.live.yaml"])
        .args(args)
        .current_dir(repo())
        .output()
        .expect("docker runs");
    assert!(
        out.status.success(),
        "docker compose {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// The containers of `services`.
fn containers(services: &[&str]) -> Vec<String> {
    services
        .iter()
        .flat_map(|s| {
            let ids: Vec<String> = compose(&["ps", "-q", s]).split_whitespace().map(String::from).collect();
            assert!(!ids.is_empty(), "{s} is not running: `just live-up` starts it");
            ids
        })
        .collect()
}

/// CPU seconds the containers have used, from their cgroups: what runs in them, the processes that come and go
/// included.
fn containers_cpu(ids: &[String]) -> f64 {
    ids.iter()
        .map(|id| {
            // cgroup v2 under systemd, then v1.
            let v2 = format!("/sys/fs/cgroup/system.slice/docker-{id}.scope/cpu.stat");
            if let Ok(stat) = std::fs::read_to_string(&v2) {
                let usec: f64 = stat
                    .lines()
                    .find_map(|l| l.strip_prefix("usage_usec "))
                    .and_then(|v| v.trim().parse().ok())
                    .expect("cpu.stat holds usage_usec");
                return usec / 1e6;
            }
            let v1 = format!("/sys/fs/cgroup/cpuacct/docker/{id}/cpuacct.usage");
            let nsec: f64 = std::fs::read_to_string(&v1)
                .unwrap_or_else(|_| panic!("no cgroup for container {id} at {v2} or {v1}"))
                .trim()
                .parse()
                .unwrap();
            nsec / 1e9
        })
        .sum()
}

/// The transform core, its siblings left idle, and the cores everything else shares.
struct Cores {
    transform: usize,
    rest: Vec<usize>,
}

impl Cores {
    fn of_this_host() -> Cores {
        let online = cpu_list(&std::fs::read_to_string("/sys/devices/system/cpu/online").expect("online CPUs"));
        let transform = online[0];
        let siblings = std::fs::read_to_string(format!(
            "/sys/devices/system/cpu/cpu{transform}/topology/thread_siblings_list"
        ))
        .map(|s| cpu_list(&s))
        .unwrap_or_else(|_| vec![transform]);
        let rest: Vec<usize> = online.into_iter().filter(|c| !siblings.contains(c)).collect();
        assert!(!rest.is_empty(), "one core leaves nothing for the rest");
        Cores { transform, rest }
    }

    fn rest_list(&self) -> String {
        self.rest.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(",")
    }

    /// Puts thread `tid` (0 for this one) on the cores everything but the transform shares. What it starts inherits
    /// them.
    fn confine(&self, tid: i32) {
        set_affinity(tid, &self.rest);
    }
}

/// `0-3,5`: the CPUs it names.
fn cpu_list(s: &str) -> Vec<usize> {
    s.trim()
        .split(',')
        .flat_map(|part| match part.split_once('-') {
            Some((a, b)) => (a.parse().unwrap()..=b.parse().unwrap()).collect::<Vec<usize>>(),
            None => vec![part.parse().unwrap()],
        })
        .collect()
}

fn set_affinity(tid: i32, cpus: &[usize]) {
    // SAFETY: `set` is a zeroed `cpu_set_t` with only the given bits set, as `sched_setaffinity` reads it.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        assert_eq!(
            libc::sched_setaffinity(tid, size_of::<libc::cpu_set_t>(), &set),
            0,
            "pin thread {tid} to {cpus:?}"
        );
    }
}

/// CPU seconds of `/proc/{path}/stat`'s process or thread, or 0 once it is gone.
fn stat_cpu(path: &str) -> f64 {
    // SAFETY: `sysconf` reads a constant.
    let tick = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{path}/stat")) else {
        return 0.0;
    };
    // utime and stime, the 14th and 15th fields; the 2nd, the command, may hold spaces and ends at the last `)`.
    let rest = &stat[stat.rfind(')').unwrap() + 2..];
    let ticks: u64 = rest
        .split(' ')
        .skip(11)
        .take(2)
        .map(|t| t.parse::<u64>().unwrap())
        .sum();
    ticks as f64 / tick
}

/// Each thread of process `pid`: its id, its name and its CPU seconds.
fn threads(pid: u32) -> Vec<(i32, String, f64)> {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|t| {
            let tid: i32 = t.file_name().to_str()?.parse().ok()?;
            let name = std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/comm")).ok()?;
            Some((tid, name.trim().to_string(), stat_cpu(&format!("{pid}/task/{tid}"))))
        })
        .collect()
}

/// The transform thread's name, as `comm` holds it: fifteen bytes of `smeltr-transform-0`.
const TRANSFORM_THREAD: &str = "smeltr-transfor";

/// A snapshot of what the worker and the peer have used.
struct Usage {
    at: Instant,
    threads: Vec<(i32, String, f64)>,
    peer: f64,
}

impl Usage {
    fn now(worker: &Smeltr, peer: &[String]) -> Usage {
        Usage {
            at: Instant::now(),
            threads: worker.runtime().map(threads).unwrap_or_default(),
            peer: containers_cpu(peer),
        }
    }

    fn since(&self, before: &Usage, rows: u64, bytes: u64) -> Measured {
        let used = |tid: i32, now: f64| now - before.threads.iter().find(|t| t.0 == tid).map_or(0.0, |t| t.2);
        let transform = self
            .threads
            .iter()
            .filter(|t| t.1 == TRANSFORM_THREAD)
            .map(|t| used(t.0, t.2))
            .sum();
        let busiest = self
            .threads
            .iter()
            .filter(|t| t.1 != TRANSFORM_THREAD)
            .map(|t| (t.1.clone(), used(t.0, t.2)))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .unwrap_or_default();
        Measured {
            rows,
            bytes,
            wall: self.at - before.at,
            transform,
            busiest,
            peer: self.peer - before.peer,
        }
    }
}

/// Waits until `count` reaches `want`, failing once it has not moved for `PATIENCE`.
fn wait_for(what: &str, want: u64, count: impl Fn() -> u64, mut each: impl FnMut()) {
    let (mut last, mut moved) = (count(), Instant::now());
    loop {
        let n = count();
        if n >= want {
            return;
        }
        if n != last {
            (last, moved) = (n, Instant::now());
        }
        assert!(
            moved.elapsed() < PATIENCE,
            "{what}: stuck at {n} of {want} for {PATIENCE:?}"
        );
        each();
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Reads what `decl` seeded and delivers it to a sink on this host.
fn time_source(run: &str, decl: Value, rows: u64, case: &Case, cores: &Cores) -> Measured {
    let sink = Sink::start();
    let worker = Smeltr::start(run);
    worker.deploy(
        decl,
        json!({"id": "out", "type": "http", "url": format!("http://127.0.0.1:{}/", sink.port)}),
    );
    let peer = containers(case.services);
    worker.warm_up(run, rows, cores, || sink.rows());
    let (before, b_rows, b_bytes) = (Usage::now(&worker, &peer), sink.rows(), sink.bytes());
    wait_for(run, rows, || sink.rows(), || {});
    let after = Usage::now(&worker, &peer);
    after.since(&before, sink.rows() - b_rows, sink.bytes() - b_bytes)
}

/// Commits a file destination is timed across, after its warm-up.
const TIMED_COMMITS: usize = 4;

/// Requests in flight to a file destination: more rows than it writes in `FILE_SECS`, since it answers a file's rows
/// only when it commits the file.
const FILE_SENDERS: usize = 1024;

/// Sends the corpus to a receiver on this host and writes it to `decl`, timed once the warm-up is answered. A
/// destination that answers rows only when it commits its files is sent the corpus over and over and timed from the
/// start of one commit to the start of a later one, so the clock spans whole intervals: timed from a request, it
/// would count the wait for a commit.
fn time_destination(run: &str, decl: Value, rows: u64, case: &Case, cores: &Cores) -> Measured {
    let files = decl.get("fileIntervalSecs").is_some();
    let port = free_port();
    let worker = Smeltr::start(run);
    worker.deploy(receivers::http(port), decl);
    worker.wait_for_receiver(run, port);
    let peer = containers(case.services);
    let url = format!("http://127.0.0.1:{port}/");
    let bodies = bodies(&receivers::HTTP);
    let send = |senders, from, to| Sender::start(&url, &receivers::HTTP, &bodies, senders, from, to);
    if files {
        let sender = send(FILE_SENDERS, 0, u64::MAX);
        worker.warm_up(run, rows, cores, || sender.acked());
        return time_commits(run, &worker, &peer, &sender);
    }
    let warm_up = send(SENDERS, 0, warm(rows));
    worker.warm_up(run, rows, cores, || warm_up.acked());
    let before = Usage::now(&worker, &peer);
    let sender = send(SENDERS, warm(rows), rows);
    wait_for(run, rows - warm(rows), || sender.acked(), || {});
    let after = Usage::now(&worker, &peer);
    after.since(&before, sender.acked(), sender.bytes())
}

/// Times `TIMED_COMMITS` commits of what `sender` keeps in flight to a file destination. A destination that commits
/// so often its answers never pause is timed across the same span, in whole intervals.
fn time_commits(run: &str, worker: &Smeltr, peer: &[String], sender: &Sender) -> Measured {
    let interval = Duration::from_secs(FILE_SECS);
    // A commit answers its requests together; the answers of two are an interval apart.
    let quiet = interval / 5;
    let mut commits: Vec<(Usage, u64, u64)> = Vec::new();
    let (mut acked, mut bytes, mut moved) = (sender.acked(), sender.bytes(), Instant::now());
    let mut last = Instant::now();
    let mut busy = 0;
    while commits.len() <= TIMED_COMMITS {
        let (n, b) = (sender.acked(), sender.bytes());
        if n != acked {
            if moved.elapsed() > quiet || last.elapsed() >= interval {
                commits.push((Usage::now(worker, peer), acked, bytes));
                last = Instant::now();
                if commits.len() == 1 {
                    busy = sender.busy();
                }
            }
            (acked, bytes, moved) = (n, b, Instant::now());
        }
        assert!(moved.elapsed() < PATIENCE, "{run}: no commit for {PATIENCE:?}");
        std::thread::sleep(Duration::from_millis(5));
    }
    // Never refused, the destination took all the harness had in flight: the harness set its speed.
    assert!(
        sender.busy() > busy,
        "{run}: the receiver took every request, so {FILE_SENDERS} in flight set its speed"
    );
    let (first, last) = (&commits[0], &commits[TIMED_COMMITS]);
    last.0.since(&first.0, last.1 - first.1, last.2 - first.2)
}

/// Sends the corpus through a relay to a receiver on this host and delivers it to a sink on this host.
fn time_receiver(run: &str, declare: Receiver, sending: &Sending, rows: u64, link: Link, cores: &Cores) -> Measured {
    let sink = Sink::start();
    let port = free_port();
    let worker = Smeltr::start(run);
    worker.deploy(
        declare(port),
        json!({"id": "out", "type": "http", "url": format!("http://127.0.0.1:{}/", sink.port)}),
    );
    worker.wait_for_receiver(run, port);
    let relay = Relay::start(port, link, cores);
    let peer = vec![relay.id.clone()];
    let _sender = match sending {
        Sending::Requests(push) => Some(Sender::start(
            &format!("http://127.0.0.1:{}{}", relay.port, push.path),
            push,
            &bodies(push),
            SENDERS,
            0,
            rows,
        )),
        Sending::Syslog => {
            stream_syslog(relay.port, 0, rows);
            None
        }
    };
    worker.warm_up(run, rows, cores, || sink.rows());
    let (before, b_rows, b_bytes) = (Usage::now(&worker, &peer), sink.rows(), sink.bytes());
    wait_for(run, rows, || sink.rows(), || {});
    let after = Usage::now(&worker, &peer);
    after.since(&before, sink.rows() - b_rows, sink.bytes() - b_bytes)
}

/// A container that relays connections from a port on this host to a receiver on this host, so a sender here
/// reaches the receiver across the link. Removed when dropped.
struct Relay {
    id: String,
    port: u16,
}

impl Relay {
    fn start(to: u16, link: Link, cores: &Cores) -> Relay {
        let out = Command::new("docker")
            .args(["run", "-d", "--rm", "--add-host", "host.docker.internal:host-gateway"])
            .args([
                "--cpuset-cpus",
                &cores.rest_list(),
                "-p",
                "127.0.0.1::9000",
                &network_image(),
            ])
            .args([
                "socat",
                "TCP-LISTEN:9000,fork,reuseaddr",
                &format!("TCP:host.docker.internal:{to}"),
            ])
            .output()
            .expect("docker runs");
        assert!(
            out.status.success(),
            "the relay: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let id = String::from_utf8(out.stdout).unwrap().trim().to_string();
        let mapped = Command::new("docker")
            .args(["port", &id, "9000"])
            .output()
            .expect("docker runs");
        let port = String::from_utf8(mapped.stdout)
            .unwrap()
            .lines()
            .find_map(|l| l.rsplit_once(':').and_then(|(_, p)| p.trim().parse().ok()))
            .expect("the relay's port");
        if link == Link::Shaped {
            shape(&["on", &format!("inside:{id}")]);
        }
        Relay { id, port }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.id])
            .stdout(Stdio::null())
            .status();
    }
}

/// `bench/bench-network.sh ARGS`.
fn shape(args: &[&str]) {
    let status = Command::new(repo().join("bench/bench-network.sh"))
        .args(args)
        .current_dir(repo())
        .status()
        .expect("bench/bench-network.sh runs");
    assert!(status.success(), "bench/bench-network.sh {} failed", args.join(" "));
}

/// The image that shapes links and relays, built when it is not.
fn network_image() -> String {
    let out = Command::new(repo().join("bench/bench-network.sh"))
        .arg("image")
        .current_dir(repo())
        .output()
        .expect("bench/bench-network.sh runs");
    assert!(
        out.status.success(),
        "the network image: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The worker-local credentials the declarations name, as the worker's environment holds them.
const SECRETS: &[(&str, &str)] = &[
    ("SM_RECEIVER_TOKEN", RECEIVER_TOKEN),
    ("SM_AWS_ID", "test"),
    ("SM_AWS_KEY", "test"),
    (
        "SM_GCS_KEY",
        r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#,
    ),
    ("SM_AZ_KEY", azure::KEY),
    ("SM_SFTP_PW", "smeltr-live-1"),
    ("SM_SQL_PW", "smeltr-live-1"),
    ("SM_EH_CONN", event_hubs::CONNECTION),
    ("SM_HEC_TOKEN", splunk::TOKEN),
    ("SM_HEC_AUTH", splunk::AUTHORIZATION),
];

/// What the sender presents to the receiver.
const RECEIVER_TOKEN: &str = "connector-speed";

/// The shipped binary as a leader with its worker inside, alone in a directory of its own. Killed, with its runtime,
/// when dropped.
struct Smeltr {
    process: Child,
    data_dir: PathBuf,
}

impl Smeltr {
    fn start(run: &str) -> Smeltr {
        use std::os::unix::process::CommandExt;
        let data_dir = repo().join("target/connector-speed").join(run);
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).unwrap();
        let process = Command::new(env!("CARGO_BIN_EXE_smeltr"))
            .env("SMELTR_LEADER_ENABLED", "1")
            .env("SMELTR_WORKER_ENABLED", "1")
            .env("SMELTR_WORKER_GROUP", "standalone")
            .env("SMELTR_LEADER_DATA_DIR", &data_dir)
            .env("SMELTR_GUI_BIND", "127.0.0.1")
            .env("SMELTR_GUI_PORT", free_port().to_string())
            .env("SMELTR_WORKER_THREAD_LIMIT", "1")
            .env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", "500")
            .envs(SECRETS.iter().copied())
            // A `smeltr.toml` in the directory it starts in would configure it.
            .current_dir(&data_dir)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(data_dir.join("console.log")).unwrap())
            .process_group(0)
            .spawn()
            .expect("spawn smeltr");
        let ca = data_dir.join("tls").join("ca.pem");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ca.exists() {
            assert!(Instant::now() < deadline, "{run}: the leader never started");
            std::thread::sleep(Duration::from_millis(100));
        }
        Smeltr { process, data_dir }
    }

    /// Deploys `source` routed unchanged to `destination`, as approving a change does.
    fn deploy(&self, source: Value, destination: Value) {
        use smeltr::leader::git;
        let repo = git::repo_path(&self.data_dir);
        let parent = git::main_head(&repo).expect("main has a commit");
        let file = |path: &str, content: String| git::FileEdit {
            path: format!("groups/standalone/{path}"),
            content: Some(content.into_bytes()),
        };
        let edits = [
            file("system.json", json!({"version": 1}).to_string()),
            file("sources.json", json!([source]).to_string()),
            file("destinations.json", json!([destination]).to_string()),
            file(
                "pipelines/p.json",
                json!({"id": "p", "name": "p", "steps": []}).to_string(),
            ),
            file(
                "routes.json",
                json!([{"id": "r", "source": "in", "pipeline": "p", "deliver": [{"to": ["out"]}]}]).to_string(),
            ),
        ];
        let actor = smeltr::leader::store::Actor::system();
        let git::Committed::At(commit) =
            git::commit_edits(&repo, "main", &parent, Some(&parent), &edits, "connector speed", &actor)
                .expect("the commit")
        else {
            panic!("the commit did not land");
        };
        git::deploy_commit(&self.data_dir, &commit, &actor).expect("the deploy");
    }

    /// Waits until the receiver on `port` takes connections. Its supervisor binds the port before the runtime serves
    /// it, so a sender's first requests may wait in the kernel's queue or be answered busy: both are retried.
    /// Waits until `count` reaches the warm-up, checking the worker is up and pinning its threads as they start.
    fn warm_up(&self, run: &str, rows: u64, cores: &Cores, count: impl Fn() -> u64) {
        wait_for(&format!("{run} warming"), warm(rows), count, || {
            self.check(run);
            self.pin(cores)
        });
    }

    fn wait_for_receiver(&self, run: &str, port: u16) {
        let deadline = Instant::now() + PATIENCE;
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            self.check(run);
            assert!(Instant::now() < deadline, "{run}: the receiver never listened");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Panics with the worker's reason when it refuses the deployment.
    fn check(&self, run: &str) {
        let console = std::fs::read_to_string(self.data_dir.join("console.log")).unwrap_or_default();
        if let Some(refused) = console.lines().find(|l| l.contains("does not load")) {
            panic!("{run}: the worker refused the deployment: {refused}");
        }
    }

    /// The process running the deployment, once there is one.
    fn runtime(&self) -> Option<u32> {
        children_of(self.process.id()).into_iter().find(|child| {
            std::fs::read(format!("/proc/{child}/environ"))
                .is_ok_and(|env| env.split(|b| *b == 0).any(|e| e == b"SMELTR_RUNTIME_CHILD=1"))
        })
    }

    /// Puts the transform thread alone on its core, once it runs.
    fn pin(&self, cores: &Cores) {
        let Some(pid) = self.runtime() else { return };
        for (tid, name, _) in threads(pid) {
            if name == TRANSFORM_THREAD {
                set_affinity(tid, &[cores.transform]);
            }
        }
    }
}

impl Drop for Smeltr {
    fn drop(&mut self) {
        // SAFETY: signals the process group `start` made, which holds only this leader and what it started.
        unsafe { libc::kill(-(self.process.id() as i32), libc::SIGKILL) };
        let _ = self.process.wait();
        // Kept after a failure: its console says why.
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}

fn children_of(pid: u32) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<u32>().ok())
        .filter(|p| {
            std::fs::read_to_string(format!("/proc/{p}/stat")).is_ok_and(|stat| {
                stat.rsplit(')').next().and_then(|rest| rest.split_whitespace().nth(1)) == Some(&pid.to_string())
            })
        })
        .collect()
}

/// Takes what the worker's HTTP destination sends, counts it and answers at once.
struct Sink {
    port: u16,
    rows: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Sink {
    fn start() -> Sink {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (rows, bytes) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let (r, b) = (rows.clone(), bytes.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                listener.set_nonblocking(true).unwrap();
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let app = axum::Router::new().fallback(move |body: axum::body::Bytes| {
                    let (r, b) = (r.clone(), b.clone());
                    async move {
                        // A body of lines, the last with or without its newline.
                        let lines = body.split(|c| *c == b'\n').filter(|l| !l.is_empty()).count();
                        r.fetch_add(lines as u64, Ordering::Relaxed);
                        b.fetch_add(body.len() as u64, Ordering::Relaxed);
                        axum::http::StatusCode::OK
                    }
                });
                axum::serve(listener, app).await.unwrap();
            });
        });
        Sink { port, rows, bytes }
    }

    fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// Requests in flight to the receiver. A receiver answers each only once its rows are delivered, so the rows they
/// carry bound what reaches the destination in a round trip: 64 held a Kafka destination 200ms away to half of what
/// it carries.
const SENDERS: usize = 256;

/// Records in one request.
const PER_REQUEST: u64 = 2000;

/// The request bodies a `Sender` reuses, built before any clock starts: what is measured is the receiver, not the
/// harness.
fn bodies(push: &Push) -> Arc<Vec<Vec<u8>>> {
    Arc::new((0..64).map(|b| (push.body)(b * PER_REQUEST, PER_REQUEST)).collect())
}

/// Posts the corpus to a receiver on this host from its threads, each request answered once the destination has its
/// records. Its threads stop when it is dropped.
struct Sender {
    acked: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    /// Requests answered busy: the receiver had more than it could take.
    busy: Arc<AtomicU64>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Sender {
    /// Sends records `from..to` to `url`, as `bodies` holds them, `senders` requests at once.
    fn start(url: &str, push: &Push, bodies: &Arc<Vec<Vec<u8>>>, senders: usize, from: u64, to: u64) -> Sender {
        let build = push.body;
        let next = Arc::new(AtomicU64::new(from));
        let (acked, bytes) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let busy = Arc::new(AtomicU64::new(0));
        let authorization = push.authorization.replace("{}", RECEIVER_TOKEN);
        let said = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        for _ in 0..senders {
            let (bodies, next, acked, bytes, busy, said, stop) = (
                bodies.clone(),
                next.clone(),
                acked.clone(),
                bytes.clone(),
                busy.clone(),
                said.clone(),
                stop.clone(),
            );
            let (url, authorization, headers) = (url.to_string(), authorization.clone(), push.headers);
            std::thread::spawn(move || {
                let agent =
                    ureq::Agent::new_with_config(ureq::Agent::config_builder().http_status_as_error(false).build());
                loop {
                    let at = next.fetch_add(PER_REQUEST, Ordering::Relaxed);
                    if at >= to || stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let count = PER_REQUEST.min(to - at);
                    // A short last request is built for its own records, so it sends no more than it is credited.
                    let short;
                    let body = if count == PER_REQUEST {
                        &bodies[(at / PER_REQUEST) as usize % bodies.len()]
                    } else {
                        short = build(at, count);
                        &short
                    };
                    // Busy is the receiver's backpressure: ask again when it says, as a sender does. Asked sooner,
                    // a full receiver spends its time refusing.
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        let mut request = agent.post(&url).header("authorization", &authorization);
                        for (name, value) in headers {
                            request = request.header(*name, *value);
                        }
                        let mut wait = Duration::from_millis(5);
                        match request.send(&body[..]) {
                            Ok(mut r) if r.status().is_success() => {
                                let _ = r.body_mut().as_reader().read_to_end(&mut Vec::new());
                                break;
                            }
                            // 429 is the Elasticsearch receiver's busy.
                            Ok(r) if r.status() == 503 || r.status() == 429 => {
                                busy.fetch_add(1, Ordering::Relaxed);
                                let after = r
                                    .headers()
                                    .get("retry-after")
                                    .and_then(|v| v.to_str().ok()?.parse().ok());
                                wait = after.map_or(wait, Duration::from_secs);
                            }
                            // Said once: a refusal that is not busy will not pass on its own.
                            Ok(mut r) if !said.swap(true, Ordering::Relaxed) => {
                                let text = r.body_mut().read_to_string().unwrap_or_default();
                                eprintln!("{url} answered {}: {text}", r.status());
                            }
                            _ => {}
                        }
                        std::thread::sleep(wait);
                    }
                    acked.fetch_add(count, Ordering::Relaxed);
                    bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
                }
            });
        }
        Sender {
            acked,
            bytes,
            busy,
            stop,
        }
    }

    fn acked(&self) -> u64 {
        self.acked.load(Ordering::Relaxed)
    }

    fn busy(&self) -> u64 {
        self.busy.load(Ordering::Relaxed)
    }

    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Writes records `from..to` as syslog lines over connections it holds open to `port`. A stream has no answer, so
/// what arrives is counted where it lands.
fn stream_syslog(port: u16, from: u64, to: u64) {
    let next = Arc::new(AtomicU64::new(from));
    for _ in 0..4 {
        let next = next.clone();
        std::thread::spawn(move || {
            use std::io::Write;
            let mut socket = std::io::BufWriter::with_capacity(
                1 << 20,
                std::net::TcpStream::connect(("127.0.0.1", port)).expect("the relay"),
            );
            loop {
                let at = next.fetch_add(PER_REQUEST, Ordering::Relaxed);
                if at >= to {
                    let _ = socket.flush();
                    return;
                }
                for i in at..(at + PER_REQUEST).min(to) {
                    let line = format!(
                        "<134>1 2026-10-01T00:00:00.000Z host-{} app - - - {}\n",
                        i % 1000,
                        line(i)
                    );
                    if socket.write_all(line.as_bytes()).is_err() {
                        return;
                    }
                }
            }
        });
    }
}

/// The test's own name for a skip message.
fn named(run: &str) -> String {
    format!("connector-speed {run}")
}

mod kafka {
    use super::*;
    use rdkafka::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    pub fn source(run: &str, rows: u64) -> Option<Value> {
        let brokers = live::kafka(&named(run))?;
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("linger.ms", "20")
            .create()
            .expect("a Kafka producer");
        for i in 0..rows {
            let line = line(i);
            let mut record = BaseRecord::<(), String>::to(run).payload(&line);
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((_, r)) => {
                        producer.poll(Duration::from_millis(50));
                        record = r;
                    }
                }
            }
            producer.poll(Duration::ZERO);
        }
        producer
            .flush(Duration::from_secs(60))
            .expect("the corpus reaches the broker");
        Some(json!({"id": "in", "type": "kafka", "brokers": brokers, "topics": [run], "groupId": run}))
    }

    pub fn destination(run: &str) -> Option<Value> {
        let brokers = live::kafka(&named(run))?;
        Some(json!({"id": "out", "type": "kafka", "brokers": brokers, "defaultTopic": run}))
    }
}

/// How long a destination that writes files keeps one open: the least it may. A destination answers a file's rows
/// when it commits the file, so the harness keeps an interval's rows in flight (`FILE_SENDERS`): the shorter the
/// interval, the fewer.
const FILE_SECS: u64 = 5;

/// Records in each file an object source reads: a log file's worth.
const FILE_ROWS: u64 = 5_000;

/// The corpus as files of lines, each named for its place in it.
fn files(rows: u64) -> Vec<(String, String)> {
    (0..rows.div_ceil(FILE_ROWS))
        .map(|f| {
            let lines = (f * FILE_ROWS..((f + 1) * FILE_ROWS).min(rows))
                .map(|i| line(i) + "\n")
                .collect();
            (format!("{f:06}.ndjson"), lines)
        })
        .collect()
}

/// One request, and its status and body.
fn call(method: &str, url: &str, headers: &[(&str, &str)], body: impl Into<Vec<u8>>) -> (u16, String) {
    let agent: ureq::Agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
    let mut request = ureq::http::Request::builder().method(method).uri(url);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let mut response = agent
        .run(request.body(body.into()).unwrap())
        .unwrap_or_else(|e| panic!("{method} {url}: {e}"));
    let status = response.status().as_u16();
    (status, response.body_mut().read_to_string().unwrap_or_default())
}

/// `call`, which must succeed.
fn ok(method: &str, url: &str, headers: &[(&str, &str)], body: impl Into<Vec<u8>>) -> String {
    let (status, text) = call(method, url, headers, body);
    assert!((200..300).contains(&status), "{method} {url}: {status} {text}");
    text
}

/// `docker compose exec` of `command` in `service`, given `input`.
fn exec(service: &str, command: &[&str], input: &[u8]) {
    use std::io::Write;
    let mut child = Command::new("docker")
        .args(["compose", "-f", "tests/live/compose.live.yaml", "exec", "-T", service])
        .args(command)
        .current_dir(repo())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("docker runs");
    // A command that fails stops reading: its status says why, not the broken pipe.
    let _ = child.stdin.take().unwrap().write_all(input);
    assert!(
        child.wait().unwrap().success(),
        "{service}: {} failed",
        command.join(" ")
    );
}

/// `run` as a name that may not hold a hyphen.
fn underscored(run: &str) -> String {
    run.replace('-', "_")
}

mod event_hubs {
    use super::*;
    use rdkafka::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    /// The emulator's own.
    pub const CONNECTION: &str = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;\
                                  SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    /// The emulator's event hubs are fixed by `tests/fixtures/event-hubs-emulator.json`.
    const HUB: &str = "smeltr-live";

    pub fn source(run: &str, rows: u64) -> Option<Value> {
        let namespace = live::event_hubs(&named(run))?;
        // The hub holds every earlier run's records too: this one reads only its own.
        let since = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &namespace)
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", "$ConnectionString")
            .set("sasl.password", CONNECTION)
            .set("linger.ms", "20")
            .create()
            .expect("an Event Hubs producer");
        for i in 0..rows {
            let line = line(i);
            let mut record = BaseRecord::<(), String>::to(HUB).payload(&line);
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((_, r)) => {
                        producer.poll(Duration::from_millis(50));
                        record = r;
                    }
                }
            }
            producer.poll(Duration::ZERO);
        }
        producer
            .flush(Duration::from_secs(120))
            .expect("the corpus reaches the hub");
        Some(
            json!({"id": "in", "type": "eventHubs", "namespace": namespace, "eventHubs": [HUB],
                    "consumerGroup": "smeltr", "connectionString": "SM_EH_CONN",
                    "start": {"type": "timestamp", "millis": since}}),
        )
    }

    pub fn destination(run: &str) -> Option<Value> {
        let namespace = live::event_hubs(&named(run))?;
        Some(
            json!({"id": "out", "type": "eventHubs", "namespace": namespace, "defaultEventHub": HUB,
                    "connectionString": "SM_EH_CONN"}),
        )
    }
}

mod s3 {
    use super::*;

    fn keys(at: &str) -> Value {
        json!({"region": "us-east-1", "endpoint": at, "accessKeyId": "SM_AWS_ID", "secretAccessKey": "SM_AWS_KEY"})
    }

    /// `decl` with the keys and endpoint every S3 declaration carries.
    fn with_keys(mut decl: Value, at: &str) -> Value {
        decl.as_object_mut()
            .unwrap()
            .extend(keys(at).as_object().unwrap().clone());
        decl
    }

    /// Makes `bucket`, or finds it made.
    fn bucket(at: &str, bucket: &str) {
        let (status, text) = call("PUT", &format!("{at}/{bucket}"), &[], Vec::new());
        assert!(status < 300 || status == 409, "bucket {bucket}: {status} {text}");
    }

    fn put(at: &str, bucket: &str, files: &[(String, String)]) {
        for (key, body) in files {
            ok("PUT", &format!("{at}/{bucket}/{key}"), &[], body.as_bytes());
        }
    }

    pub fn list_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::s3(&named(run))?;
        bucket(&at, run);
        put(&at, run, &files(rows));
        Some(with_keys(
            json!({"id": "in", "type": "s3List", "bucket": run, "pollIntervalSecs": 1}),
            &at,
        ))
    }

    pub fn sqs_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::s3(&named(run))?;
        let sqs = |action: &str, body: Value| {
            let target = format!("AmazonSQS.{action}");
            let headers = [
                ("x-amz-target", target.as_str()),
                ("content-type", "application/x-amz-json-1.0"),
            ];
            ok("POST", &at, &headers, body.to_string())
        };
        let created: Value = serde_json::from_str(&sqs("CreateQueue", json!({"QueueName": run}))).unwrap();
        let queue = created["QueueUrl"]
            .as_str()
            .expect("CreateQueue answers its URL")
            .to_string();
        bucket(&at, run);
        let files = files(rows);
        put(&at, run, &files);
        for (key, body) in &files {
            let event = json!({"Records": [{"eventVersion": "2.1", "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {"bucket": {"name": run}, "object": {"key": key, "size": body.len()}}}]});
            sqs(
                "SendMessage",
                json!({"QueueUrl": queue, "MessageBody": event.to_string()}),
            );
        }
        Some(with_keys(json!({"id": "in", "type": "s3Sqs", "queueUrl": queue}), &at))
    }

    pub fn destination(run: &str) -> Option<Value> {
        let at = live::s3(&named(run))?;
        bucket(&at, run);
        Some(with_keys(
            json!({"id": "out", "type": "s3", "bucket": run, "fileIntervalSecs": FILE_SECS}),
            &at,
        ))
    }

    pub fn iceberg(run: &str) -> Option<Value> {
        let catalog = live::iceberg(&named(run))?;
        let at = live::s3(&named(run))?;
        // The catalog's warehouse, as tests/live/compose.live.yaml gives it.
        bucket(&at, "iceberg-live");
        let (status, text) = call(
            "POST",
            &format!("{catalog}/v1/namespaces"),
            &[("content-type", "application/json")],
            json!({"namespace": ["connector_speed"]}).to_string(),
        );
        assert!(status < 300 || status == 409, "namespace: {status} {text}");
        Some(with_keys(
            json!({"id": "out", "type": "iceberg", "catalog": catalog, "namespace": "connector_speed",
                   "table": underscored(run), "fileIntervalSecs": FILE_SECS}),
            &at,
        ))
    }

    pub fn delta(run: &str) -> Option<Value> {
        let at = live::s3(&named(run))?;
        bucket(&at, "delta-live");
        Some(with_keys(
            json!({"id": "out", "type": "delta", "location": format!("s3://delta-live/{run}"),
                   "fileIntervalSecs": FILE_SECS}),
            &at,
        ))
    }
}

mod gcs {
    use super::*;

    const KEY: &str = "SM_GCS_KEY";

    fn bucket(at: &str, bucket: &str) {
        ok(
            "POST",
            &format!("{at}/storage/v1/b?project=smeltr"),
            &[("content-type", "application/json")],
            json!({"name": bucket}).to_string(),
        );
    }

    fn put(at: &str, bucket: &str, files: &[(String, String)]) {
        for (key, body) in files {
            ok(
                "POST",
                &format!("{at}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}"),
                &[("content-type", "application/x-ndjson")],
                body.as_bytes(),
            );
        }
    }

    pub fn list_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::gcs(&named(run))?;
        bucket(&at, run);
        put(&at, run, &files(rows));
        Some(
            json!({"id": "in", "type": "gcsList", "bucket": run, "endpoint": at, "serviceAccountKey": KEY,
                    "pollIntervalSecs": 1}),
        )
    }

    pub fn pubsub_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::gcs(&named(run))?;
        let pubsub = live::pubsub(&named(run))?;
        let project = format!("{pubsub}/v1/projects/smeltr");
        let json_type = [("content-type", "application/json")];
        ok("PUT", &format!("{project}/topics/{run}"), &json_type, "{}");
        ok(
            "PUT",
            &format!("{project}/subscriptions/{run}"),
            &json_type,
            json!({"topic": format!("projects/smeltr/topics/{run}")}).to_string(),
        );
        bucket(&at, run);
        let files = files(rows);
        put(&at, run, &files);
        for (key, _) in &files {
            // `e30=` is `{}`: the notification's attributes carry all it says.
            let message = json!({"data": "e30=", "attributes": {"eventType": "OBJECT_FINALIZE",
                "payloadFormat": "NONE", "bucketId": run, "objectId": key,
                "notificationConfig": "projects/_/buckets/b/notificationConfigs/1"}});
            ok(
                "POST",
                &format!("{project}/topics/{run}:publish"),
                &json_type,
                json!({"messages": [message]}).to_string(),
            );
        }
        Some(
            json!({"id": "in", "type": "gcsPubSub", "subscription": format!("projects/smeltr/subscriptions/{run}"),
                    "pubsubEndpoint": pubsub, "endpoint": at, "serviceAccountKey": KEY}),
        )
    }

    pub fn destination(run: &str) -> Option<Value> {
        let at = live::gcs(&named(run))?;
        bucket(&at, run);
        Some(
            json!({"id": "out", "type": "gcs", "bucket": run, "endpoint": at, "serviceAccountKey": KEY,
                    "fileIntervalSecs": FILE_SECS}),
        )
    }
}

mod azure {
    use super::*;

    /// Azurite's published key for `devstoreaccount1`.
    pub const KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /// A signature for every service and permission of the account, signed with `KEY`, for the harness's own
    /// requests.
    const SAS: &str = "sv=2021-08-06&ss=bq&srt=sco&sp=racwdlup&se=2099-01-01T00%3A00%3A00Z&spr=https%2Chttp&\
                       sig=%2BMnHWtCOrvDBlW0BOdq7P%2BNdl9UZ3hedCH6rBnGFXq0%3D";

    fn container(at: &str, container: &str) {
        ok(
            "PUT",
            &format!("{at}/{container}?restype=container&{SAS}"),
            &[],
            Vec::new(),
        );
    }

    fn put(at: &str, container: &str, files: &[(String, String)]) {
        for (key, body) in files {
            ok(
                "PUT",
                &format!("{at}/{container}/{key}?{SAS}"),
                &[("x-ms-blob-type", "BlockBlob")],
                body.as_bytes(),
            );
        }
    }

    fn decl(at: &str, mut fields: Value) -> Value {
        let common = json!({"account": "devstoreaccount1", "endpoint": at, "accessKey": "SM_AZ_KEY"});
        fields
            .as_object_mut()
            .unwrap()
            .extend(common.as_object().unwrap().clone());
        fields
    }

    pub fn list_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::azure(&named(run))?;
        container(&at, run);
        put(&at, run, &files(rows));
        Some(decl(
            &at,
            json!({"id": "in", "type": "azureBlobList", "container": run, "pollIntervalSecs": 1}),
        ))
    }

    pub fn queue_source(run: &str, rows: u64) -> Option<Value> {
        let at = live::azure(&named(run))?;
        let queues = live::azure_queue(&named(run))?;
        let queue = format!("{queues}/{run}");
        ok("PUT", &format!("{queue}?{SAS}"), &[], Vec::new());
        container(&at, run);
        let files = files(rows);
        put(&at, run, &files);
        for (key, _) in &files {
            let event = json!({"eventType": "Microsoft.Storage.BlobCreated", "subject": "/blobServices/default",
                "id": key, "data": {"api": "PutBlob", "url": format!("{at}/{run}/{key}"), "blobType": "BlockBlob"}});
            let text = event
                .to_string()
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;");
            ok(
                "POST",
                &format!("{queue}/messages?{SAS}"),
                &[],
                format!("<QueueMessage><MessageText>{text}</MessageText></QueueMessage>"),
            );
        }
        Some(decl(
            &at,
            json!({"id": "in", "type": "azureBlobQueue", "queueUrl": queue}),
        ))
    }

    pub fn destination(run: &str) -> Option<Value> {
        let at = live::azure(&named(run))?;
        container(&at, run);
        Some(decl(
            &at,
            json!({"id": "out", "type": "azureBlob", "container": run, "fileIntervalSecs": FILE_SECS}),
        ))
    }
}

mod sftp {
    use super::*;

    /// The server's own key, which `tests/live/compose.live.yaml` fixes.
    const HOST_KEY: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICtrmFhIzNfZykft0qVM01An049uIALQomfg4Iag8jK5 smeltr live sftp host";

    /// Makes `upload/{run}`, owned by the user the worker signs in as.
    fn directory(run: &str) -> String {
        let dir = format!("/home/smeltr/upload/{run}");
        exec(
            "sftp",
            &["sh", "-c", &format!("mkdir -p {dir} && chown 1001 {dir}")],
            b"",
        );
        dir
    }

    fn decl(run: &str, id: &str, kind: &str) -> Option<Value> {
        let (host, port) = live::sftp(&named(run))?;
        Some(
            json!({"id": id, "type": kind, "directory": format!("upload/{run}"), "host": host, "port": port,
                    "username": "smeltr", "password": "SM_SFTP_PW", "hostKey": HOST_KEY}),
        )
    }

    pub fn source(run: &str, rows: u64) -> Option<Value> {
        let mut decl = decl(run, "in", "sftp")?;
        let dir = directory(run);
        for (key, body) in files(rows) {
            let path = format!("{dir}/{key}");
            exec(
                "sftp",
                &["sh", "-c", &format!("cat > {path} && chown 1001 {path}")],
                body.as_bytes(),
            );
        }
        decl["pollIntervalSecs"] = json!(1);
        Some(decl)
    }

    pub fn destination(run: &str) -> Option<Value> {
        let mut decl = decl(run, "out", "sftp")?;
        directory(run);
        decl["fileIntervalSecs"] = json!(FILE_SECS);
        Some(decl)
    }
}

mod sql {
    use super::*;

    /// The columns every table has: `at` follows from `id`, a millisecond apart from the first of October, so rows
    /// are long settled and in order.
    fn fields() -> Value {
        json!([{"name": "id", "type": "i64"}, {"name": "at", "type": "datetime"}, {"name": "line", "type": "str"}])
    }

    fn decl(kind: &str, host: String, port: u16, table: &str) -> Value {
        json!({"id": "in", "type": kind, "host": host, "port": port, "database": "smeltr", "user": "smeltr_reader",
               "password": "SM_SQL_PW", "tls": "disable", "query": format!("SELECT id, at, line FROM {table}"),
               "mode": {"type": "incrementing", "position": ["at", "id"], "settleSecs": 1, "start": "beginning"},
               "fields": fields(), "pollIntervalSecs": 1})
    }

    pub fn postgres(run: &str, rows: u64) -> Option<Value> {
        let (host, port) = live::postgres(&named(run))?;
        let table = underscored(run);
        let mut script = format!(
            "CREATE TABLE {table} (id bigint PRIMARY KEY, \
             at timestamp GENERATED ALWAYS AS (timestamp '2026-10-01 00:00:00' + id * interval '1 millisecond') STORED, \
             line text NOT NULL);\n\
             GRANT SELECT ON {table} TO smeltr_reader;\n\
             COPY {table} (id, line) FROM STDIN;\n"
        );
        for i in 0..rows {
            // COPY's text format: a backslash escapes, and a tab or a newline would end the field.
            let escaped = line(i).replace('\\', "\\\\").replace('\t', "\\t").replace('\n', "\\n");
            script.push_str(&format!("{i}\t{escaped}\n"));
        }
        script.push_str("\\.\n");
        exec(
            "postgres",
            &["psql", "-q", "-v", "ON_ERROR_STOP=1", "-U", "smeltr", "-d", "smeltr"],
            script.as_bytes(),
        );
        Some(decl("postgres", host, port, &table))
    }

    /// Row `i`'s `at`, as MySQL writes a `DATETIME(6)`: MariaDB will not compute it from `id` as a stored column.
    fn at(i: u64) -> String {
        assert!(i < 86_400_000, "a day of milliseconds");
        format!(
            "2026-10-01 {:02}:{:02}:{:02}.{:03}000",
            i / 3_600_000,
            i / 60_000 % 60,
            i / 1000 % 60,
            i % 1000
        )
    }

    /// MySQL and MariaDB share a script: the reader may already read the whole database.
    fn mysql_like(service: &str, client: &str, host: String, port: u16, run: &str, rows: u64) -> Value {
        let table = underscored(run);
        let mut script = format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, \
             at DATETIME(6) NOT NULL, line TEXT NOT NULL);\n"
        );
        for chunk in (0..rows).collect::<Vec<_>>().chunks(500) {
            let values: Vec<String> = chunk
                .iter()
                .map(|&i| {
                    format!(
                        "({i},'{}','{}')",
                        at(i),
                        line(i).replace('\\', "\\\\").replace('\'', "\\'")
                    )
                })
                .collect();
            script.push_str(&format!(
                "INSERT INTO {table} (id, at, line) VALUES {};\n",
                values.join(",")
            ));
        }
        exec(
            service,
            &[client, "-usmeltr", "-psmeltr-live-1", "smeltr"],
            script.as_bytes(),
        );
        decl("mysql", host, port, &table)
    }

    pub fn mysql(run: &str, rows: u64) -> Option<Value> {
        let (host, port) = live::mysql(&named(run))?;
        Some(mysql_like("mysql", "mysql", host, port, run, rows))
    }

    pub fn mariadb(run: &str, rows: u64) -> Option<Value> {
        let (host, port) = live::mariadb(&named(run))?;
        Some(mysql_like("mariadb", "mariadb", host, port, run, rows))
    }
}

mod elastic {
    use super::*;

    pub fn destination(run: &str) -> Option<Value> {
        let url = live::elastic(&named(run))?;
        Some(json!({"id": "out", "type": "elasticsearch", "url": url, "defaultIndex": run}))
    }
}

mod splunk {
    use super::*;

    /// The token `tests/live/compose.live.yaml` gives HEC, with indexer acknowledgement off.
    pub const TOKEN: &str = "00000000-0000-0000-0000-000000000001";

    /// `TOKEN` as the HTTP destination presents it.
    pub const AUTHORIZATION: &str = "Splunk 00000000-0000-0000-0000-000000000001";

    pub fn hec(run: &str) -> Option<Value> {
        let splunk = live::splunk(&named(run))?;
        Some(
            json!({"id": "out", "type": "splunkHec", "url": splunk.hec, "token": "SM_HEC_TOKEN",
                    "acknowledgement": "receipt", "index": "main"}),
        )
    }

    /// The HTTP destination, posting lines to HEC's raw endpoint.
    pub fn http(run: &str) -> Option<Value> {
        let splunk = live::splunk(&named(run))?;
        Some(json!({"id": "out", "type": "http",
                    "url": format!("{}/services/collector/raw?index=main&sourcetype=connector:speed", splunk.hec),
                    "credentials": {"Authorization": "SM_HEC_AUTH"}}))
    }
}

/// How the harness sends a receiver the corpus: requests of `PER_REQUEST` records to `path`, each built by `body`.
struct Push {
    path: &'static str,
    /// The `Authorization` header, with `RECEIVER_TOKEN` where `{}` stands.
    authorization: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: fn(first: u64, count: u64) -> Vec<u8>,
}

/// The receiver's declaration on `port`, admitting `RECEIVER_TOKEN`.
type Receiver = fn(port: u16) -> Value;

mod receivers {
    use super::*;

    fn tokens() -> Value {
        json!({"bench": "SM_RECEIVER_TOKEN"})
    }

    pub fn http(port: u16) -> Value {
        json!({"id": "in", "type": "httpReceiver", "port": port, "records": "lines", "tokens": tokens()})
    }

    pub const HTTP: Push = Push {
        path: "/",
        authorization: "Bearer {}",
        headers: &[],
        body: lines,
    };

    pub fn hec(port: u16) -> Value {
        json!({"id": "in", "type": "hecReceiver", "port": port, "tokens": {"bench": {"token": "SM_RECEIVER_TOKEN"}}})
    }

    pub const HEC: Push = Push {
        path: "/services/collector/raw",
        authorization: "Splunk {}",
        headers: &[],
        body: lines,
    };

    pub fn elastic(port: u16) -> Value {
        json!({"id": "in", "type": "elasticReceiver", "port": port, "tokens": tokens()})
    }

    pub const ELASTIC: Push = Push {
        path: "/logs/_bulk",
        authorization: "Bearer {}",
        headers: &[("content-type", "application/x-ndjson")],
        body: |first, count| {
            (first..first + count)
                .flat_map(|i| format!("{{\"create\":{{}}}}\n{}\n", line(i)).into_bytes())
                .collect()
        },
    };

    pub fn otlp(port: u16) -> Value {
        json!({"id": "in", "type": "otlpReceiver", "port": port, "tokens": tokens()})
    }

    pub const OTLP: Push = Push {
        path: "/v1/logs",
        authorization: "Bearer {}",
        headers: &[("content-type", "application/json")],
        body: |first, count| {
            let records: Vec<Value> = (first..first + count)
                .map(|i| {
                    json!({"timeUnixNano": (1_790_000_000_000 + i).to_string() + "000000",
                                "body": {"stringValue": line(i)}})
                })
                .collect();
            json!({"resourceLogs": [{"resource": {}, "scopeLogs": [{"scope": {}, "logRecords": records}]}]})
                .to_string()
                .into_bytes()
        },
    };

    pub fn prometheus(port: u16) -> Value {
        json!({"id": "in", "type": "prometheusReceiver", "port": port, "tokens": tokens()})
    }

    pub const PROMETHEUS: Push = Push {
        path: "/api/v1/write",
        authorization: "Bearer {}",
        headers: &[
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "snappy"),
            ("x-prometheus-remote-write-version", "0.1.0"),
        ],
        body: remote_write,
    };

    fn lines(first: u64, count: u64) -> Vec<u8> {
        (first..first + count)
            .flat_map(|i| (line(i) + "\n").into_bytes())
            .collect()
    }

    /// A remote write 1.0 `WriteRequest` of `count` series, a sample each, snappy-compressed. Its protobuf written
    /// out by hand: field 1 of a request is a series, whose field 1 is a label and field 2 a sample.
    fn remote_write(first: u64, count: u64) -> Vec<u8> {
        fn varint(out: &mut Vec<u8>, mut v: u64) {
            while v >= 0x80 {
                out.push(v as u8 | 0x80);
                v >>= 7;
            }
            out.push(v as u8);
        }
        fn field(out: &mut Vec<u8>, number: u64, bytes: &[u8]) {
            varint(out, number << 3 | 2);
            varint(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
        fn label(name: &str, value: &str) -> Vec<u8> {
            let mut l = Vec::new();
            field(&mut l, 1, name.as_bytes());
            field(&mut l, 2, value.as_bytes());
            l
        }
        let mut request = Vec::new();
        for i in first..first + count {
            let mut series = Vec::new();
            field(&mut series, 1, &label("__name__", "connector_speed_requests_total"));
            field(&mut series, 1, &label("instance", &format!("host-{}", i % 1000)));
            field(&mut series, 1, &label("path", &format!("/api/v{}/items", i % 7)));
            let mut sample = Vec::new();
            // A double, field 1; the time in milliseconds, field 2.
            sample.push(1 << 3 | 1);
            sample.extend_from_slice(&(i as f64).to_le_bytes());
            varint(&mut sample, 2 << 3);
            varint(&mut sample, 1_790_000_000_000 + i);
            field(&mut series, 2, &sample);
            field(&mut request, 1, &series);
        }
        snap::raw::Encoder::new().compress_vec(&request).expect("snappy")
    }

    pub fn syslog(port: u16) -> Value {
        json!({"id": "in", "type": "syslogReceiver", "port": port, "allowFrom": ["0.0.0.0/0"]})
    }
}
