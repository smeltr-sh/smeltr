//! Smeltr beside the open-source engines that do its work, each timed on the same cores, the same memory and the
//! same records: `just engine-speed`. Every engine runs in a container of the same shape. It takes the corpus either
//! over HTTP, pushed by the harness's sender, or from a Kafka topic it pulls, and it delivers what its pipeline makes
//! the same way: to the harness's sink, or to a topic of its own. A row is one engine running one pipeline over one
//! transport on a number of cores.
//!
//! ```mermaid
//! flowchart LR
//!   sender -- HTTP --> e1[engine] -- HTTP --> sink
//!   t1[(input topic)] -- Kafka --> e2[engine] -- Kafka --> t2[(output topic)]
//! ```
//!
//! Each engine's pipelines are in this directory and its transports below, each tuned as the engine's own
//! documentation tunes it for throughput, so its maintainers can read what ran and say what they would change. Before
//! any row is timed, each engine's output for the first records of the corpus is checked against what the pipeline
//! must make: a row is only a row when every engine does the same work.

use super::*;

/// What `just engine-speed` compares. Each runs the image named here, pinned so a rerun runs the same.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Engine {
    /// The `dist` binary, in `smeltr/Dockerfile`'s image: the distribution it was built on.
    Smeltr,
    /// The Apache-2.0 distribution, without X-Pack.
    Logstash,
    Vector,
    /// Mapping in a processor of its own, packaged as a NAR, as a NiFi shop builds one.
    Nifi,
    /// Mapping in a Groovy script, as stock NiFi does with no code of its own.
    NifiGroovy,
}

const ENGINES: [Engine; 5] = [
    Engine::Smeltr,
    Engine::Logstash,
    Engine::Vector,
    Engine::Nifi,
    Engine::NifiGroovy,
];

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Smeltr => "smeltr",
            Engine::Logstash => "logstash",
            Engine::Vector => "vector",
            Engine::Nifi => "nifi",
            Engine::NifiGroovy => "nifi-groovy",
        }
    }

    fn image(self) -> &'static str {
        match self {
            Engine::Smeltr => "smeltr-engine-speed",
            Engine::Logstash => "docker.elastic.co/logstash/logstash-oss:9.5.6",
            Engine::Vector => "timberio/vector:0.62.0-debian",
            Engine::Nifi | Engine::NifiGroovy => "apache/nifi:2.12.0",
        }
    }

    /// The JVM engines' heap: half the container, as their documentation advises.
    fn heap(gb: u64) -> String {
        format!("{}m", gb * 1024 / 2)
    }

    /// `tests/engines/{engine}/{pipeline}.{extension}`, when the pipeline needs one for this engine.
    fn pipeline(self, w: &Workload, extension: &str) -> Option<String> {
        let dir = match self {
            Engine::NifiGroovy => "nifi",
            _ => self.label(),
        };
        let path = repo()
            .join("tests/engines")
            .join(dir)
            .join(format!("{}.{extension}", w.name));
        std::fs::read_to_string(path).ok()
    }

    /// Whether the engine runs `w` over `t`: NiFi's Groovy row only where it maps, since it forwards as NiFi does;
    /// another source or destination only where both engines have it, smeltr and Vector.
    fn runs(self, w: &Workload, t: Transport) -> bool {
        (w.over)(t)
            && (self != Engine::NifiGroovy || self.pipeline(w, "groovy").is_some())
            && (t.shared() || matches!(self, Engine::Smeltr | Engine::Vector))
    }
}

/// Where an engine takes records: HTTP and Kafka for every engine, and each other source smeltr and Vector share.
#[derive(Clone, Copy, PartialEq, Debug)]
enum In {
    /// Pushed by the harness's sender, each request answered by the engine.
    Http,
    /// Pulled from the topic the harness seeded, on the broker the run starts.
    Kafka,
    /// Splunk's HTTP Event Collector, its raw endpoint taking lines.
    Hec,
    /// OpenTelemetry logs over HTTP, a record the body of each.
    Otlp,
    /// Prometheus remote write, a record each sample.
    Prometheus,
    /// Syslog lines over TCP, on connections held open.
    Syslog,
}

impl In {
    fn label(self) -> &'static str {
        match self {
            In::Http => "http",
            In::Kafka => "kafka",
            In::Hec => "hec",
            In::Otlp => "otlp",
            In::Prometheus => "prometheus",
            In::Syslog => "syslog",
        }
    }

    /// How the harness sends a receiver the corpus, for each engine its own path and credentials; `None` for a
    /// stream of lines rather than requests.
    fn push(self, engine: Engine) -> Option<Push> {
        let push = match self {
            In::Http => {
                let (path, authorization) = match engine {
                    Engine::Smeltr => ("/", "Bearer {}"),
                    Engine::Logstash | Engine::Vector => ("/", BASIC),
                    Engine::Nifi | Engine::NifiGroovy => ("/in", "Bearer {}"),
                };
                Push {
                    path,
                    authorization,
                    ..receivers::HTTP
                }
            }
            In::Hec => receivers::HEC,
            In::Otlp => receivers::OTLP,
            // Vector serves remote write at its root.
            In::Prometheus if engine == Engine::Vector => Push {
                path: "/",
                authorization: BASIC,
                ..receivers::PROMETHEUS
            },
            In::Prometheus => receivers::PROMETHEUS,
            In::Kafka | In::Syslog => return None,
        };
        Some(push)
    }

    /// What a row that forwards delivers of record `i` of the corpus, sent this way: the record as it was sent.
    fn sent(self, i: u64) -> Value {
        match self {
            In::Syslog => Value::String(syslog_line(i)),
            In::Prometheus => json!({
                "name": "connector_speed_requests_total",
                "labels": {"instance": format!("host-{}", i % 1000), "path": format!("/api/v{}/items", i % 7)},
                "value": i as f64,
                "time": utc_millis(1_790_000_000_000 + i),
            }),
            In::Http | In::Kafka | In::Hec | In::Otlp => serde_json::from_str(&line(i)).unwrap(),
        }
    }

    /// An engine's document in the form `sent` writes it. Smeltr and Vector each write a sample as JSON of their own.
    fn normal(self, doc: Value) -> Value {
        if self != In::Prometheus {
            return doc;
        }
        let labels = ["labels", "attributes", "tags"].iter().find_map(|k| doc.get(*k));
        // Vector reads a sample as a gauge, or as a counter where its name says it counts.
        let value = ["/value", "/gauge/value", "/counter/value"]
            .iter()
            .find_map(|p| doc.pointer(p));
        let time = doc.get("time").or_else(|| doc.get("timestamp"));
        json!({
            "name": doc["name"],
            "labels": labels,
            "value": value,
            "time": time.and_then(Value::as_str).map(instant),
        })
    }
}

/// Where an engine delivers records.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Out {
    /// To the harness's sink.
    Http,
    /// To a topic of the run's own, on the broker the run starts.
    Kafka,
    /// Bulk requests, to the harness's sink speaking Elasticsearch's bulk API.
    Elasticsearch,
    /// HEC event requests, to the harness's sink speaking Splunk's HTTP Event Collector.
    SplunkHec,
    /// Remote write, to the harness's sink.
    Prometheus,
    /// OpenTelemetry logs over HTTP, to the harness's sink.
    Otlp,
    /// Objects, to an S3 the harness starts.
    S3,
    /// Blobs, to an Azure Blob Storage emulator the harness starts.
    AzureBlob,
}

impl Out {
    fn label(self) -> &'static str {
        match self {
            Out::Http => "http",
            Out::Kafka => "kafka",
            Out::Elasticsearch => "elasticsearch",
            Out::SplunkHec => "splunk-hec",
            Out::Prometheus => "prometheus",
            Out::Otlp => "otlp",
            Out::S3 => "s3",
            Out::AzureBlob => "azure-blob",
        }
    }
}

/// How records reach an engine and leave it. Each source and each destination is timed alone, HTTP its other end: the
/// leg every engine runs fastest, so the leg under test sets the row's speed.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Transport {
    from: In,
    to: Out,
}

const TRANSPORTS: &[Transport] = &[
    Transport {
        from: In::Http,
        to: Out::Http,
    },
    Transport {
        from: In::Kafka,
        to: Out::Kafka,
    },
    Transport {
        from: In::Hec,
        to: Out::Http,
    },
    Transport {
        from: In::Otlp,
        to: Out::Http,
    },
    Transport {
        from: In::Prometheus,
        to: Out::Http,
    },
    Transport {
        from: In::Syslog,
        to: Out::Http,
    },
    Transport {
        from: In::Http,
        to: Out::Elasticsearch,
    },
    Transport {
        from: In::Http,
        to: Out::SplunkHec,
    },
    Transport {
        from: In::Http,
        to: Out::S3,
    },
    // Not GCS: its emulator refuses the multipart uploads smeltr writes an object of more than one part with.
    Transport {
        from: In::Http,
        to: Out::AzureBlob,
    },
    // Only samples leave as samples, and only OpenTelemetry's records as its records.
    Transport {
        from: In::Prometheus,
        to: Out::Prometheus,
    },
    Transport {
        from: In::Otlp,
        to: Out::Otlp,
    },
];

impl Transport {
    /// `http` and `kafka` for the transports every engine runs, and `prometheus` and `otlp`, both legs one protocol;
    /// otherwise the leg under test, `hec-in` or `s3-out`.
    fn label(self) -> String {
        match (self.from, self.to) {
            (In::Http, Out::Http) => "http".into(),
            (In::Kafka, Out::Kafka) => "kafka".into(),
            (from, Out::Http) => format!("{}-in", from.label()),
            (from, to) if from.label() == to.label() => from.label().into(),
            (_, to) => format!("{}-out", to.label()),
        }
    }

    fn kafka(self) -> bool {
        self.from == In::Kafka || self.to == Out::Kafka
    }

    /// Whether every engine has both legs: Logstash and NiFi are timed over HTTP and Kafka alone.
    fn shared(self) -> bool {
        matches!((self.from, self.to), (In::Http, Out::Http) | (In::Kafka, Out::Kafka))
    }
}

/// Where one engine takes its records and delivers them.
struct Ends {
    take: Take,
    give: Give,
}

impl Ends {
    /// Where it delivers, when to the harness's sink.
    fn give_to(&self) -> Option<Out> {
        match self.give {
            Give::Sink { to, .. } => Some(to),
            Give::Kafka { .. } | Give::Store(_) => None,
        }
    }

    fn from(&self) -> In {
        match self.take {
            Take::Listen { from, .. } => from,
            Take::Kafka { .. } => In::Kafka,
        }
    }
}

enum Take {
    /// A listener on `port`, taking what `from` sends it.
    Listen { port: u16, from: In },
    Kafka {
        brokers: String,
        topic: String,
        group: String,
    },
}

enum Give {
    /// The harness's sink at `url`, speaking `to`'s protocol.
    Sink {
        url: String,
        to: Out,
    },
    /// An object store the run started.
    Store(stores::Store),
    Kafka {
        brokers: String,
        topic: String,
    },
}

/// What a pipeline makes of record `line`, as the check compares it: `None` when the pipeline drops it.
type Expect = fn(line: &str) -> Option<Value>;

/// One pipeline, as each engine runs it.
struct Workload {
    name: &'static str,
    /// Smeltr's steps for it.
    steps: fn() -> Value,
    expect: Expect,
    /// An engine's document, put in the form `expect` writes.
    normal: fn(Value) -> Value,
    /// Each record leaves as it came: every engine sends the bytes it read, wrapped in nothing of its own.
    forwards: bool,
    /// Whether it runs over `t`.
    over: fn(t: Transport) -> bool,
}

const WORKLOADS: &[Workload] = &[
    Workload {
        name: "passthrough",
        steps: || json!([]),
        expect: |line| serde_json::from_str(line).ok(),
        normal: |doc| doc,
        forwards: true,
        // And each source delivering to the sink, each protocol of its own records to itself, and each store of lines.
        over: |t| !matches!(t.to, Out::Elasticsearch | Out::SplunkHec),
    },
    Workload {
        name: "json",
        steps: || {
            json!([{"id": "parse", "type": "parser", "input": "_raw", "kind": {"type": "json"}},
                   {"id": "unraw", "type": "eval", "assignments": [{"target": "_raw", "expr": "null"}]}])
        },
        expect: |line| serde_json::from_str(line).ok(),
        normal: |doc| doc,
        forwards: false,
        // Each destination of documents, a document each record, taken from the sender.
        over: |t| matches!(t.to, Out::Elasticsearch | Out::SplunkHec),
    },
    Workload {
        name: "json_app",
        steps: || {
            let bundle: Value = serde_json::from_str(&(reference::json_app::PIPELINE.bundle)()).unwrap();
            bundle["pipelines"][0]["steps"].clone()
        },
        expect: json_app,
        normal: |mut doc| {
            if let Some(at) = doc.get_mut("@timestamp") {
                *at = Value::String(instant(at.as_str().unwrap_or_default()));
            }
            doc
        },
        forwards: false,
        over: Transport::shared,
    },
];

/// `2026-10-01T00:00:01.727000Z` and `2026-10-01T00:00:01.727+00:00` alike as `2026-10-01T00:00:01.727Z`: the engines
/// write a UTC instant each in its own way. Any other instant is returned as it is, so it differs from what is wanted.
fn instant(at: &str) -> String {
    let Some(zone) = ["Z", "+00:00", "-00:00"].into_iter().find(|z| at.ends_with(z)) else {
        return at.to_string();
    };
    let at = &at[..at.len() - zone.len()];
    let (seconds, rest) = at.split_at(at.len().min(19));
    let fraction = match rest.strip_prefix('.') {
        Some(f) if f.chars().all(|c| c.is_ascii_digit()) => f.trim_end_matches('0'),
        None if rest.is_empty() => "",
        _ => return at.to_string(),
    };
    match fraction {
        "" => format!("{seconds}Z"),
        f => format!("{seconds}.{f}Z"),
    }
}

/// `tests/reference/json_app.rs`'s ECS document for `line`, as its `check` describes it.
fn json_app(line: &str) -> Option<Value> {
    let src: Value = serde_json::from_str(line).unwrap();
    if src["level"] == "debug" {
        return None;
    }
    let mut labels = json!({"attempt": src["attempt"], "tier": src["user"]["tier"]});
    if let Some(order) = src.get("order_id") {
        labels["order_id"] = order.clone();
    }
    let mut doc = json!({
        "@timestamp": instant(src["ts"].as_str().unwrap()),
        "event": {"duration": (src["latency_ms"].as_f64().unwrap() * 1e6).round() as i64},
        "labels": labels,
        "log": {"level": src["level"], "logger": src["logger"]},
        "message": src["msg"],
        "user": {"id": src["user"]["id"]},
    });
    if let Some(e) = src.get("error") {
        doc["error"] = json!({"type": e["type"], "message": e["message"], "stack_trace": e["stack"]});
    }
    Some(doc)
}

/// `millis` after the Unix epoch as a UTC instant, `2026-09-21T14:13:20.001Z`, as `instant` writes one.
pub fn utc_millis(millis: u64) -> String {
    let (secs, ms) = (millis / 1000, millis % 1000);
    let (days, day) = ((secs / 86_400) as i64, secs % 86_400);
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let at = format!(
        "{y}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        day / 3600,
        day / 60 % 60,
        day % 60
    );
    instant(&at)
}

/// `value` with every object's keys in order, as text: two documents are the same when these are.
fn canonical(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(o) => {
                let mut pairs: Vec<(&String, &Value)> = o.iter().collect();
                pairs.sort_by_key(|p| p.0);
                Value::Object(pairs.into_iter().map(|(k, v)| (k.clone(), sorted(v))).collect())
            }
            Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
            v => v.clone(),
        }
    }
    sorted(value).to_string()
}

/// The cores each row may give an engine, from `ENGINE_CORES`.
fn core_counts() -> Vec<usize> {
    std::env::var("ENGINE_CORES").map_or(vec![1, 2], |t| {
        t.split(',')
            .map(|n| n.trim().parse().expect("ENGINE_CORES is a list of numbers"))
            .collect()
    })
}

/// The online CPUs of each physical core, its hyperthreads together. A row gives an engine whole cores, so every
/// engine has the same hyperthreads, and none shares a core with the harness.
fn cores_by_cpu() -> Vec<Vec<usize>> {
    let online = cpu_list(&std::fs::read_to_string("/sys/devices/system/cpu/online").expect("online CPUs"));
    let mut cores: Vec<Vec<usize>> = Vec::new();
    for &cpu in &online {
        if cores.iter().any(|c| c.contains(&cpu)) {
            continue;
        }
        let siblings = std::fs::read_to_string(format!(
            "/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
        ))
        .map(|s| cpu_list(&s))
        .unwrap_or_else(|_| vec![cpu]);
        cores.push(siblings.into_iter().filter(|c| online.contains(c)).collect());
    }
    cores
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).map_or(default, |v| v.parse().unwrap_or_else(|_| panic!("{name} is not {v}")))
}

/// Records each row times, after its warm-up.
fn rows() -> u64 {
    env_or("ENGINE_ROWS", 2_000_000)
}

/// Each engine warms for this long and for a tenth of its rows, whichever is longer: the JVM engines compile their
/// hot paths as they run.
const WARM_SECS: u64 = 20;

/// Seconds an engine keeps adding to an object before it writes it: smeltr's file interval, Vector's batch timeout.
const STORE_SECS: u64 = 5;

/// The longest a run may take, its warm-up, timing and delivering what it took. The slowest takes a few minutes; one
/// that takes this long is failing.
const RUN_LIMIT: Duration = Duration::from_secs(20 * 60);

/// Records the check sends each engine.
const CHECK_ROWS: u64 = 20_000;

/// The password the sender presents to engines that take a user and password, as `BASIC` carries it.
const PASSWORD: &str = RECEIVER_TOKEN;

/// `bench:connector-speed`, as basic authentication sends it.
const BASIC: &str = "Basic YmVuY2g6Y29ubmVjdG9yLXNwZWVk";

/// A container the harness started. Removed, with its volumes, when dropped: NiFi's image keeps its repositories in
/// volumes of their own.
struct Container {
    id: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", "-v", &self.id])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Container {
    /// Runs `docker`, returning the container it started.
    fn run(mut docker: Command, run: &str) -> Container {
        let out = docker.output().expect("docker runs");
        assert!(
            out.status.success(),
            "{run}: the container did not start: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Container {
            id: String::from_utf8(out.stdout).unwrap().trim().to_string(),
        }
    }

    /// Panics with the end of the container's log once it has stopped.
    fn check(&self, what: &str) {
        let out = Command::new("docker")
            .args([
                "inspect",
                "-f",
                "{{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}",
                &self.id,
            ])
            .output()
            .expect("docker runs");
        let state = String::from_utf8_lossy(&out.stdout);
        let state: Vec<&str> = state.split_whitespace().collect();
        if state.first() != Some(&"true") {
            let why = match state[..] {
                [_, "true", _] => "out of memory".to_string(),
                [_, _, code] => format!("exit {code}"),
                _ => "gone".to_string(),
            };
            panic!("{what}: the container stopped ({why}): {}", self.log_tail());
        }
    }

    fn log_tail(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", "--tail", "20", &self.id])
            .output()
            .expect("docker runs");
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr)
    }

    /// CPU seconds and the most memory the container has used, as its cgroup counts them.
    fn usage(&self) -> (f64, u64) {
        // cgroup v2 under systemd, then v1.
        let peak = [
            format!("/sys/fs/cgroup/system.slice/docker-{}.scope/memory.peak", self.id),
            format!("/sys/fs/cgroup/memory/docker/{}/memory.max_usage_in_bytes", self.id),
        ]
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok()?.trim().parse().ok())
        .unwrap_or(0);
        (containers_cpu(std::slice::from_ref(&self.id)), peak)
    }
}

/// A container on `cpus` with `gb` of memory and the host's network.
fn container(run: &str, cpus: &[usize], gb: u64) -> Command {
    let mut docker = Command::new("docker");
    let list: Vec<String> = cpus.iter().map(|c| c.to_string()).collect();
    docker.args([
        "run",
        "-d",
        "--name",
        run,
        "--network",
        "host",
        "--cpuset-cpus",
        &list.join(","),
        "--memory",
        &format!("{gb}g"),
        "--memory-swap",
        &format!("{gb}g"),
    ]);
    docker
}

/// The Kafka broker every Kafka row shares, on the harness's cores. Its log is in memory, so a row measures the
/// engine, not the disk, and its CPU is reported beside each row's.
struct Broker {
    container: Container,
    brokers: String,
    runtime: tokio::runtime::Runtime,
}

/// Partitions in every topic: at least as many as the hyperthreads the most cores a row gives an engine have, so each
/// consumer thread can have its own, and never fewer than 16.
fn partitions() -> i32 {
    let (cores, most) = (cores_by_cpu(), *core_counts().iter().max().unwrap());
    cores[..most.min(cores.len())].concat().len().max(16) as i32
}

impl Broker {
    fn start(run: &str, cpus: &[usize]) -> Broker {
        let (port, controller) = (free_port(), free_port());
        let mut docker = container(run, cpus, env_or("ENGINE_BROKER_MEMORY_GB", 8));
        docker
            .args(["--tmpfs", "/var/lib/kafka/data:size=6g,uid=1000"])
            .args(["-e", "KAFKA_NODE_ID=1", "-e", "KAFKA_PROCESS_ROLES=broker,controller"])
            .args([
                "-e",
                &format!("KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}"),
            ])
            .args([
                "-e",
                &format!("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}"),
            ])
            .args([
                "-e",
                &format!("KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:{controller}"),
            ])
            .args(["-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER"])
            .args([
                "-e",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            ])
            .args([
                "-e",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            ])
            .args([
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                "-e",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            ])
            .args([
                "-e",
                "KAFKA_LOG_DIRS=/var/lib/kafka/data",
                "-e",
                "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
            ])
            .args(["-e", &format!("KAFKA_NUM_PARTITIONS={}", partitions())])
            // A deleted output topic frees its tmpfs at once rather than a minute later, by when the next run's
            // output would fill it.
            .args(["-e", "KAFKA_LOG_SEGMENT_DELETE_DELAY_MS=0"])
            .arg("apache/kafka:4.4.0");
        let broker = Broker {
            container: Container::run(docker, run),
            brokers: format!("127.0.0.1:{port}"),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        };
        let deadline = Instant::now() + PATIENCE;
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            broker.container.check(run);
            assert!(Instant::now() < deadline, "{run}: the broker never listened");
            std::thread::sleep(Duration::from_millis(200));
        }
        broker
    }

    fn admin(&self) -> rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext> {
        rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .expect("a Kafka admin client")
    }

    fn create(&self, topic: &str) {
        use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
        let admin = self.admin();
        let made = self.runtime.block_on(admin.create_topics(
            &[NewTopic::new(topic, partitions(), TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(30))),
        ));
        for result in made.expect("the broker answers") {
            result.unwrap_or_else(|(t, e)| panic!("topic {t}: {e}"));
        }
    }

    fn delete(&self, topic: &str) {
        let admin = self.admin();
        let _ = self
            .runtime
            .block_on(admin.delete_topics(&[topic], &rdkafka::admin::AdminOptions::new()));
    }

    /// Writes records `0..rows` of the corpus to `topic`.
    fn seed(&self, topic: &str, rows: u64) {
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        let producer: BaseProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("linger.ms", "20")
            .set("queue.buffering.max.messages", "1000000")
            .create()
            .expect("a Kafka producer");
        let partitions = partitions();
        for i in 0..rows {
            let line = line(i);
            // Spread evenly, so no partition is left to drain alone at the end.
            let mut record = BaseRecord::<(), String>::to(topic)
                .payload(&line)
                .partition((i % partitions as u64) as i32);
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
            .expect("the corpus reaches the broker");
    }

    fn consumer(&self) -> rdkafka::consumer::BaseConsumer {
        rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", "engine-speed-harness")
            .set("enable.auto.commit", "false")
            .create()
            .expect("a Kafka consumer")
    }

    /// Records in `topic`, every partition's end added up.
    fn count(&self, consumer: &rdkafka::consumer::BaseConsumer, topic: &str) -> u64 {
        use rdkafka::consumer::Consumer;
        (0..partitions())
            .map(|p| {
                consumer
                    .fetch_watermarks(topic, p, Duration::from_secs(10))
                    .map_or(0, |(_, high)| high as u64)
            })
            .sum()
    }

    /// Every record in `topic`, which holds `count` of them, as documents.
    fn read(&self, topic: &str, count: u64) -> Vec<Value> {
        use rdkafka::Message;
        use rdkafka::consumer::Consumer;
        let consumer = self.consumer();
        let mut assignment = rdkafka::TopicPartitionList::new();
        for p in 0..partitions() {
            assignment
                .add_partition_offset(topic, p, rdkafka::Offset::Beginning)
                .unwrap();
        }
        consumer.assign(&assignment).expect("the assignment");
        let mut docs = Vec::new();
        let deadline = Instant::now() + PATIENCE;
        while (docs.len() as u64) < count {
            assert!(Instant::now() < deadline, "{topic}: read {} of {count}", docs.len());
            if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500)) {
                docs.push(
                    m.payload()
                        .and_then(|p| serde_json::from_slice(p).ok())
                        .unwrap_or(Value::Null),
                );
            }
        }
        docs
    }
}

/// An engine running in its container, taking records at `ends`.
struct Running {
    container: Container,
    /// Where the sender posts, and how, to a receiver that takes requests.
    push: Option<(String, Push)>,
}

/// Starts `engine` running `workload` on `cpus` between `ends`, and waits until it takes records.
fn start(engine: Engine, w: &Workload, run: &str, cpus: &[usize], ends: &Ends) -> Running {
    let gb = env_or("ENGINE_MEMORY_GB", 4);
    let cores = cpus.len();
    let dir = repo().join("target/engine-speed").join(run);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // NiFi serves its API on a port of its own.
    let api_port = free_port();
    let mut docker = container(run, cpus, gb);
    match engine {
        Engine::Smeltr => {
            // SAFETY: neither reads anything but this process's own credentials.
            let user = unsafe { format!("{}:{}", libc::getuid(), libc::getgid()) };
            let dir_s = dir.to_str().unwrap();
            docker
                .args(["--user", &user, "-w", dir_s, "-v", &format!("{dir_s}:{dir_s}")])
                .args([
                    "-v",
                    &format!("{}:/usr/local/bin/smeltr:ro", env!("CARGO_BIN_EXE_smeltr")),
                ])
                .args([
                    "-e",
                    "HOME=/tmp",
                    "-e",
                    "SMELTR_LEADER_ENABLED=1",
                    "-e",
                    "SMELTR_WORKER_ENABLED=1",
                ])
                .args([
                    "-e",
                    "SMELTR_WORKER_GROUP=standalone",
                    "-e",
                    &format!("SMELTR_LEADER_DATA_DIR={dir_s}"),
                ])
                .args([
                    "-e",
                    "SMELTR_GUI_BIND=127.0.0.1",
                    "-e",
                    &format!("SMELTR_GUI_PORT={}", free_port()),
                ])
                .args(["-e", &format!("SMELTR_WORKER_THREAD_LIMIT={cores}")])
                .args(SECRETS.iter().flat_map(|(k, v)| ["-e".to_string(), format!("{k}={v}")]))
                .args([engine.image(), "/usr/local/bin/smeltr"]);
        }
        Engine::Logstash => {
            std::fs::create_dir_all(dir.join("pipeline")).unwrap();
            std::fs::write(dir.join("pipeline/p.conf"), logstash::pipeline(w, ends, cores)).unwrap();
            std::fs::write(dir.join("logstash.yml"), logstash::settings(cores)).unwrap();
            docker
                .args([
                    "-v",
                    &format!("{}:/usr/share/logstash/pipeline:ro", dir.join("pipeline").display()),
                ])
                .args([
                    "-v",
                    &format!(
                        "{}:/usr/share/logstash/config/logstash.yml:ro",
                        dir.join("logstash.yml").display()
                    ),
                ])
                .args(["-e", &format!("LS_JAVA_OPTS=-Xms{0} -Xmx{0}", Engine::heap(gb))])
                .arg(engine.image());
        }
        Engine::Vector => {
            std::fs::write(dir.join("vector.json"), vector::config(w, ends).to_string()).unwrap();
            docker
                .args([
                    "-v",
                    &format!("{}:/etc/engine/vector.json:ro", dir.join("vector.json").display()),
                ])
                .args([engine.image(), "--config", "/etc/engine/vector.json", "--threads"])
                .arg(cores.to_string());
        }
        Engine::Nifi | Engine::NifiGroovy => {
            if engine == Engine::Nifi {
                docker.args([
                    "-v",
                    &format!(
                        "{}:/opt/nifi/nifi-current/nar_extensions/json-app.nar:ro",
                        nifi::nar().display()
                    ),
                ]);
            }
            docker
                .args(["-e", &format!("NIFI_WEB_HTTPS_PORT={api_port}")])
                .args(["-e", "NIFI_WEB_HTTPS_HOST=127.0.0.1"])
                .args(["-e", "SINGLE_USER_CREDENTIALS_USERNAME=bench"])
                .args(["-e", &format!("SINGLE_USER_CREDENTIALS_PASSWORD={}", nifi::PASSWORD)])
                .args(["-e", &format!("NIFI_JVM_HEAP_INIT={}", Engine::heap(gb))])
                .args(["-e", &format!("NIFI_JVM_HEAP_MAX={}", Engine::heap(gb))])
                // NiFi archives the content it has sent, and stops taking more while the disk is over half full,
                // which says how full the host's disk is rather than how fast NiFi is.
                .args(["--entrypoint", "sh", engine.image(), "-c"])
                .arg(
                    "sed -i 's/^nifi.content.repository.archive.enabled=.*/nifi.content.repository.archive.enabled=false/' \
                     conf/nifi.properties && exec ../scripts/start.sh",
                );
        }
    }
    let container = Container::run(docker, run);
    match engine {
        Engine::Smeltr => {
            let ca = dir.join("tls").join("ca.pem");
            let deadline = Instant::now() + PATIENCE;
            while !ca.exists() {
                container.check(run);
                assert!(Instant::now() < deadline, "{run}: the leader never started");
                std::thread::sleep(Duration::from_millis(100));
            }
            let source = match &ends.take {
                Take::Listen { port, from } => match from {
                    In::Http => receivers::http(*port),
                    In::Hec => receivers::hec(*port),
                    In::Otlp => receivers::otlp(*port),
                    In::Prometheus => receivers::prometheus(*port),
                    In::Syslog => receivers::syslog(*port),
                    In::Kafka => unreachable!("Kafka is pulled"),
                },
                Take::Kafka { brokers, topic, group } => {
                    json!({"id": "in", "type": "kafka", "brokers": brokers, "topics": [topic], "groupId": group})
                }
            };
            let mut destination = match &ends.give {
                Give::Sink { url, to } => match to {
                    Out::Http => json!({"id": "out", "type": "http", "url": format!("{url}/")}),
                    Out::Elasticsearch => {
                        json!({"id": "out", "type": "elasticsearch", "url": url, "defaultIndex": "bench"})
                    }
                    Out::SplunkHec => json!({"id": "out", "type": "splunkHec", "url": url, "token": "SM_HEC_TOKEN",
                            "acknowledgement": "receipt", "index": "main"}),
                    Out::Prometheus => json!({"id": "out", "type": "prometheus", "url": format!("{url}/api/v1/write")}),
                    Out::Otlp => json!({"id": "out", "type": "otlp", "url": url}),
                    Out::Kafka | Out::S3 | Out::AzureBlob => unreachable!("{to:?} is no sink"),
                },
                Give::Kafka { brokers, topic } => {
                    json!({"id": "out", "type": "kafka", "brokers": brokers, "defaultTopic": topic})
                }
                Give::Store(store) => {
                    // Its files are finished as often as Vector's batches, so neither holds the run's last records
                    // for the minutes each holds them by default.
                    let common = json!({"id": "out", "fileIntervalSecs": STORE_SECS});
                    let mut d = match store.to {
                        Out::S3 => json!({"type": "s3", "bucket": store.bucket, "region": "us-east-1",
                                "endpoint": store.url, "accessKeyId": "SM_AWS_ID", "secretAccessKey": "SM_AWS_KEY"}),
                        _ => json!({"type": "azureBlob", "container": store.bucket, "account": stores::ACCOUNT,
                                "endpoint": store.url, "accessKey": "SM_AZ_KEY"}),
                    };
                    d.as_object_mut().unwrap().extend(common.as_object().unwrap().clone());
                    d
                }
            };
            if w.forwards
                && matches!(
                    ends.give,
                    Give::Kafka { .. } | Give::Sink { to: Out::Http, .. } | Give::Store(_)
                )
            {
                match ends.from() {
                    // A sample is a record of fields, which only JSON writes.
                    In::Prometheus => {}
                    In::Otlp => {
                        destination["format"] = json!("text");
                        destination["field"] = json!("body");
                    }
                    _ => destination["format"] = json!("text"),
                }
            }
            deploy(&dir, source, destination, (w.steps)());
        }
        Engine::Nifi | Engine::NifiGroovy => nifi::build(&container, run, api_port, ends, w, cores, engine),
        Engine::Logstash | Engine::Vector => {}
    }
    let push = match ends.take {
        Take::Listen { port, from } => {
            let deadline = Instant::now() + PATIENCE;
            while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
                container.check(run);
                assert!(
                    Instant::now() < deadline,
                    "{run}: the engine never listened: {}",
                    container.log_tail()
                );
                std::thread::sleep(Duration::from_millis(200));
            }
            from.push(engine)
                .map(|push| (format!("http://127.0.0.1:{port}{}", push.path), push))
        }
        Take::Kafka { .. } => None,
    };
    Running { container, push }
}

/// Logstash's pipeline and settings, as its performance tuning guide sets them for throughput.
mod logstash {
    use super::*;

    /// The transport's input, the pipeline's filters, and the transport's output.
    pub fn pipeline(w: &Workload, ends: &Ends, cores: usize) -> String {
        let mut filter = Engine::Logstash.pipeline(w, "conf").unwrap_or_default();
        let (input, output) = match (&ends.take, &ends.give) {
            (
                Take::Listen { port, from: In::Http },
                Give::Sink {
                    url: sink,
                    to: Out::Http,
                },
            ) => {
                // Every request is lines, whatever its content type says. Logstash posts many events to a request
                // only as a JSON array of them, so a record it forwards is read as the JSON it is, and leaves
                // without the fields the input adds. Those go at the top level, apart from the record's own `http`.
                let codec = if w.forwards {
                    filter =
                        "filter { mutate { remove_field => [\"@timestamp\", \"@version\", \"headers\", \"host\"] } }"
                            .to_string();
                    "json_lines ecs_compatibility => disabled"
                } else {
                    "line"
                };
                (
                    format!(
                        "http {{ port => {port} user => \"bench\" password => \"{PASSWORD}\" codec => {codec} \
                         additional_codecs => {{}} max_pending_requests => 1024 }}"
                    ),
                    format!(
                        "http {{ url => \"{sink}\" http_method => \"post\" format => \"json_batch\" pool_max => 64 }}"
                    ),
                )
            }
            // A consumer thread for each core. The producer batches as the Kafka clients of the other engines do
            // by default: up to a megabyte, waiting up to 5 ms to fill one. Every broker acknowledges a write, as
            // the other engines' producers ask.
            (Take::Kafka { brokers, topic, group }, Give::Kafka { topic: to, .. }) => {
                let codec = if w.forwards {
                    "plain { format => \"%{message}\" }"
                } else {
                    "json"
                };
                (
                    format!(
                        "kafka {{ bootstrap_servers => \"{brokers}\" topics => [\"{topic}\"] group_id => \"{group}\" \
                         auto_offset_reset => \"earliest\" consumer_threads => {cores} }}"
                    ),
                    format!(
                        "kafka {{ bootstrap_servers => \"{brokers}\" topic_id => \"{to}\" codec => {codec} \
                         acks => \"all\" linger_ms => 5 batch_size => 1000000 }}"
                    ),
                )
            }
            _ => panic!("Logstash runs HTTP and Kafka alone"),
        };
        format!("input {{ {input} }}\n{filter}\noutput {{ {output} }}\n")
    }

    /// A worker for each core, batches larger than the default 125, and no ordering, which only one worker would
    /// keep.
    pub fn settings(cores: usize) -> String {
        format!("pipeline.workers: {cores}\npipeline.batch.size: 1000\npipeline.ordered: false\napi.enabled: false\n")
    }
}

/// Vector's configuration, as JSON, which Vector reads as it reads YAML.
mod vector {
    use super::*;

    /// The transport's source, the pipeline's remap when it has one, and the transport's sink. The HTTP source answers
    /// on receipt, as the other engines' listeners do; the Kafka sink acknowledges delivery, so the source commits
    /// what was delivered, as smeltr does.
    pub fn config(w: &Workload, ends: &Ends) -> Value {
        let remap = Engine::Vector.pipeline(w, "vrl");
        // A pipeline that reads the record parses it as it arrives; one that forwards it sends the line it read.
        let codec = if w.forwards { "bytes" } else { "json" };
        let encoding = if w.forwards && ends.from() != In::Prometheus {
            "text"
        } else {
            "json"
        };
        let address = |port: &u16| format!("0.0.0.0:{port}");
        let basic = json!({"strategy": "basic", "username": "bench", "password": PASSWORD});
        let source = match &ends.take {
            Take::Listen { port, from } => match from {
                In::Http => json!({"type": "http_server", "address": address(port), "auth": basic,
                       "framing": {"method": "newline_delimited"}, "decoding": {"codec": codec}}),
                // Acknowledgements are off, so the collector answers on receipt, as smeltr's does.
                In::Hec => json!({"type": "splunk_hec", "address": address(port), "valid_tokens": [RECEIVER_TOKEN]}),
                // Its gRPC listener is required, and idle. Its sink encodes only what the source keeps whole, so a
                // request it forwards it keeps as one event, as its documentation does.
                In::Otlp => json!({"type": "opentelemetry", "http": {"address": address(port)},
                "grpc": {"address": address(&free_port())},
                "decoding": if ends.give_to() == Some(Out::Otlp) {
                    json!({"codec": "otlp", "signal_types": ["logs"]})
                } else {
                    json!({})
                }}),
                In::Prometheus => json!({"type": "prometheus_remote_write", "address": address(port), "auth": basic}),
                // Its socket source, since smeltr's syslog receiver takes each message whole, as the socket source
                // does, and its syslog source would parse them.
                In::Syslog => json!({"type": "socket", "mode": "tcp", "address": address(port),
                       "framing": {"method": "newline_delimited"}, "decoding": {"codec": "bytes"}}),
                In::Kafka => unreachable!("Kafka is pulled"),
            },
            Take::Kafka { brokers, topic, group } => {
                json!({"type": "kafka", "bootstrap_servers": brokers, "group_id": group, "topics": [topic],
                       "auto_offset_reset": "earliest", "decoding": {"codec": codec}})
            }
        };
        let sink = match &ends.give {
            Give::Sink { url, to } => match to {
                Out::Http => json!({"type": "http", "uri": format!("{url}/"), "encoding": {"codec": encoding},
                       "framing": {"method": "newline_delimited"},
                       "batch": {"max_bytes": 10_000_000, "timeout_secs": 0.2}}),
                // The sink is no cluster to ask its version or health.
                Out::Elasticsearch => json!({"type": "elasticsearch", "endpoints": [url], "api_version": "v8",
                       "mode": "bulk", "bulk": {"index": "bench", "action": "create"},
                       "healthcheck": {"enabled": false}}),
                Out::SplunkHec => json!({"type": "splunk_hec_logs", "endpoint": url, "default_token": RECEIVER_TOKEN,
                       "index": "main", "encoding": {"codec": "json"}, "healthcheck": {"enabled": false}}),
                Out::Prometheus => json!({"type": "prometheus_remote_write", "endpoint": format!("{url}/api/v1/write"),
                       "healthcheck": {"enabled": false}}),
                Out::Otlp => {
                    json!({"type": "opentelemetry", "protocol": {"type": "http", "uri": format!("{url}/v1/logs"),
                       "method": "post", "encoding": {"codec": "otlp"}}})
                }
                Out::Kafka | Out::S3 | Out::AzureBlob => unreachable!("{to:?} is no sink"),
            },
            Give::Kafka { brokers, topic } => {
                json!({"type": "kafka", "bootstrap_servers": brokers, "topic": topic, "encoding": {"codec": encoding},
                       "acknowledgements": {"enabled": true}})
            }
            // Both its batches and smeltr's files end every `STORE_SECS`, and are gzipped, as both do by default.
            Give::Store(store) => {
                let common = json!({"encoding": {"codec": "text"}, "framing": {"method": "newline_delimited"},
                       "batch": {"timeout_secs": STORE_SECS}, "healthcheck": {"enabled": false}});
                let mut sink = match store.to {
                    Out::S3 => json!({"type": "aws_s3", "bucket": store.bucket, "region": "us-east-1",
                           "endpoint": store.url, "auth": {"access_key_id": "test", "secret_access_key": "test"}}),
                    _ => json!({"type": "azure_blob", "container_name": store.bucket,
                           "connection_string": format!("DefaultEndpointsProtocol=http;AccountName={0};AccountKey={1};\
                                BlobEndpoint={2};", stores::ACCOUNT, azure::KEY, store.url)}),
                };
                sink.as_object_mut()
                    .unwrap()
                    .extend(common.as_object().unwrap().clone());
                sink
            }
        };
        // Each event is what its source read, with what the source knows of it kept apart, as smeltr keeps it.
        let mut config = json!({
            "data_dir": "/tmp",
            "schema": {"log_namespace": true},
            "sources": {"in": source},
            "sinks": {"out": sink},
        });
        config["sinks"]["out"]["inputs"] = match remap {
            Some(vrl) => {
                config["transforms"] =
                    json!({"pipeline": {"type": "remap", "inputs": ["in"], "drop_on_abort": true, "source": vrl}});
                json!(["pipeline"])
            }
            // The OpenTelemetry source puts its logs, metrics and traces each on an output of its own.
            None if ends.from() == In::Otlp => json!(["in.logs"]),
            None => json!(["in"]),
        };
        config
    }
}

impl Ends {
    /// `transport`'s ends for `run`: what it takes from `topic` when it takes from Kafka, and what it gives to `sink`
    /// or to a topic of its own, which this makes.
    fn new(transport: Transport, run: &str, sink: &Sink, broker: Option<&Broker>, topic: &str, rest: &[usize]) -> Ends {
        let take = match transport.from {
            In::Http | In::Hec | In::Otlp | In::Prometheus | In::Syslog => Take::Listen {
                port: free_port(),
                from: transport.from,
            },
            In::Kafka => Take::Kafka {
                brokers: broker.expect("a broker").brokers.clone(),
                topic: topic.into(),
                group: run.into(),
            },
        };
        let give = match transport.to {
            Out::Http | Out::Elasticsearch | Out::SplunkHec | Out::Prometheus | Out::Otlp => Give::Sink {
                url: format!("http://127.0.0.1:{}", sink.port),
                to: transport.to,
            },
            Out::S3 | Out::AzureBlob => Give::Store(stores::Store::start(transport.to, run, rest)),
            Out::Kafka => {
                let broker = broker.expect("a broker");
                broker.create(&format!("{run}-out"));
                Give::Kafka {
                    brokers: broker.brokers.clone(),
                    topic: format!("{run}-out"),
                }
            }
        };
        Ends { take, give }
    }
}

/// What an engine delivered where `give` says, counted and read as the harness's other end counts and reads it.
struct Delivered<'a> {
    give: &'a Give,
    sink: &'a Sink,
    broker: Option<&'a Broker>,
    consumer: Option<rdkafka::consumer::BaseConsumer>,
}

impl<'a> Delivered<'a> {
    fn new(give: &'a Give, sink: &'a Sink, broker: Option<&'a Broker>) -> Delivered<'a> {
        let consumer = matches!(give, Give::Kafka { .. }).then(|| broker.expect("a broker").consumer());
        Delivered {
            give,
            sink,
            broker,
            consumer,
        }
    }

    fn count(&self) -> u64 {
        match self.give {
            Give::Sink { .. } => self.sink.rows(),
            Give::Kafka { topic, .. } => self.broker.unwrap().count(self.consumer.as_ref().unwrap(), topic),
            Give::Store(store) => store.count(),
        }
    }

    /// Every document delivered, from a sink that keeps them.
    fn read(&self) -> Vec<Value> {
        match self.give {
            Give::Sink { .. } => self.sink.kept.as_ref().unwrap().lock().unwrap().drain(..).collect(),
            Give::Kafka { topic, .. } => self.broker.unwrap().read(topic, self.count()),
            Give::Store(store) => store.read(),
        }
    }

    /// How long to wait between counts: a broker or a store is asked over the network, and a store's objects are
    /// read whole.
    fn pause(&self) -> Duration {
        match self.give {
            Give::Sink { .. } => Duration::ZERO,
            Give::Kafka { .. } => Duration::from_millis(50),
            Give::Store(_) => Duration::from_secs(1),
        }
    }
}

impl Drop for Delivered<'_> {
    fn drop(&mut self) {
        if let Give::Kafka { topic, .. } = self.give {
            self.broker.unwrap().delete(topic);
        }
    }
}

/// The documents `engine` makes of the first `CHECK_ROWS` records of the corpus, sent over `transport`.
fn made(
    engine: Engine,
    w: &Workload,
    run: &str,
    cpus: &[usize],
    rest: &[usize],
    transport: Transport,
    broker: Option<&Broker>,
) -> Vec<Value> {
    let want = (0..CHECK_ROWS).filter(|&i| (w.expect)(&line(i)).is_some()).count() as u64;
    let sink = Sink::keeping();
    let ends = Ends::new(transport, run, &sink, broker, CHECK_TOPIC, rest);
    let delivered = Delivered::new(&ends.give, &sink, broker);
    let running = start(engine, w, run, cpus, &ends);
    let _sender = running
        .push
        .as_ref()
        .map(|(url, push)| Sender::start(url, push, &bodies(push), SENDERS, 0, CHECK_ROWS));
    if let Take::Listen { port, from: In::Syslog } = ends.take {
        stream_syslog(port, 0, CHECK_ROWS);
    }
    let mut polls = 0;
    wait_for(
        &format!("{run} check"),
        want,
        || delivered.count(),
        || {
            polls += 1;
            if polls % 200 == 0 {
                running.container.check(run);
            }
            std::thread::sleep(delivered.pause());
        },
    );
    // Late extras would show here.
    std::thread::sleep(Duration::from_secs(1));
    delivered.read()
}

/// The topic every Kafka check reads, seeded with the first `CHECK_ROWS` records.
const CHECK_TOPIC: &str = "engine-speed-check";

/// The topic every timed Kafka row reads, seeded with `FINITE` times the rows a row times.
const INPUT_TOPIC: &str = "engine-speed-input";

/// How many rows' worth of records a finite input holds, the input topic or a stream of syslog lines: enough for the
/// warm-up and the timed rows of the fastest engine.
const FINITE: u64 = 3;

/// Panics unless the documents `engine` makes of the first `CHECK_ROWS` records are those `workload` must make, no
/// more and no fewer, in any order.
fn check(
    engine: Engine,
    w: &Workload,
    run: &str,
    cpus: &[usize],
    rest: &[usize],
    transport: Transport,
    broker: Option<&Broker>,
) {
    let from = transport.from;
    let mut want: Vec<String> = (0..CHECK_ROWS)
        .filter_map(|i| {
            if w.forwards {
                Some(from.sent(i))
            } else {
                (w.expect)(&line(i))
            }
        })
        .map(|d| canonical(&d))
        .collect();
    let mut got: Vec<String> = made(engine, w, run, cpus, rest, transport, broker)
        .into_iter()
        .map(|d| canonical(&from.normal((w.normal)(d))))
        .collect();
    want.sort_unstable();
    got.sort_unstable();
    if got != want {
        let missing = want.iter().find(|w| got.binary_search(w).is_err());
        let extra = got.iter().find(|g| want.binary_search(g).is_err());
        panic!(
            "{run}: {} documents, want {}. First missing: {}. First unexpected: {}",
            got.len(),
            want.len(),
            missing.map_or("none", String::as_str),
            extra.map_or("none", String::as_str)
        );
    }
}

/// What a row cost, between the end of its warm-up and its last record.
#[derive(Clone)]
struct Timed {
    /// Input records a second.
    rows_per_sec: f64,
    /// Cores the container used, on average.
    cpu: f64,
    /// The most memory the container held, from its start.
    peak: u64,
    /// Cores the service at the other end used, on average: the broker under Kafka, or a store.
    broker: f64,
    /// How busy the harness's cores were, the broker's included: near 100%, the harness, not the engine, set the
    /// row's speed.
    harness: f64,
}

/// Times `engine` running `workload` over `transport` on `cpus`, the harness on `rest`.
fn time(
    engine: Engine,
    w: &Workload,
    run: &str,
    cpus: &[usize],
    rest: &[usize],
    transport: Transport,
    broker: Option<&Broker>,
) -> Timed {
    let rows = rows();
    // Under HTTP the sender cycles through its bodies; under Kafka the engine reads the seeded topic.
    let kept_of = |from: u64, n: u64| (from..from + n).filter(|&i| (w.expect)(&line(i)).is_some()).count() as u64;
    let keeps = |n: u64| kept_of(0, n) as f64 / n as f64;
    let sink = Sink::start();
    let ends = Ends::new(transport, run, &sink, broker, INPUT_TOPIC, rest);
    let delivered = Delivered::new(&ends.give, &sink, broker);
    // A stream of lines, like a topic, is as long as a topic is seeded.
    let (kept, most) = match ends.take {
        Take::Listen { from: In::Syslog, .. } | Take::Kafka { .. } => (keeps(FINITE * rows), kept_of(0, FINITE * rows)),
        Take::Listen { .. } => (keeps(64 * PER_REQUEST), u64::MAX),
    };
    let running = start(engine, w, run, cpus, &ends);
    let sender = running.push.as_ref().map(|(url, push)| {
        Sender::start(
            url,
            push,
            &bodies(push),
            env_or("ENGINE_SENDERS", SENDERS),
            0,
            u64::MAX / 2,
        )
    });
    if let Take::Listen { port, from: In::Syslog } = ends.take {
        stream_syslog(port, 0, FINITE * rows);
    }
    let count = || delivered.count();
    let began = Instant::now();
    let mut polls = 0;
    let mut each = || {
        polls += 1;
        // A destination that keeps failing still moves a few records, so no stall shows it.
        assert!(began.elapsed() < RUN_LIMIT, "{run}: not done in {RUN_LIMIT:?}");
        if polls % 200 == 0 {
            running.container.check(run);
            if let Some(b) = broker {
                b.container.check("the broker");
            }
        }
        std::thread::sleep(delivered.pause());
    };
    let window = (rows as f64 * kept) as u64;
    wait_for(
        &format!("{run} warming"),
        (warm(rows) as f64 * kept) as u64,
        count,
        &mut each,
    );
    // A finite topic ends the warm-up early for an engine fast enough to need most of it.
    while began.elapsed() < Duration::from_secs(WARM_SECS) && count() + window < most {
        std::thread::sleep(Duration::from_millis(100));
        each();
    }
    // The service at the other end, the broker or the store, whose cores the harness's are.
    let peer = |b: Option<&Broker>| match &ends.give {
        Give::Store(store) => store.container.usage().0,
        _ => b.map_or(0.0, |b| b.container.usage().0),
    };
    let (at, docs, (cpu, _), b_cpu, idle_before) = (
        Instant::now(),
        count(),
        running.container.usage(),
        peer(broker),
        idle(rest),
    );
    wait_for(run, (docs + window).min(most), count, &mut each);
    let wall = at.elapsed().as_secs_f64();
    let (docs, (cpu_after, peak)) = (count() - docs, running.container.usage());
    let timed = Timed {
        rows_per_sec: docs as f64 / kept / wall,
        cpu: (cpu_after - cpu) / wall,
        peak,
        broker: (peer(broker) - b_cpu) / wall,
        harness: 1.0 - (idle(rest) - idle_before) / wall / rest.len() as f64,
    };
    // Every record the engine took is delivered once: the count holds no loss and no duplicate.
    let want = match &sender {
        Some(sender) => {
            sender.stop();
            let in_body: Vec<u64> = (0..64).map(|b| kept_of(b * PER_REQUEST, PER_REQUEST)).collect();
            // Every request in flight when it stopped is answered, or the engine never answers it.
            let stopped = Instant::now();
            while sender.running() > 0 {
                assert!(stopped.elapsed() < PATIENCE, "{run}: a request was never answered");
                each();
                std::thread::sleep(Duration::from_millis(100));
            }
            sender.sent().iter().zip(&in_body).map(|(n, k)| n * k).sum::<u64>()
        }
        None => most,
    };
    wait_for(&format!("{run} delivering the rest"), want, count, &mut each);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(count(), want, "{run}: records delivered, against records taken");
    timed
}

/// One row: which pipeline, over which transport.
type Case<'a> = (&'a Workload, Transport);

#[test]
#[ignore = "run by `just engine-speed`"]
fn speed() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    {
        use base64::Engine as _;
        assert_eq!(
            BASIC,
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!("bench:{PASSWORD}"))
            )
        );
    }
    let counts = core_counts();
    let cores = cores_by_cpu();
    let most = *counts.iter().max().unwrap();
    assert!(
        most < cores.len(),
        "{most} cores for the engine leave none for the harness"
    );
    let on = |n: usize| cores[..n].concat();
    let rest = cores[most..].concat();
    // The sender, the sink and the broker run on the cores the engines do not.
    set_affinity(0, &rest);
    let only = |var: &str, name: &str| {
        std::env::var(var)
            .ok()
            .is_none_or(|o| o.split(',').any(|o| o.trim() == name))
    };
    let engines: Vec<Engine> = ENGINES.into_iter().filter(|e| only("ENGINES", e.label())).collect();
    let transports: Vec<Transport> = TRANSPORTS
        .iter()
        .copied()
        .filter(|t| only("ENGINE_TRANSPORTS", &t.label()))
        .collect();
    let cases: Vec<Case> = WORKLOADS
        .iter()
        .filter(|w| only("ENGINE_PIPELINES", w.name))
        .flat_map(|w| transports.iter().filter(|t| (w.over)(**t)).map(move |&t| (w, t)))
        .collect();
    if engines.contains(&Engine::Smeltr) {
        let out = Command::new("docker")
            .args(["build", "-q", "-t", Engine::Smeltr.image()])
            .arg(repo().join("tests/engines/smeltr"))
            .output()
            .expect("docker runs");
        assert!(
            out.status.success(),
            "smeltr's image: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let rounds = env_or("ENGINE_ROUNDS", 3usize);
    let id = std::process::id();
    let broker = cases.iter().any(|c| c.1.kafka()).then(|| {
        let broker = Broker::start(&format!("es-broker-{id}"), &rest);
        broker.create(CHECK_TOPIC);
        broker.seed(CHECK_TOPIC, CHECK_ROWS);
        broker.create(INPUT_TOPIC);
        broker.seed(INPUT_TOPIC, FINITE * rows());
        broker
    });

    // Checked once each, on the fewest cores: an engine that makes other documents is not timed.
    let mut refused: Vec<(Case, Engine, String)> = Vec::new();
    for &(w, t) in &cases {
        for &e in engines.iter().filter(|e| e.runs(w, t)) {
            let run = format!("es-check-{}-{}-{}-{id}", w.name, t.label(), e.label());
            let fewest = *counts.iter().min().unwrap();
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                check(e, w, &run, &on(fewest), &rest, t, broker.as_ref())
            })) {
                let why = panic_message(&panic);
                println!("{:<12} {:<6} {:<9} check failed: {why}", w.name, t.label(), e.label());
                refused.push(((w, t), e, why));
            } else {
                println!("{:<12} {:<6} {:<9} check passed", w.name, t.label(), e.label());
            }
        }
    }

    let mut timed: Vec<(Case, Engine, usize, Result<Timed, String>)> = Vec::new();
    for round in 0..rounds {
        // Each round starts with another engine and runs the core counts the other way, so a host that slows as it
        // warms costs every engine alike.
        let mut order = engines.clone();
        order.rotate_left(round % engines.len().max(1));
        let mut cores = counts.clone();
        if round % 2 == 1 {
            cores.reverse();
        }
        for &(w, t) in &cases {
            for &n in &cores {
                for &e in order.iter().filter(|e| e.runs(w, t)) {
                    if refused.iter().any(|r| same(r.0, (w, t)) && r.1 == e) {
                        continue;
                    }
                    let run = format!("es-{}-{}-{}-{n}c-{round}-{id}", w.name, t.label(), e.label());
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        time(e, w, &run, &on(n), &rest, t, broker.as_ref())
                    }))
                    .map_err(|p| panic_message(&p));
                    match &result {
                        Ok(r) => println!(
                            "{:<12} {:<6} {:<9} {n:>2} cores {:>9.0} records/s  cpu {:>5.2}  peak {:>6.0} MB  service {:>5.2}  \
                             harness {:>3.0}%",
                            w.name,
                            t.label(),
                            e.label(),
                            r.rows_per_sec,
                            r.cpu,
                            r.peak as f64 / 1e6,
                            r.broker,
                            r.harness * 100.0
                        ),
                        Err(why) => println!(
                            "{:<12} {:<6} {:<9} {n:>2} cores failed: {why}",
                            w.name,
                            t.label(),
                            e.label()
                        ),
                    }
                    timed.push(((w, t), e, n, result));
                }
            }
        }
    }
    println!("\n{}", table(&cases, &engines, &counts, &timed, &refused));
    let failed = timed.iter().filter(|t| t.3.is_err()).count() + refused.len();
    assert_eq!(
        failed, 0,
        "{failed} rows or checks failed; the table says which and why"
    );
}

fn same(a: Case, b: Case) -> bool {
    a.0.name == b.0.name && a.1 == b.1
}

/// One row for each pipeline, transport, engine and core count: the median of its rounds, and their spread.
fn table(
    cases: &[Case],
    engines: &[Engine],
    counts: &[usize],
    timed: &[(Case, Engine, usize, Result<Timed, String>)],
    refused: &[(Case, Engine, String)],
) -> String {
    let mut out = String::from(
        "| Pipeline | Transport | Engine | Cores | records/s | Spread | records/s per core used | Cores used | Peak memory | \
         Service cores | Harness busy |\n|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    let one_line = |why: &str| why.replace('|', "/").replace('\n', " ");
    for &case in cases {
        let (w, t) = case;
        for &n in counts {
            for &e in engines {
                let head = format!("| {} | {} | {} | {n} |", w.name, t.label(), e.label());
                if let Some((_, _, why)) = refused.iter().find(|r| same(r.0, case) && r.1 == e) {
                    out.push_str(&format!("{head} check failed: {} | | | | | | |\n", one_line(why)));
                    continue;
                }
                let runs: Vec<&Result<Timed, String>> = timed
                    .iter()
                    .filter(|r| same(r.0, case) && r.1 == e && r.2 == n)
                    .map(|r| &r.3)
                    .collect();
                if let Some(Err(why)) = runs.iter().find(|r| r.is_err()) {
                    out.push_str(&format!("{head} failed: {} | | | | | | |\n", one_line(why)));
                    continue;
                }
                let mut ok: Vec<&Timed> = runs.iter().filter_map(|r| r.as_ref().ok()).collect();
                if ok.is_empty() {
                    continue;
                }
                ok.sort_by(|a, b| a.rows_per_sec.total_cmp(&b.rows_per_sec));
                let median = ok[ok.len() / 2];
                let spread = (ok[ok.len() - 1].rows_per_sec - ok[0].rows_per_sec) / median.rows_per_sec;
                let broker = if t.kafka() || matches!(t.to, Out::S3 | Out::AzureBlob) {
                    format!("{:.2}", median.broker)
                } else {
                    String::new()
                };
                out.push_str(&format!(
                    "{head} {:.0} | {:.0}% | {:.0} | {:.2} | {:.0} MB | {broker} | {:.0}% |\n",
                    median.rows_per_sec,
                    spread * 100.0,
                    median.rows_per_sec / median.cpu,
                    median.cpu,
                    median.peak as f64 / 1e6,
                    median.harness * 100.0
                ));
            }
        }
    }
    out
}

/// NiFi takes its flow from its REST API, so the harness builds it there: the transport's consumer, the pipeline's
/// record transform when it has one, and the transport's producer, each as NiFi's documentation tunes it for
/// throughput.
mod nifi {
    use super::*;

    /// The single user NiFi's image starts with, which the API needs a token for. NiFi wants twelve characters.
    pub const PASSWORD: &str = "connector-speed-bench";

    struct Api {
        base: String,
        token: String,
        agent: ureq::Agent,
        root: String,
    }

    impl Api {
        fn call(&self, method: &str, path: &str, body: Option<Value>) -> Value {
            let mut request = ureq::http::Request::builder()
                .method(method)
                .uri(format!("{}{path}", self.base))
                .header("authorization", format!("Bearer {}", self.token));
            let body = match body {
                Some(b) => {
                    request = request.header("content-type", "application/json");
                    b.to_string().into_bytes()
                }
                None => Vec::new(),
            };
            let mut response = self
                .agent
                .run(request.body(body).unwrap())
                .unwrap_or_else(|e| panic!("NiFi {method} {path}: {e}"));
            let status = response.status().as_u16();
            let text = response.body_mut().read_to_string().unwrap_or_default();
            assert!((200..300).contains(&status), "NiFi {method} {path}: {status} {text}");
            serde_json::from_str(&text).unwrap_or(Value::Null)
        }

        /// A controller service of `kind`, enabled.
        fn service(&self, kind: &str, properties: Value) -> String {
            let s = self.call(
                "POST",
                &format!("/process-groups/{}/controller-services", self.root),
                Some(json!({"revision": {"version": 0}, "component": {"type": kind, "properties": properties}})),
            );
            let id = s["id"].as_str().unwrap().to_string();
            self.call(
                "PUT",
                &format!("/controller-services/{id}/run-status"),
                Some(json!({"revision": s["revision"], "state": "ENABLED"})),
            );
            id
        }

        /// A processor of `kind`, its relationships in `ended` going nowhere, running `tasks` at once.
        fn processor(&self, kind: &str, properties: Value, ended: &[&str], tasks: usize) -> String {
            let p = self.call(
                "POST",
                &format!("/process-groups/{}/processors", self.root),
                Some(json!({"revision": {"version": 0}, "component": {
                    "type": kind,
                    "config": {
                        "properties": properties,
                        "autoTerminatedRelationships": ended,
                        "concurrentlySchedulableTaskCount": tasks,
                    },
                }})),
            );
            p["id"].as_str().unwrap().to_string()
        }

        fn connect(&self, from: &str, to: &str, relationships: &[&str]) {
            self.call(
                "POST",
                &format!("/process-groups/{}/connections", self.root),
                Some(json!({"revision": {"version": 0}, "component": {
                    "source": {"id": from, "groupId": self.root, "type": "PROCESSOR"},
                    "destination": {"id": to, "groupId": self.root, "type": "PROCESSOR"},
                    "selectedRelationships": relationships,
                }})),
            );
        }
    }

    /// `nifi/nar`'s NAR, built once a run by Maven in its own container, its downloads kept in a volume.
    pub fn nar() -> PathBuf {
        static NAR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        NAR.get_or_init(|| {
            let dir = repo().join("target/engine-speed/nar");
            let _ = std::fs::remove_dir_all(&dir);
            let copied = Command::new("cp")
                .arg("-r")
                .arg(repo().join("tests/engines/nifi/nar"))
                .arg(&dir)
                .status()
                .unwrap();
            assert!(copied.success(), "could not copy the NAR's sources");
            let built = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "--network",
                    "host",
                    "-v",
                    "smeltr-engine-speed-m2:/root/.m2",
                    "-v",
                ])
                .arg(format!("{}:/nar", dir.display()))
                .args([
                    "-w",
                    "/nar",
                    "maven:3.9.11-eclipse-temurin-21",
                    "mvn",
                    "-q",
                    "-B",
                    "package",
                ])
                .status()
                .unwrap();
            assert!(built.success(), "Maven could not build the NAR");
            dir.join("nar/target/json-app-nar-1.0.nar")
        })
        .clone()
    }

    /// Builds `w`'s flow between `ends` in the NiFi whose API is on `api_port`, and starts it.
    pub fn build(
        container: &Container,
        run: &str,
        api_port: u16,
        ends: &Ends,
        w: &Workload,
        cores: usize,
        engine: Engine,
    ) {
        // NiFi's certificate is its own, for `localhost`, on loopback.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .tls_config(
                ureq::tls::TlsConfig::builder()
                    .provider(ureq::tls::TlsProvider::Rustls)
                    .disable_verification(true)
                    .build(),
            )
            .build()
            .into();
        let base = format!("https://localhost:{api_port}/nifi-api");
        let deadline = Instant::now() + PATIENCE;
        let token = loop {
            container.check(run);
            assert!(Instant::now() < deadline, "{run}: NiFi never served its API");
            let request = ureq::http::Request::builder()
                .method("POST")
                .uri(format!("{base}/access/token"))
                .header("content-type", "application/x-www-form-urlencoded")
                .body(format!("username=bench&password={PASSWORD}").into_bytes())
                .unwrap();
            if let Ok(mut r) = agent.run(request)
                && r.status().is_success()
            {
                break r.body_mut().read_to_string().unwrap();
            }
            std::thread::sleep(Duration::from_secs(1));
        };
        let mut api = Api {
            base,
            token,
            agent,
            root: String::new(),
        };
        api.root = api.call("GET", "/flow/process-groups/root", None)["processGroupFlow"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        // Four threads a core, the most the administration guide advises: the producer waits on its destination.
        let config = api.call("GET", "/controller/config", None);
        api.call(
            "PUT",
            "/controller/config",
            Some(json!({"revision": config["revision"], "component": {"maxTimerDrivenThreadCount": 4 * cores}})),
        );
        // A flowfile holds many records, a line each, as a request body does.
        let (consume, produce) = match (&ends.take, &ends.give) {
            (
                Take::Listen { port, from: In::Http },
                Give::Sink {
                    url: sink,
                    to: Out::Http,
                },
            ) => (
                api.processor(
                    "org.apache.nifi.processors.standard.ListenHTTP",
                    json!({"Listening Port": port.to_string(), "Base Path": "in"}),
                    &[],
                    1,
                ),
                // A producer spends its task waiting on the answer, so it runs more than the cores: eight tasks a
                // hyperthread, past which it took no more on one core.
                api.processor(
                    "org.apache.nifi.processors.standard.InvokeHTTP",
                    json!({"HTTP Method": "POST", "HTTP URL": sink}),
                    &["Original", "Response"],
                    8 * cores,
                ),
            ),
            (
                Take::Kafka {
                    brokers,
                    topic: from,
                    group,
                },
                Give::Kafka { topic: to, .. },
            ) => {
                let kafka = api.service(
                    "org.apache.nifi.kafka.service.Kafka3ConnectionService",
                    json!({"bootstrap.servers": brokers, "security.protocol": "PLAINTEXT"}),
                );
                (
                    api.processor(
                        "org.apache.nifi.kafka.processors.ConsumeKafka",
                        json!({"Kafka Connection Service": kafka, "Group ID": group, "Topics": from,
                               "auto.offset.reset": "earliest", "Processing Strategy": "DEMARCATOR",
                               "Message Demarcator": "\n"}),
                        &[],
                        cores,
                    ),
                    // Without transactions, as the other engines produce.
                    api.processor(
                        "org.apache.nifi.kafka.processors.PublishKafka",
                        json!({"Kafka Connection Service": kafka, "Topic Name": to, "Message Demarcator": "\n",
                               "Transactions Enabled": "false"}),
                        &["success"],
                        2 * cores,
                    ),
                )
            }
            _ => panic!("NiFi runs HTTP and Kafka alone"),
        };
        // What the producer could not deliver it tries again, as the other engines retry, rather than lose it.
        let failed: &[&str] = match ends.give {
            Give::Sink { .. } => &["Retry", "No Retry", "Failure"],
            Give::Kafka { .. } => &["failure"],
            Give::Store(_) => unreachable!("NiFi runs HTTP and Kafka alone"),
        };
        api.connect(&produce, &produce, failed);
        match Engine::Nifi.pipeline(w, "groovy") {
            Some(script) => {
                // A schema read once beats inferring one from each flowfile, as the record readers' documentation
                // says. It names only the fields the mapping reads: the rest leave at the reader.
                let schema = Engine::Nifi.pipeline(w, "avsc").expect("the script's schema");
                let reader = api.service(
                    "org.apache.nifi.json.JsonTreeReader",
                    json!({"Schema Access Strategy": "schema-text-property", "Schema Text": schema}),
                );
                let writer = api.service(
                    "org.apache.nifi.json.JsonRecordSetWriter",
                    json!({"Schema Write Strategy": "no-schema", "Output Grouping": "output-oneline",
                           "Suppress Null Values": "always-suppress"}),
                );
                let transform = match engine {
                    Engine::NifiGroovy => api.processor(
                        "org.apache.nifi.processors.script.ScriptedTransformRecord",
                        json!({"Record Reader": reader, "Record Writer": writer, "Script Engine": "Groovy",
                               "Script Body": script}),
                        &["failure"],
                        cores,
                    ),
                    // The same mapping, in `nifi/nar`'s processor.
                    _ => api.processor(
                        "sh.smeltr.bench.NormalizeJsonApp",
                        json!({"Record Reader": reader, "Record Writer": writer}),
                        &["failure"],
                        cores,
                    ),
                };
                api.connect(&consume, &transform, &["success"]);
                api.connect(&transform, &produce, &["success"]);
            }
            None => api.connect(&consume, &produce, &["success"]),
        }
        let root = api.root.clone();
        api.call(
            "PUT",
            &format!("/flow/process-groups/{root}"),
            Some(json!({"id": root, "state": "RUNNING"})),
        );
    }
}

/// The object stores the harness starts for a run, each on the harness's cores, keeping what it is given in memory,
/// and how the harness reads back what an engine wrote there.
mod stores {
    use super::*;

    pub struct Store {
        pub container: Container,
        pub to: Out,
        /// Where an engine reaches it: an S3 endpoint, or an Azure account's address.
        pub url: String,
        /// The bucket, or the container, the engine writes to.
        pub bucket: String,
        /// The objects counted, and the records in them.
        counted: std::sync::Mutex<(std::collections::HashSet<String>, u64)>,
    }

    /// Azure's account in its emulator.
    pub const ACCOUNT: &str = "devstoreaccount1";

    impl Store {
        /// Starts `to`'s store for `run` on `cpus`, and makes its bucket.
        pub fn start(to: Out, run: &str, cpus: &[usize]) -> Store {
            let port = free_port();
            let mut docker = container(&format!("{run}-store"), cpus, env_or("ENGINE_BROKER_MEMORY_GB", 8));
            let url = match to {
                // SeaweedFS serves S3 from every core it has, where LocalStack serves it from about one. With no
                // identities configured it takes any request, signed or not.
                Out::S3 => {
                    // Its master, volume and filer take ports of their own, and gRPC ports 10000 above each.
                    let low = || loop {
                        let p = free_port();
                        if p < 55_000 {
                            break p;
                        }
                    };
                    let port = low();
                    docker.args(["--tmpfs", "/data"]).args([
                        "chrislusf/seaweedfs:4.52",
                        "server",
                        "-s3",
                        "-dir=/data",
                        "-ip=127.0.0.1",
                        "-ip.bind=127.0.0.1",
                    ]);
                    docker.arg(format!("-s3.port={port}"));
                    for flag in ["-master.port", "-volume.port", "-filer.port"] {
                        docker.arg(format!("{flag}={}", low()));
                    }
                    format!("http://127.0.0.1:{port}")
                }
                Out::AzureBlob => {
                    docker.args([
                        "mcr.microsoft.com/azure-storage/azurite:3.35.0",
                        "azurite-blob",
                        "--blobHost",
                        "127.0.0.1",
                        "--blobPort",
                        &port.to_string(),
                        "--inMemoryPersistence",
                        "--skipApiVersionCheck",
                    ]);
                    format!("http://127.0.0.1:{port}/{ACCOUNT}")
                }
                _ => unreachable!("{to:?} is no store"),
            };
            let store = Store {
                container: Container::run(docker, &format!("{run}: the store")),
                to,
                url,
                bucket: "bench".into(),
                counted: Default::default(),
            };
            let deadline = Instant::now() + PATIENCE;
            loop {
                store.container.check("the store");
                let made = match to {
                    Out::S3 => fetch("PUT", &format!("{}/{}", store.url, store.bucket), &[], Vec::new()),
                    _ => fetch(
                        "PUT",
                        &format!("{}/{}?restype=container&{}", store.url, store.bucket, azure::SAS),
                        &[],
                        Vec::new(),
                    ),
                };
                if made.is_some() {
                    break;
                }
                assert!(Instant::now() < deadline, "{run}: the store never made its bucket");
                std::thread::sleep(Duration::from_millis(500));
            }
            store
        }

        /// Every object's name.
        fn names(&self) -> Vec<String> {
            let mut names = Vec::new();
            let mut next = String::new();
            loop {
                let page = match self.to {
                    Out::S3 => {
                        let token = if next.is_empty() {
                            String::new()
                        } else {
                            format!("&continuation-token={}", encode(&next))
                        };
                        fetch(
                            "GET",
                            &format!("{}/{}?list-type=2{token}", self.url, self.bucket),
                            &[],
                            Vec::new(),
                        )
                    }
                    _ => fetch(
                        "GET",
                        &format!(
                            "{}/{}?restype=container&comp=list&marker={}&{}",
                            self.url,
                            self.bucket,
                            encode(&next),
                            azure::SAS
                        ),
                        &[],
                        Vec::new(),
                    ),
                }
                .expect("the store lists its objects");
                let page = String::from_utf8_lossy(&page).into_owned();
                next = match self.to {
                    Out::S3 => {
                        names.extend(tags(&page, "Key"));
                        tags(&page, "NextContinuationToken")
                            .into_iter()
                            .next()
                            .unwrap_or_default()
                    }
                    _ => {
                        names.extend(tags(&page, "Name"));
                        tags(&page, "NextMarker").into_iter().next().unwrap_or_default()
                    }
                };
                if next.is_empty() {
                    return names;
                }
            }
        }

        /// Object `name`'s lines, ungzipped: every engine gzips what it writes, as each does by default.
        fn lines(&self, name: &str) -> Vec<u8> {
            let url = match self.to {
                Out::S3 => format!("{}/{}/{name}", self.url, self.bucket),
                _ => format!("{}/{}/{name}?{}", self.url, self.bucket, azure::SAS),
            };
            let (body, _) =
                fetched("GET", &url, &[], Vec::new()).unwrap_or_else(|| panic!("the store does not give {name}"));
            assert!(body.starts_with(&[0x1f, 0x8b]), "{name} is not gzipped");
            let mut lines = Vec::new();
            flate2::read::MultiGzDecoder::new(body.as_slice())
                .read_to_end(&mut lines)
                .expect("gzip");
            lines
        }

        /// Records in the objects written so far: each object is read once, when it is first listed.
        pub fn count(&self) -> u64 {
            let names = self.names();
            let mut counted = self.counted.lock().unwrap();
            for name in names {
                if !counted.0.contains(&name) {
                    let lines = self.lines(&name);
                    counted.1 += lines.iter().filter(|&&b| b == b'\n').count() as u64;
                    counted.0.insert(name);
                }
            }
            counted.1
        }

        /// Every record written, each line read as JSON, or as its text where it is not.
        pub fn read(&self) -> Vec<Value> {
            let mut docs = Vec::new();
            for name in self.names() {
                let lines = self.lines(&name);
                docs.extend(lines.split(|&b| b == b'\n').filter(|l| !l.is_empty()).map(|l| {
                    serde_json::from_slice(l).unwrap_or_else(|_| Value::String(String::from_utf8_lossy(l).into_owned()))
                }));
            }
            docs
        }
    }

    /// The text of each `<tag>` in `xml`.
    fn tags(xml: &str, tag: &str) -> Vec<String> {
        let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
        xml.split(&open)
            .skip(1)
            .filter_map(|s| s.split_once(&close).map(|(t, _)| t.replace("&amp;", "&")))
            .collect()
    }

    fn encode(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
                _ => format!("%{b:02X}"),
            })
            .collect()
    }

    /// The body of a request that succeeded, or `None`.
    fn fetch(method: &str, url: &str, headers: &[(&str, &str)], body: Vec<u8>) -> Option<Vec<u8>> {
        fetched(method, url, headers, body).map(|f| f.0)
    }

    fn fetched(method: &str, url: &str, headers: &[(&str, &str)], body: Vec<u8>) -> Option<(Vec<u8>, bool)> {
        // reqwest, built without its decompression, leaves a gzipped body and its header as they came.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let client = reqwest::Client::builder().timeout(PATIENCE).build().unwrap();
            let mut request = client.request(method.parse().unwrap(), url).body(body);
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            let response = request.send().await.ok()?;
            let ok = response.status().is_success() || response.status().as_u16() == 409;
            let gzipped = response.headers().get("content-encoding").is_some_and(|e| e == "gzip");
            let body = response.bytes().await.ok()?.to_vec();
            ok.then_some((body, gzipped))
        })
    }
}
