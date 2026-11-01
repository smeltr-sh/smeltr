//! Kubernetes container logs as Fluent Bit ships them: the `tail` input's
//! CRI parser and the `kubernetes` filter wrap each line a container wrote
//! in an envelope of its own. Most services log structured JSON, so the
//! envelope's `log` is JSON inside JSON; about one line in ten is a stack
//! trace or plain text, and still becomes a document, its text the message.
//!
//! The pipeline is `docs/examples.md` section 2 in the GUI's steps: parse
//! the envelope, parse the app's line, map both to ECS, drop the rest.

use super::{Pipeline, mix, pick};

pub const PIPELINE: Pipeline = Pipeline {
    name: "k8s",
    line,
    bundle,
    check,
};

/// What a service writes when it does not write JSON: its runtime's stack
/// trace lines, or a component that only logs text.
#[derive(Clone, Copy, PartialEq)]
enum Lang {
    Go,
    Java,
    Python,
    Node,
}

/// One workload: the namespace it runs in, its deployment name, the
/// container that logs, its runtime, and the image tag running.
#[derive(Clone, Copy)]
struct Service {
    namespace: &'static str,
    app: &'static str,
    container: &'static str,
    lang: Lang,
    version: &'static str,
}

/// A cluster's mix: a storefront's services, identity, the platform layer,
/// and data plumbing. A deployment runs a few replicas, so one service is
/// several pods.
const SERVICES: &[Service] = &[
    svc("shop", "checkout", "api", Lang::Go, "1.42.0"),
    svc("shop", "cart", "cart", Lang::Go, "2.7.3"),
    svc("shop", "catalog", "catalog", Lang::Java, "5.0.12"),
    svc("shop", "payments", "payments", Lang::Java, "3.18.1"),
    svc("shop", "recommendations", "recommender", Lang::Python, "0.9.4"),
    svc("shop", "storefront", "web", Lang::Node, "4.3.0"),
    svc("identity", "auth-service", "auth", Lang::Go, "1.12.5"),
    svc("identity", "user-profile", "profile", Lang::Java, "2.2.0"),
    svc("platform", "api-gateway", "gateway", Lang::Go, "0.31.2"),
    svc("platform", "notification-worker", "worker", Lang::Python, "1.5.7"),
    svc("data", "order-events", "consumer", Lang::Java, "1.8.0"),
    svc("data", "search-indexer", "indexer", Lang::Python, "2.0.1"),
];

const fn svc(
    namespace: &'static str,
    app: &'static str,
    container: &'static str,
    lang: Lang,
    version: &'static str,
) -> Service {
    Service {
        namespace,
        app,
        container,
        lang,
        version,
    }
}

const LEVELS: &[&str] = &[
    "info", "info", "info", "info", "info", "debug", "debug", "warn", "error",
];

const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"];

/// What the request-handling services log, by route.
const ROUTES: &[&str] = &[
    "/api/v1/cart",
    "/api/v1/cart/items",
    "/api/v1/checkout",
    "/api/v1/orders",
    "/api/v1/products",
    "/api/v1/products/search",
    "/api/v1/users/me",
    "/api/v1/payments/authorize",
    "/healthz",
    "/readyz",
];

/// Messages by level: what a service says while things go well, and what
/// it says when they do not.
const INFO_MSGS: &[&str] = &[
    "request completed",
    "order placed",
    "cart updated",
    "cache hit",
    "token issued",
    "message consumed",
    "index batch flushed",
    "connection pool resized",
];
const DEBUG_MSGS: &[&str] = &[
    "cache miss",
    "retrying request",
    "feature flag evaluated",
    "query planned",
];
const WARN_MSGS: &[&str] = &[
    "slow query",
    "upstream latency above threshold",
    "rate limit approaching",
    "deprecated API version used",
];
const ERROR_MSGS: &[&str] = &[
    "db timeout",
    "upstream connect error",
    "payment declined by processor",
    "failed to publish event",
    "context deadline exceeded",
];

/// The text lines a runtime writes on its way down, by runtime.
const GO_TRACE: &[&str] = &[
    "panic: runtime error: invalid memory address or nil pointer dereference",
    "[signal SIGSEGV: segmentation violation code=0x1 addr=0x18 pc=0x8b1c2e]",
    "goroutine 12 [running]:",
    "main.(*Handler).ServeHTTP(0xc0001a2000, {0xa3c4e0, 0xc0004b6000}, 0xc0002f1200)",
    "\t/app/internal/handler/checkout.go:142 +0x2ae",
];
const JAVA_TRACE: &[&str] = &[
    "java.net.SocketTimeoutException: Read timed out",
    "\tat java.base/sun.nio.ch.NioSocketImpl.timedRead(NioSocketImpl.java:288)",
    "\tat com.shop.payments.ProcessorClient.authorize(ProcessorClient.java:88)",
    "\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:883)",
    "Caused by: org.postgresql.util.PSQLException: This connection has been closed.",
    "\t... 42 common frames omitted",
];
const PYTHON_TRACE: &[&str] = &[
    "Traceback (most recent call last):",
    "  File \"/app/worker/consumer.py\", line 57, in handle",
    "    payload = json.loads(message.value)",
    "redis.exceptions.ConnectionError: Error 111 connecting to redis-master:6379. Connection refused.",
];
const NODE_TRACE: &[&str] = &[
    "TypeError: Cannot read properties of undefined (reading 'price')",
    "    at renderCart (/usr/src/app/dist/cart.js:211:34)",
    "    at processTicksAndRejections (node:internal/process/task_queues:95:5)",
    "> storefront@4.3.0 start",
];

const HEX: &[u8; 16] = b"0123456789abcdef";

/// `n` hex digits drawn from `seed`, a fresh hash every 16.
fn hex(seed: u64, n: usize) -> String {
    (0..n)
        .map(|k| {
            let h = mix(seed ^ ((k / 16) as u64 * 0x2545_F491_4F6C_DD1D)) >> (4 * (k % 16));
            HEX[(h & 0xf) as usize] as char
        })
        .collect()
}

/// The lower-case alphanumerics Kubernetes draws a pod's suffixes from.
fn suffix(mut h: u64, n: usize) -> String {
    const ALPHA: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    (0..n)
        .map(|_| {
            let c = ALPHA[(h % ALPHA.len() as u64) as usize] as char;
            h /= ALPHA.len() as u64;
            c
        })
        .collect()
}

/// The app's own line: a zap-style JSON object, or a stack-trace or text
/// line for the one in ten that is not.
fn app_line(s: &Service, i: u64, ts: &str, a: u64, b: u64, c: u64) -> (String, &'static str) {
    if a.is_multiple_of(10) {
        let pool = match s.lang {
            Lang::Go => GO_TRACE,
            Lang::Java => JAVA_TRACE,
            Lang::Python => PYTHON_TRACE,
            Lang::Node => NODE_TRACE,
        };
        // A runtime writes its traces to stderr; npm's banner goes to stdout.
        let text = pick(pool, b >> 3);
        let stream = if text.starts_with('>') { "stdout" } else { "stderr" };
        return (text.to_string(), stream);
    }

    let level = pick(LEVELS, b >> 5);
    let msg = match level {
        "info" => pick(INFO_MSGS, c >> 7),
        "debug" => pick(DEBUG_MSGS, c >> 7),
        "warn" => pick(WARN_MSGS, c >> 7),
        _ => pick(ERROR_MSGS, c >> 7),
    };
    let mut fields = vec![
        format!("\"level\":\"{level}\""),
        format!("\"ts\":\"{ts}\""),
        format!("\"caller\":\"{}/{}.go:{}\"", s.app, s.container, 40 + (c >> 12) % 300),
        format!("\"msg\":\"{msg}\""),
    ];
    // A request the mesh traced carries its ids; background work does not.
    if !(b >> 9).is_multiple_of(5) {
        fields.push(format!("\"trace_id\":\"{}\"", hex(c ^ i, 32)));
        fields.push(format!("\"span_id\":\"{}\"", hex(b ^ c, 16)));
    }
    // Request logs: the route and how it went. One line in three is not
    // about a request and carries none of this.
    if !(c >> 20).is_multiple_of(3) {
        let route = pick(ROUTES, c >> 24);
        let status = match level {
            "error" => pick(&[500u16, 502, 503, 504], a >> 9),
            "warn" => pick(&[200u16, 404, 409, 429], a >> 9),
            _ => pick(&[200u16, 200, 200, 201, 204, 304], a >> 9),
        };
        fields.push(format!("\"method\":\"{}\"", pick(METHODS, a >> 13)));
        fields.push(format!("\"path\":\"{route}\""));
        fields.push(format!("\"status\":{status}"));
        fields.push(format!("\"duration_ms\":{:.3}", ((a >> 20) % 250_000) as f64 / 1000.0));
        fields.push(format!(
            "\"remote_addr\":\"10.{}.{}.{}\"",
            32 + (b >> 40) % 8,
            (b >> 48) % 256,
            1 + (c >> 40) % 254
        ));
    }
    if level == "error" {
        let err = pick(
            &[
                "dial tcp 10.96.14.7:5432: i/o timeout",
                "rpc error: code = Unavailable desc = connection refused",
                "kafka: client has run out of available brokers",
                "EOF",
            ],
            c >> 33,
        );
        fields.push(format!("\"err\":\"{err}\""));
        fields.push(format!("\"attempt\":{}", 1 + (a >> 30) % 3));
    }
    if s.namespace == "shop" && (a >> 34).is_multiple_of(2) {
        fields.push(format!("\"user_id\":\"u-{:07}\"", (a >> 36) % 2_000_000));
        fields.push(format!("\"order_id\":\"ord-{}\"", hex(a, 12)));
    }
    (format!("{{{}}}", fields.join(",")), "stdout")
}

/// One Fluent Bit record, as the Kafka output writes it.
pub fn line(i: u64) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));

    let s = &SERVICES[(a >> 40) as usize % SERVICES.len()];
    // Three replicas a deployment, each with its own pod name, node and
    // container id, all stable for the corpus's run.
    let replica = (b >> 50) % 3;
    let app_seed = s.app.bytes().fold(0xCBF2_9CE4_8422_2325u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100_0000_01B3)
    });
    let pod_seed = mix(app_seed ^ (replica + 1).wrapping_mul(0x9E37_79B9));
    let template = suffix(mix(app_seed), 10);
    let pod = format!("{}-{}-{}", s.app, template, suffix(pod_seed, 5));
    let node = format!(
        "ip-10-0-{}-{}.ec2.internal",
        (pod_seed >> 20) % 64,
        (pod_seed >> 28) % 254 + 1
    );

    let secs = i * 2 + (a % 2);
    let (hh, mm, ss) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let ts = format!("2026-10-01T{hh:02}:{mm:02}:{ss:02}.{:09}Z", b % 1_000_000_000);

    let (log, stream) = app_line(s, i, &ts, a, b, c);
    let log = serde_json::to_string(&log).expect("a string serializes");
    let pid = format!(
        "{}-{}-{}-{}-{}",
        hex(pod_seed, 8),
        hex(pod_seed >> 3, 4),
        hex(pod_seed >> 7, 4),
        hex(pod_seed >> 11, 4),
        hex(mix(pod_seed), 12)
    );

    format!(
        "{{\"time\":\"{ts}\",\"stream\":\"{stream}\",\"_p\":\"F\",\"log\":{log},\"kubernetes\":{{\
         \"pod_name\":\"{pod}\",\"namespace_name\":\"{ns}\",\"pod_id\":\"{pid}\",\"labels\":{{\
         \"app.kubernetes.io/name\":\"{app}\",\"app.kubernetes.io/version\":\"{ver}\",\
         \"pod-template-hash\":\"{template}\"}},\"host\":\"{node}\",\
         \"container_name\":\"{container}\",\"docker_id\":\"{cid}\",\
         \"container_hash\":\"registry.acme.internal/{ns}/{app}@sha256:{digest}\",\
         \"container_image\":\"registry.acme.internal/{ns}/{app}:{ver}\"}}}}",
        ns = s.namespace,
        app = s.app,
        ver = s.version,
        container = s.container,
        cid = hex(mix(pod_seed ^ 1), 64),
        digest = hex(app_seed ^ 0xD16E57, 64),
    )
}

// -- the pipeline -------------------------------------------------------------

/// The keys the services' JSON carries beyond what the document keeps. The
/// app parse lays every key of the line at the top level, so each is named
/// here to be taken out again.
const APP_EXTRAS: &[&str] = &[
    "ts",
    "caller",
    "span_id",
    "method",
    "path",
    "status",
    "duration_ms",
    "remote_addr",
    "err",
    "attempt",
    "user_id",
    "order_id",
];

fn bundle() -> String {
    // Read by the steps above and not carried into the document.
    let prune: Vec<_> = ["_raw", "_p", "msg", "kubernetes"]
        .iter()
        .chain(APP_EXTRAS)
        .map(|n| serde_json::json!({"target": n, "expr": "null"}))
        .collect();
    let steps = vec![
        // The envelope. `kubernetes` is a closed object: the three names the
        // document keeps, and the filter's labels, ids and image hashes
        // dropped at the parse.
        serde_json::json!({
            "id": "envelope", "type": "parser", "input": "_raw",
            "kind": {"type": "json"},
            "declared": [
                {"name": "time", "type": "datetime"},
                {"name": "stream", "type": "str"},
                {"name": "log", "type": "str"},
                {"name": "kubernetes", "type": {"type": "struct", "fields": [
                    {"name": "namespace_name", "type": "str"},
                    {"name": "pod_name", "type": "str"},
                    {"name": "container_name", "type": "str"},
                ]}},
            ],
        }),
        // The envelope's `log` is the app's line, and ECS's `log` is an
        // object: the line takes the name it ends under, the message, and
        // `log` is free for `log.level`.
        serde_json::json!({
            "id": "line", "type": "rename",
            "pairs": [{"from": "log", "to": "message"}],
        }),
        // The app's line. Text that is not JSON leaves every field here null
        // and the row goes on.
        serde_json::json!({
            "id": "app", "type": "parser", "input": "message",
            "kind": {"type": "json"},
            "declared": [
                {"name": "level", "type": "str"},
                {"name": "msg", "type": "str"},
                {"name": "trace_id", "type": "str"},
            ],
        }),
        // A JSON line's message is its `msg`; any other line's is the line.
        serde_json::json!({
            "id": "message", "type": "eval",
            "assignments": [{"target": "message", "expr": "msg ?? message"}],
        }),
        serde_json::json!({
            "id": "to-ecs", "type": "rename",
            "pairs": [
                {"from": "time", "to": "@timestamp"},
                {"from": "level", "to": "log.level"},
                {"from": "trace_id", "to": "trace.id"},
                {"from": "kubernetes.namespace_name", "to": "orchestrator.namespace"},
                {"from": "kubernetes.pod_name", "to": "orchestrator.pod"},
                {"from": "kubernetes.container_name", "to": "orchestrator.container"},
            ],
        }),
        serde_json::json!({"id": "prune", "type": "eval", "assignments": prune}),
    ];
    super::bundle(steps, vec![])
}

// -- the check ----------------------------------------------------------------

/// Each kept document against the record it came from: the envelope's
/// names moved, the app's fields lifted where the line was JSON, the text
/// kept as the message where it was not.
fn check(rows: u64, docs: u64, kept: &[serde_json::Value]) {
    assert_eq!(docs, rows, "every line, JSON or not, should reach the destination");
    let mut text_rows = 0;
    for (n, doc) in kept.iter().enumerate() {
        let rec: serde_json::Value = serde_json::from_str(&line(n as u64)).expect("the generator writes JSON");
        let k = &rec["kubernetes"];
        let log = rec["log"].as_str().unwrap();
        assert_eq!(doc["orchestrator"]["namespace"], k["namespace_name"], "doc {n}: {doc}");
        assert_eq!(doc["orchestrator"]["pod"], k["pod_name"], "doc {n}: {doc}");
        assert_eq!(doc["orchestrator"]["container"], k["container_name"], "doc {n}: {doc}");
        assert_eq!(doc["stream"], rec["stream"], "doc {n}: {doc}");
        let ts = doc["@timestamp"]
            .as_str()
            .unwrap_or_else(|| panic!("doc {n} has no @timestamp: {doc}"));
        assert!(ts.starts_with(&rec["time"].as_str().unwrap()[..19]), "doc {n}: {ts}");

        match serde_json::from_str::<serde_json::Value>(log) {
            Ok(app) => {
                assert_eq!(doc["message"], app["msg"], "doc {n}: {doc}");
                assert_eq!(doc["log"]["level"], app["level"], "doc {n}: {doc}");
                assert_eq!(doc.pointer("/trace/id"), app.get("trace_id"), "doc {n}: {doc}");
                assert!(doc.pointer("/smeltr/exception").is_none(), "doc {n}: {doc}");
            }
            Err(_) => {
                text_rows += 1;
                assert_eq!(doc["message"], log, "doc {n}: {doc}");
                assert!(doc.get("log").is_none() && doc.get("trace").is_none(), "doc {n}: {doc}");
            }
        }
        for gone in [
            "kubernetes",
            "time",
            "msg",
            "_raw",
            "_p",
            "caller",
            "ts",
            "span_id",
            "path",
        ] {
            assert!(doc.get(gone).is_none(), "doc {n} still has `{gone}`: {doc}");
        }
    }
    assert!(
        text_rows > 0,
        "the kept documents should include a line that is not JSON"
    );
}
