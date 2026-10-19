//! A service's structured JSON log, normalized to ECS: `docs/examples.md`,
//! "1. JSON application log".
//!
//! The pipeline is the shape most in-house services need: a JSON parse
//! with the keys the mapping computes with declared, an `if` that drops
//! debug lines, and the moves from the app's names to ECS ones. No lookup.

use super::Pipeline;

pub const PIPELINE: Pipeline = Pipeline {
    name: "json_app",
    line,
    bundle,
    check,
};

/// splitmix64. A record is a pure function of its index, so a run is
/// reproducible and two runs of different lengths agree row for row.
fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The services one log stream carries, with whether their lines name an
/// order. A shared topic for a handful of services is the common
/// deployment, and only the order path carries `order_id`.
const LOGGERS: &[(&str, bool)] = &[
    ("checkout", true),
    ("checkout.payment", true),
    ("cart", true),
    ("inventory.reserve", true),
    ("auth.session", false),
    ("search.query", false),
    ("http.access", true),
    ("notify.email", true),
];

/// What each logger says at info, in `LOGGERS` order.
const INFO: &[&str] = &[
    "order placed",
    "payment authorized",
    "item added to cart",
    "stock reserved",
    "session refreshed",
    "query served",
    "request completed",
    "confirmation email queued",
];

/// What each logger says at warn, in `LOGGERS` order.
const WARN: &[&str] = &[
    "retrying payment",
    "slow upstream response",
    "cart total recalculated after price change",
    "reservation nearly expired",
    "token close to expiry",
    "query fell back to cached results",
    "request exceeded latency budget",
    "smtp relay deferred message",
];

const DEBUG: &[&str] = &[
    "computed cart totals",
    "cache hit for price list",
    "loaded feature flags",
    "opened db connection from pool",
];

/// Error lines and the exception each carries in `error`.
const ERRORS: &[(&str, &str, &str)] = &[
    (
        "payment declined by processor",
        "PaymentDeclinedError",
        "card_declined: insufficient_funds",
    ),
    (
        "inventory service unavailable",
        "UpstreamTimeoutError",
        "GET http://inventory:8080/v1/reserve timed out after 2000ms",
    ),
    (
        "failed to persist order",
        "IntegrityError",
        "duplicate key value violates unique constraint \"orders_pkey\"",
    ),
    (
        "unhandled exception in request handler",
        "KeyError",
        "'shipping_address'",
    ),
];

const TIERS: &[&str] = &["free", "free", "free", "silver", "silver", "gold", "platinum"];

const ROUTES: &[(&str, &str)] = &[
    ("POST", "/api/v1/checkout"),
    ("POST", "/api/v1/cart/items"),
    ("GET", "/api/v1/cart"),
    ("GET", "/api/v1/orders"),
    ("GET", "/api/v1/products/search"),
    ("PUT", "/api/v1/orders/status"),
];

/// The level a record logs at: mostly info, some warn and debug, a few
/// errors. The debug share is what the drop removes.
fn level(i: u64) -> &'static str {
    match mix(i ^ 0x1E7E1) % 100 {
        0..=2 => "error",
        3..=14 => "warn",
        15..=26 => "debug",
        _ => "info",
    }
}

/// Record `i`: one line of the service's JSON log, keys in the order its
/// logging library writes them.
fn line(i: u64) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));
    let level = level(i);
    let j = (a % LOGGERS.len() as u64) as usize;
    let (logger, has_order) = LOGGERS[j];
    let k = (b % 8) as usize;

    // One day of traffic, millisecond precision, as most JSON loggers write.
    let ms = (i * 1_727) % 86_400_000;
    let ts = format!(
        "2026-10-01T{:02}:{:02}:{:02}.{:03}Z",
        ms / 3_600_000,
        ms / 60_000 % 60,
        ms / 1000 % 60,
        ms % 1000
    );

    let msg = match level {
        "error" => ERRORS[k % ERRORS.len()].0,
        "warn" => WARN[j],
        "debug" => DEBUG[k % DEBUG.len()],
        _ => INFO[j],
    };
    let attempt = if level == "warn" || level == "error" {
        1 + (c % 4)
    } else {
        1
    };
    // Latency is long-tailed. A third of the lines come from an integer
    // timer, which JSON carries without a fraction, and `f64` must take both.
    let base = 3
        + (b >> 8) % 120
        + if (c >> 4).is_multiple_of(20) {
            (c >> 12) % 4_000
        } else {
            0
        };
    let latency = if a.is_multiple_of(3) {
        format!("{base}")
    } else {
        format!("{base}.{}", (b >> 20) % 10)
    };

    let mut s = String::with_capacity(512);
    s.push_str(&format!(
        r#"{{"ts":"{ts}","level":"{level}","logger":"{logger}","msg":"{msg}","attempt":{attempt}"#
    ));
    if has_order {
        s.push_str(&format!(r#","order_id":"o-{}""#, 10_000 + (a >> 12) % 990_000));
    }
    s.push_str(&format!(
        r#","user":{{"id":"u-{}","tier":"{}"}},"latency_ms":{latency}"#,
        (b >> 16) % 250_000,
        TIERS[((b >> 40) % TIERS.len() as u64) as usize]
    ));
    // Services instrumented with OpenTelemetry write the trace context;
    // most of the fleet is, some is not yet.
    if !(c >> 8).is_multiple_of(4) {
        s.push_str(&format!(r#","trace_id":"{:016x}{:016x}","span_id":"{:016x}""#, a, b, c));
    }
    if logger == "http.access" || logger == "checkout" {
        let (method, path) = ROUTES[((c >> 20) % ROUTES.len() as u64) as usize];
        let status = match level {
            "error" => 500 + (c >> 30) % 4,
            "warn" => [409, 429, 404][((c >> 30) % 3) as usize],
            _ => [200, 200, 200, 201, 204][((c >> 30) % 5) as usize],
        };
        s.push_str(&format!(
            r#","http":{{"method":"{method}","path":"{path}","status_code":{status},"client_ip":"10.{}.{}.{}"}}"#,
            (a >> 24) % 32,
            (a >> 32) % 256,
            1 + (a >> 40) % 254
        ));
    }
    if level == "error" {
        let (_, kind, message) = ERRORS[k % ERRORS.len()];
        s.push_str(&format!(
            r#","error":{{"type":"{kind}","message":"{}","stack":"Traceback (most recent call last):\n  File \"/srv/app/{}/handlers.py\", line {}, in handle\n    return self.process(req)\n{kind}: {}"}}"#,
            message.replace('"', "\\\""),
            logger.split('.').next().unwrap(),
            40 + c % 400,
            message.replace('"', "\\\"")
        ));
    }
    s.push('}');
    s
}

fn bundle() -> String {
    // Typed at the parse: `level` is compared, the other three change type.
    // Everything else is read as it arrived.
    let parse = serde_json::json!({
        "id": "parse", "type": "parser", "input": "_raw",
        "kind": {"type": "json"},
        "declared": [
            {"name": "ts", "type": "datetime"},
            {"name": "level", "type": "str"},
            {"name": "attempt", "type": "i64"},
            {"name": "latency_ms", "type": "f64"},
        ],
    });
    let no_debug = serde_json::json!({
        "id": "no-debug", "type": "if", "condition": r#"level == "debug""#,
        "then": [{"id": "drop-debug", "type": "drop", "condition": "true"}],
        "else": [],
    });
    let derive = serde_json::json!({
        "id": "derive", "type": "eval",
        "assignments": [
            // ECS durations are nanoseconds, a long.
            {"target": "event.duration", "expr": "round(latency_ms * 1000000)"},
            // Undeclared keys, copied as they arrived.
            {"target": "log.logger", "expr": "$.logger"},
            {"target": "message", "expr": "$.msg"},
            {"target": "labels.order_id", "expr": "$.order_id"},
            {"target": "labels.tier", "expr": "$.user.tier"},
            // The app's `error` is ECS-shaped already but for the stack's
            // name, and it stays.
            {"target": "error.stack_trace", "expr": "$.error.stack"},
        ],
    });
    let renames = [
        ("ts", "@timestamp"),
        ("level", "log.level"),
        ("attempt", "labels.attempt"),
    ];
    let to_ecs = serde_json::json!({
        "id": "to-ecs", "type": "rename",
        "pairs": renames.iter().map(|(f, t)| serde_json::json!({"from": f, "to": t})).collect::<Vec<_>>(),
    });
    // Every non-null field is serialized, so nulling is how a field leaves
    // the document: what the ECS literal does not name is gone.
    let dropped = [
        "_raw",
        "latency_ms",
        "logger",
        "msg",
        "order_id",
        "user.tier",
        "trace_id",
        "span_id",
        "http",
        "error.stack",
    ];
    let prune = serde_json::json!({
        "id": "prune", "type": "eval",
        "assignments": dropped.iter().map(|n| serde_json::json!({"target": n, "expr": "null"})).collect::<Vec<_>>(),
    });
    super::bundle(vec![parse, no_debug, derive, to_ecs, prune], vec![])
}

fn check(rows: u64, docs: u64, kept: &[serde_json::Value]) {
    // `rows` is what followed the warm-up batch, which the check is not
    // told the size of, so the debug count is the corpus's share of it.
    let debug = (0..super::CORPUS).filter(|i| level(*i) == "debug").count() as f64 / super::CORPUS as f64;
    assert!(
        (0.08..0.16).contains(&debug),
        "the corpus should carry some debug lines: {debug}"
    );
    let want = rows as f64 * (1.0 - debug);
    assert!(
        (docs as f64 - want).abs() < rows as f64 * 0.03,
        "every row but the debug ones should reach the destination: {docs} of {rows}, want about {want:.0}"
    );

    // The kept documents are the first non-debug records, in order.
    let inputs = (0..)
        .filter(|i| level(*i) != "debug")
        .map(|i| serde_json::from_str::<serde_json::Value>(&line(i)).expect("the generator writes JSON"));
    for (doc, src) in kept.iter().zip(inputs) {
        let ts = src["ts"].as_str().unwrap();
        let at = doc["@timestamp"].as_str().expect("@timestamp is a string");
        assert_eq!(&at[..19], &ts[..19], "@timestamp: {doc}");
        assert_eq!(doc["log"]["level"], src["level"], "log.level: {doc}");
        assert_eq!(doc["log"]["logger"], src["logger"], "log.logger: {doc}");
        assert_eq!(doc["message"], src["msg"], "message: {doc}");
        let nanos = (src["latency_ms"].as_f64().unwrap() * 1e6).round() as i64;
        assert_eq!(doc["event"]["duration"].as_i64(), Some(nanos), "event.duration: {doc}");
        assert_eq!(doc["user"], serde_json::json!({"id": src["user"]["id"]}), "user: {doc}");
        assert_eq!(doc["labels"]["tier"], src["user"]["tier"], "labels.tier: {doc}");
        assert_eq!(
            doc["labels"]["attempt"].as_i64(),
            src["attempt"].as_i64(),
            "labels.attempt: {doc}"
        );
        assert_eq!(
            doc["labels"].get("order_id"),
            src.get("order_id"),
            "labels.order_id: {doc}"
        );
        let mut want = vec!["@timestamp", "event", "labels", "log", "message", "user"];
        if let Some(e) = src.get("error") {
            // The app's `error.message` arrives as the app wrote it.
            let ecs = serde_json::json!({"message": e["message"], "type": e["type"], "stack_trace": e["stack"]});
            assert_eq!(doc["error"], ecs, "error: {doc}");
            want.insert(1, "error");
        }
        let mut keys: Vec<&str> = doc.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, want, "only what the ECS mapping names: {doc}");
    }
}
