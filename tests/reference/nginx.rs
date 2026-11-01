//! nginx access log, combined format, normalized to ECS: one Grok pattern
//! over the line, a GeoIP lookup on the client address, and the field
//! mapping. `docs/examples.md`, "Web server access log, Grok (nginx)".

use super::{Clock, Pipeline, mix, pick};

pub const PIPELINE: Pipeline = Pipeline {
    name: "nginx",
    line,
    bundle,
    check,
};

/// What a public storefront's access log carries: browsers on every
/// platform, the crawlers that index it, and the scripts that poll its API.
const AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.127 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "curl/8.5.0",
    "python-requests/2.32.3",
    "Go-http-client/2.0",
    "kube-probe/1.30",
];

const REFERRERS: &[&str] = &[
    "https://shop.example.com/",
    "https://shop.example.com/products",
    "https://shop.example.com/cart",
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "https://t.co/",
];

const SEARCHES: &[&str] = &[
    "running+shoes",
    "usb-c+cable",
    "rain+jacket",
    "coffee+grinder",
    "wireless%20earbuds",
    "desk+lamp",
];

const USERS: &[&str] = &["alice", "bob", "carol", "deploy", "svc-monitor"];

/// The request a row carries: its method, target, a status that fits it,
/// and the size of the body such a response has.
fn request(r: u64) -> (&'static str, String, u16, u64) {
    let kind = r % 100;
    let id = (r >> 8) % 90_000 + 1_000;
    let hash = (r >> 20) & 0xFFFF_FFFF;
    let s = (r >> 40) % 100;
    match kind {
        // Static assets: the bulk of a storefront's lines, and a share
        // served from the browser cache as 304 with no body.
        0..=34 => {
            let path = match kind % 4 {
                0 => format!("/static/js/app.{hash:08x}.js"),
                1 => format!("/static/css/main.{hash:08x}.css"),
                2 => format!("/images/products/{id}/thumb.webp"),
                _ => format!("/images/products/{id}/large.jpg"),
            };
            if s < 25 {
                ("GET", path, 304, 0)
            } else if s < 27 {
                ("GET", path, 404, 153)
            } else {
                ("GET", path, 200, 2_000 + (r >> 12) % 480_000)
            }
        }
        35..=49 => {
            let path = match kind % 3 {
                0 => "/".to_string(),
                1 => format!("/products/{id}"),
                _ => format!("/search?q={}&page={}", pick(SEARCHES, r >> 16), 1 + (r >> 24) % 5),
            };
            ("GET", path, if s < 2 { 500 } else { 200 }, 8_000 + (r >> 12) % 90_000)
        }
        50..=69 => {
            let path = match kind % 3 {
                0 => format!(
                    "/api/v1/products?category={}&sort=price&page={}",
                    id % 40,
                    1 + (r >> 24) % 9
                ),
                1 => format!("/api/v1/products/{id}"),
                _ => "/api/v1/cart".to_string(),
            };
            let status = match s {
                0..=1 => 502,
                2..=4 => 401,
                5..=6 => 499,
                _ => 200,
            };
            (
                "GET",
                path,
                status,
                if status == 200 { 300 + (r >> 12) % 40_000 } else { 0 },
            )
        }
        70..=79 => {
            let (path, ok) = match kind % 3 {
                0 => ("/api/v1/cart/items".to_string(), 201),
                1 => ("/api/v1/orders".to_string(), 201),
                _ => ("/login".to_string(), 302),
            };
            let status = match s {
                0..=3 => 422,
                4..=5 => 503,
                _ => ok,
            };
            ("POST", path, status, 60 + (r >> 12) % 2_000)
        }
        80..=82 => ("PUT", format!("/api/v1/cart/items/{id}"), 204, 0),
        83 => ("DELETE", format!("/api/v1/cart/items/{id}"), 204, 0),
        84..=85 => ("OPTIONS", "/api/v1/cart".to_string(), 204, 0),
        86..=88 => ("HEAD", "/".to_string(), 200, 0),
        89..=91 => ("GET", "/healthz".to_string(), 200, 2),
        92..=93 => ("GET", "/favicon.ico".to_string(), 200, 15_086),
        94 => ("GET", "/robots.txt".to_string(), 200, 67),
        95 => ("GET", "/sitemap.xml".to_string(), 200, 48_211),
        96 => ("GET", "/old-catalog/index.php".to_string(), 301, 169),
        // Scanners probing for software the site does not run.
        _ => {
            let path = ["/wp-login.php", "/.env", "/admin/config.php", "/.git/config"][(r >> 16) as usize % 4];
            (if s < 50 { "GET" } else { "POST" }, path.to_string(), 404, 153)
        }
    }
}

/// The client address. Internal users reach the site from the office
/// ranges the GeoIP table knows (`10.1.0.0/16`, `10.2.0.0/24`); the rest
/// come from the internet, where the test database has no answer. Tied to
/// `i % 10` so any window of rows, the checked one included, holds both.
fn client(i: u64, r: u64) -> String {
    match i % 10 {
        0 | 3 | 6 => format!("10.1.{}.{}", (r >> 8) % 256, (r >> 16) % 254 + 1),
        8 => format!("10.2.0.{}", (r >> 8) % 254 + 1),
        9 => format!("2001:db8:{:x}::{:x}", (r >> 8) % 0x1_0000, (r >> 24) % 0x1_0000),
        1 => ["81.2.69.142", "89.160.20.112", "216.160.83.56", "175.16.199.4"][(r >> 8) as usize % 4].to_string(),
        _ => format!(
            "{}.{}.{}.{}",
            [203, 198, 192, 45, 66, 104, 172][(r >> 8) as usize % 7],
            (r >> 16) % 256,
            (r >> 24) % 256,
            (r >> 32) % 254 + 1
        ),
    }
}

/// One line as nginx's `combined` log format writes it.
pub fn line(i: u64) -> String {
    record(i, &Clock::of(i), false)
}

/// Line `i` of the log, written at `at`. With `failing`, the API's upstream is
/// failing: a quarter of the API's requests end in a 5xx.
pub fn record(i: u64, at: &Clock, failing: bool) -> String {
    let a = mix(i);
    let b = mix(i ^ 0x5DEE_CE66);
    let c = mix(i.wrapping_mul(31));

    // Most servers log UTC; a share keep the host's local zone.
    let (zone, offset) = [("+0000", 0), ("+0000", 0), ("+0000", 0), ("-0700", -7), ("+0200", 2)][(c >> 4) as usize % 5];
    let local = at.plus(offset * 3_600);

    let (method, path, mut status, mut bytes) = request(c);
    if failing && path.starts_with("/api/") && (a >> 52).is_multiple_of(4) {
        (status, bytes) = [(500, 212), (502, 157), (503, 190)][(a >> 56) as usize % 3];
    }
    let user = if b % 100 < 8 { pick(USERS, b >> 32) } else { "-" };
    let version = match (a >> 20) % 10 {
        0..=5 => "1.1",
        6..=8 => "2.0",
        _ => "1.0",
    };
    let agent = pick(AGENTS, b >> 40);
    let referrer = if a % 100 < 35 || agent.starts_with("curl") || agent.starts_with("kube") {
        "-"
    } else {
        pick(REFERRERS, a >> 40)
    };

    format!(
        "{client} - {user} [{day:02}/{month}/{year}:{hms} {zone}] \"{method} {path} HTTP/{version}\" \
         {status} {bytes} \"{referrer}\" \"{agent}\"",
        client = client(i, a),
        day = local.day,
        month = local.mon(),
        year = local.year,
        hms = local.hms(),
    )
}

/// A GeoIP2 City database, read by path. The repository's fixture knows
/// `10.1.0.0/16` (US, Springfield) and `10.2.0.0/24` (CA, Toronto), and
/// carries no location.
fn geoip_table() -> serde_json::Value {
    let path = super::lookup_file(
        "city.mmdb",
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/lookup/test-data/test-city.mmdb"
        )),
    );
    serde_json::json!({ "id": "geoip", "type": "mmdb", "path": path })
}

/// The client's location, from the GeoIP table.
fn geo_step() -> serde_json::Value {
    serde_json::json!({
        "id": "geo", "type": "lookup", "input": "client", "table": "geoip",
        "matchKind": "cidr", "multiplicity": "best",
        "outputs": [
            {"source": "country.iso_code", "field": "source.geo.country_iso_code"},
            {"source": "city.names.en", "field": "source.geo.city_name"},
            {"source": "location.latitude", "field": "source.geo.location.lat", "type": "f64"},
            {"source": "location.longitude", "field": "source.geo.location.lon", "type": "f64"},
        ],
    })
}

/// Read by the evals and not carried into the document.
const PRUNED: [&str; 4] = ["_raw", "remote_user", "target", "referrer"];

fn steps() -> Vec<serde_json::Value> {
    let renames = [
        ("ts", "@timestamp"),
        ("client", "source.ip"),
        ("method", "http.request.method"),
        ("version", "http.version"),
        ("status", "http.response.status_code"),
        ("bytes", "http.response.body.bytes"),
        ("agent", "user_agent.original"),
    ];
    vec![
        serde_json::json!({
            "id": "access", "type": "parser", "input": "_raw",
            "kind": {
                "type": "grok",
                "patterns": [
                    r#"%{IP:client} - %{DATA:remote_user} \[%{HTTPDATE:ts}\] "%{WORD:method} %{NOTSPACE:target} HTTP/%{NUMBER:version}" %{INT:status} %{INT:bytes} "%{DATA:referrer}" "%{DATA:agent}""#,
                ],
            },
            // Grok captures are text; the declared types make the two
            // numbers numbers and the time a time at the parse.
            "declared": [
                {"name": "client", "type": "str"},
                {"name": "remote_user", "type": "str"},
                {"name": "ts", "type": {"type": "datetime", "format": "%d/%b/%Y:%H:%M:%S %z"}},
                {"name": "method", "type": "str"},
                {"name": "target", "type": "str"},
                {"name": "version", "type": "str"},
                {"name": "status", "type": "i64"},
                {"name": "bytes", "type": "i64"},
                {"name": "referrer", "type": "str"},
                {"name": "agent", "type": "str"},
            ],
        }),
        geo_step(),
        serde_json::json!({
            "id": "derive", "type": "eval",
            "assignments": [
                // nginx writes `-` for a field it has no value for.
                {"target": "http.request.referrer", "expr": r#"referrer == "-" ? null : referrer"#},
                {"target": "user.name", "expr": r#"remote_user == "-" ? null : remote_user"#},
                {"target": "url.path", "expr": r#"target.split("?")[0]"#},
                // An index past the end reads null but also warns on the
                // row, so a target with no query is not indexed.
                {"target": "url.query", "expr": r#"target.includes("?") ? target.split("?")[1] : null"#},
                {"target": "event.category", "expr": r#""web""#},
                {"target": "event.outcome", "expr": r#"status >= 400 ? "failure" : "success""#},
            ],
        }),
        serde_json::json!({
            "id": "to-ecs", "type": "rename",
            "pairs": renames.iter().map(|(f, t)| serde_json::json!({"from": f, "to": t})).collect::<Vec<_>>(),
        }),
        serde_json::json!({
            "id": "prune", "type": "eval",
            "assignments": PRUNED.iter().map(|n| serde_json::json!({"target": n, "expr": "null"})).collect::<Vec<_>>(),
        }),
    ]
}

fn bundle() -> String {
    super::bundle(steps(), vec![geoip_table()])
}

/// Each kept document is row `i` of the corpus, in order: one source, one
/// destination. So each is checked against the line it came from, cut
/// apart here by hand rather than by the pattern under test.
fn check(rows: u64, docs: u64, kept: &[serde_json::Value]) {
    assert_eq!(docs, rows, "one document per line");
    let (mut queries, mut hits) = (0, 0);
    for (i, d) in kept.iter().enumerate() {
        let raw = line(i as u64);
        let quoted: Vec<&str> = raw.split('"').collect();
        let (head, request, tail) = (quoted[0], quoted[1], quoted[2]);
        let (client, user) = {
            let mut f = head.split(' ');
            (f.next().unwrap(), f.nth(1).unwrap())
        };
        let mut req = request.split(' ');
        let (method, target) = (req.next().unwrap(), req.next().unwrap());
        let mut nums = tail.split_whitespace().map(|n| n.parse::<i64>().unwrap());
        let (status, bytes) = (nums.next().unwrap(), nums.next().unwrap());

        let at = |p: &str| d.pointer(p).unwrap_or_else(|| panic!("row {i}: no {p} in {d}"));
        assert!(d.get("error").is_none(), "row {i} mapped with an error: {d}");
        assert_eq!(at("/source/ip"), client, "row {i}");
        assert_eq!(at("/http/request/method"), method, "row {i}");
        assert_eq!(
            at("/http/response/status_code").as_i64(),
            Some(status),
            "row {i}: a number"
        );
        assert_eq!(
            at("/http/response/body/bytes").as_i64(),
            Some(bytes),
            "row {i}: a number"
        );
        assert_eq!(at("/url/path"), target.split('?').next().unwrap(), "row {i}");
        assert_eq!(
            d.pointer("/url/query").and_then(|q| q.as_str()),
            target.split_once('?').map(|(_, q)| q)
        );
        queries += usize::from(target.contains('?'));
        assert_eq!(
            d.pointer("/user/name").and_then(|u| u.as_str()),
            (user != "-").then_some(user),
            "row {i}"
        );
        assert_eq!(
            d.pointer("/http/request/referrer").and_then(|r| r.as_str()),
            Some(quoted[3]).filter(|r| *r != "-"),
            "row {i}"
        );
        assert_eq!(at("/user_agent/original"), quoted[5], "row {i}");
        assert_eq!(at("/event/category"), "web");
        assert_eq!(
            at("/event/outcome"),
            if status >= 400 { "failure" } else { "success" },
            "row {i}"
        );
        let ts = at("/@timestamp").as_str().expect("a timestamp");
        assert!(
            ts.starts_with("2026-") && ts.ends_with('Z'),
            "row {i}: {ts} is UTC RFC 3339"
        );
        let geo = if client.starts_with("10.1.") {
            Some(("US", "Springfield"))
        } else if client.starts_with("10.2.0.") {
            Some(("CA", "Toronto"))
        } else {
            None
        };
        match geo {
            Some((country, city)) => {
                assert_eq!(at("/source/geo/country_iso_code"), country, "row {i}");
                assert_eq!(at("/source/geo/city_name"), city, "row {i}");
                hits += 1;
            }
            None => assert!(
                d.pointer("/source/geo").is_none(),
                "row {i}: {client} is not in the table"
            ),
        }
        for gone in PRUNED {
            assert!(d.get(gone).is_none(), "row {i}: {gone} is an intermediate");
        }
    }
    assert!(
        queries > 0 && queries < kept.len(),
        "the kept rows have targets with and without a query"
    );
    assert!(
        hits > 0 && hits < kept.len(),
        "the kept rows have GeoIP hits and misses"
    );
}
