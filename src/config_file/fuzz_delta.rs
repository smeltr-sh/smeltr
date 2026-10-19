//! Random Delta destination configs through the config loader.
//!
//! Two generators feed one check. `known` picks every field from a pool whose verdict is written beside each value,
//! so the fields the loader must refuse are known in advance and compared with what it refused. `garbage` puts
//! random text and values of the wrong JSON type in the same fields, where only the loader's conduct is checked.
//!
//! Either way the loader must not panic, must name a field in each refusal, and must build a Delta destination's
//! runtime config from each file it accepts. Nothing here reaches a network or a store.
//!
//! Seeded through `crate::fuzz`: a failure names its seed and the destination it generated, and `Coverage` fails
//! the run when an option, or a refusal at it, was never reached.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::dto::{BufferBytes, ObjectBytes};
use super::fuzz_loader::{self, Fuzzed, Pool, Verdict, garbage_value, parse, random_text, random_value};
use super::test_fixtures::example_json;
use crate::fuzz::Rng;
use crate::plan::topology::{DestinationKind, MAX_SENDERS};

/// The destination's id in every generated file.
const LAKE: &str = "lake";

/// The identity the generated file declares. Any other name is one it does not.
const DECLARED_IDENTITY: &str = "trust";

const LOCATIONS: Pool = &[
    (r#""s3://lake/tables/web""#, false),
    (r#""s3a://lake/web""#, false),
    (r#""s3://lake""#, false),
    (r#""s3://lake/""#, false),
    (r#""  s3://lake/web  ""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
    (r#""lake/web""#, true),
    (r#""https://lake/web""#, true),
    (r#""s3://""#, true),
    (r#""s3:///web""#, true),
    (r#""S3://lake/web""#, true),
    (r#""gs://lake/web""#, true),
];

/// Refused when it is no root URL.
const WORKSPACES: Pool = &[
    (r#""https://dbc-1234.cloud.databricks.com""#, false),
    (r#""https://dbc-1234.cloud.databricks.com/""#, false),
    (r#"" https://dbc-1234.cloud.databricks.com ""#, false),
    (r#""HTTPS://dbc-1234.cloud.databricks.com""#, false),
    (r#""http://localhost:8080""#, false),
    (r#""http://127.0.0.1:8080""#, false),
    (r#""http://[::1]:8080""#, false),
    (r#""http://dbc-1234.cloud.databricks.com""#, false),
    (r#""http://10.0.0.7:8080""#, false),
    (r#""dbc-1234.cloud.databricks.com""#, true),
    (r#""""#, true),
    (r#""https://dbc-1234.cloud.databricks.com/?o=1234""#, true),
    (r#""https://dbc-1234.cloud.databricks.com/browse""#, true),
    (r#""https://dbc-1234.cloud.databricks.com#x""#, true),
    (r#""ftp://dbc-1234.cloud.databricks.com""#, true),
    (r#""https://""#, true),
    (r#""http://localhost:8080/api""#, true),
];

const TABLES: Pool = &[
    (r#""main.logs.web""#, false),
    (r#"" main.logs.web ""#, false),
    (r#""a.b.c""#, false),
    (r#""web""#, true),
    (r#""logs.web""#, true),
    (r#""main.logs.web.v2""#, true),
    (r#""main..web""#, true),
    (r#""main. .web""#, true),
    (r#""main.logs.""#, true),
    (r#""""#, true),
];

const CLIENT_IDS: Pool = &[
    (r#""6f1c2a3b""#, false),
    (r#"" 6f1c2a3b ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

/// A credential is refused when its name is blank: no worker could look it up.
const SECRETS: Pool = &[
    (r#""DATABRICKS_CLIENT_SECRET""#, false),
    (r#"{"secret": "uc", "version": 3}"#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#"{"secret": " ", "version": 1}"#, true),
];

const REGIONS: Pool = &[(r#""us-west-2""#, false), (r#""""#, true), (r#"" ""#, true)];

const ROLES: Pool = &[
    (r#""arn:aws:iam::111122223333:role/smeltr""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
];

const EXTERNAL_IDS: Pool = &[(r#""ext-1""#, false), (r#""""#, true)];

/// An address a credential is sent to: the endpoint, and the authEndpoint.
struct Address {
    json: &'static str,
    https: bool,
}

impl Address {
    fn blank(&self) -> bool {
        serde_json::from_str::<String>(self.json).unwrap().trim().is_empty()
    }
}

const ADDRESSES: &[Address] = &[
    Address {
        json: r#""https://s3.us-west-2.amazonaws.com""#,
        https: true,
    },
    Address {
        json: r#""HTTPS://S3.example.com""#,
        https: true,
    },
    Address {
        json: r#""http://localhost:9000""#,
        https: false,
    },
    Address {
        json: r#""http://127.0.0.1:9000""#,
        https: false,
    },
    Address {
        json: r#""http://minio.internal:9000""#,
        https: false,
    },
    Address {
        json: r#""""#,
        https: false,
    },
    Address {
        json: r#""  ""#,
        https: false,
    },
];

/// A partition column is a top-level column's name.
const COLUMNS: Pool = &[
    (r#""host""#, false),
    (r#""day""#, false),
    (r#""region""#, false),
    (r#""http.host""#, true),
    (r#""day(ts)""#, true),
    (r#""a,b""#, true),
    (r#""""#, true),
    (r#"" ""#, true),
];

const COMPRESSIONS: Pool = &[
    (r#""none""#, false),
    (r#""gzip""#, false),
    (r#""snappy""#, false),
    (r#""lz4""#, false),
    (r#""zstd""#, false),
];

const IN_FLIGHTS: Pool = &[
    ("1", false),
    ("4", false),
    ("64", false),
    ("0", true),
    ("65", true),
    ("1000", true),
];

const OBJECT_BYTES: Pool = &[
    ("65536", false),
    (r#""64 KiB""#, false),
    (r#""64 MiB""#, false),
    (r#""64 GiB""#, false),
    ("68719476736", false),
    ("65535", true),
    ("0", true),
    (r#""65 GiB""#, true),
    ("68719476737", true),
];

const BUFFER_BYTES: Pool = &[
    ("4194304", false),
    (r#""256 MiB""#, false),
    (r#""1 GiB""#, false),
    (r#""32 TiB""#, false),
    ("4194303", true),
    ("0", true),
    (r#""33 TiB""#, true),
];

const STALL_AFTERS: Pool = &[("1", false), ("30", false), ("120", false), ("0", true), ("121", true)];

const FILE_INTERVALS: Pool = &[
    ("5", false),
    ("60", false),
    ("240", false),
    ("0", true),
    ("4", true),
    ("241", true),
    ("3600", true),
];

/// One generated destination, and what the loader must make of it.
struct Case {
    dest: Value,
    /// Whether it is sent rows, so compiling it works out its Parquet schema.
    routed: bool,
    /// The fields the loader must refuse, when `known` generated it.
    refused: Option<BTreeSet<&'static str>>,
}

impl Fuzzed for Case {
    const SECTION: &'static str = "destinations";
    const ID: &'static str = LAKE;
    const NOUN: &'static str = "destination";

    fn item(&self) -> &Value {
        &self.dest
    }

    fn refused(&self) -> Option<&BTreeSet<&'static str>> {
        self.refused.as_ref()
    }

    fn built(&self, system: &super::LoadedSystem) -> Result<(), String> {
        built(self, system)
    }
}

/// A destination of known values, and the fields among them the loader must refuse.
fn known(rng: &mut Rng) -> Case {
    let mut dest = json!({ "id": LAKE, "type": "delta" });
    let fields = dest.as_object_mut().unwrap();
    let mut refused = BTreeSet::new();
    // A clean case keeps every value inside its own field's rules, so whole configs are accepted often enough to
    // check what the loader builds from them.
    let clean = rng.chance(2);
    let take = |rng: &mut Rng, pool: Pool| -> (Value, bool) {
        let good: Vec<_> = pool.iter().filter(|(_, bad)| !clean || !bad).collect();
        let (json, bad) = rng.pick(&good);
        (parse(json), *bad)
    };
    // Present one time in `n`.
    let field = |rng: &mut Rng, n: usize, pool: Pool| rng.chance(n).then(|| take(rng, pool));

    let (has_location, has_uc) = match rng.below(10) {
        0 => (true, true),
        1 => (false, false),
        n => (n % 2 == 0, n % 2 == 1),
    };
    let has_uc = has_uc && !(clean && has_location);
    if has_location == has_uc {
        refused.insert("location");
    }
    // A table named both ways is refused before its Unity Catalog fields are looked at. Its credential is checked all
    // the same.
    let uc_only = has_uc && !has_location;

    if has_location {
        let (v, bad) = field(rng, 1, LOCATIONS).unwrap();
        fields.insert("location".into(), v);
        if bad && !has_uc {
            refused.insert("location");
        }
    }

    let mut uc = serde_json::Map::new();
    for (key, pool) in [
        ("workspace", WORKSPACES),
        ("table", TABLES),
        ("clientId", CLIENT_IDS),
        ("clientSecret", SECRETS),
    ] {
        let (v, bad) = field(rng, 1, pool).unwrap();
        let checked = match key {
            "clientSecret" => has_uc,
            _ => uc_only,
        };
        uc.insert(key.into(), v);
        if bad && checked {
            refused.insert(match key {
                "workspace" => "unityCatalog.workspace",
                "table" => "unityCatalog.table",
                "clientId" => "unityCatalog.clientId",
                _ => "unityCatalog.clientSecret",
            });
        }
    }
    if has_uc {
        fields.insert("unityCatalog".into(), Value::Object(uc));
    }

    // Storage credentials stay out of a clean case beside a Unity Catalog table, which vends its own.
    let credentials = if clean && has_uc { 0 } else { 3 };
    let region = field(rng, if clean && has_uc { 1 } else { 2 }, REGIONS);
    match &region {
        Some((_, true)) => {
            refused.insert("region");
        }
        None if uc_only => {
            refused.insert("region");
        }
        _ => {}
    }

    let identity = match rng.below(4) {
        0 => Some(DECLARED_IDENTITY),
        1 if !clean => Some("nobody"),
        _ => None,
    };
    if identity.is_some_and(|i| i != DECLARED_IDENTITY) {
        refused.insert("identity");
    }

    let endpoint = rng.chance(2).then(|| rng.pick(ADDRESSES));
    let endpoint_https = endpoint.is_none_or(|a| a.https);
    if let Some(a) = endpoint
        && a.blank()
    {
        refused.insert("endpoint");
    }
    // An identity is offered only over TLS.
    if identity.is_some() && !endpoint_https {
        refused.insert("endpoint");
    }

    let (key_id, secret_key) = if credentials == 0 {
        (None, None)
    } else {
        let both = rng.chance(2);
        let key_id = (both || rng.chance(credentials)).then(|| take(rng, SECRETS));
        let secret_key = (both || rng.chance(credentials)).then(|| take(rng, SECRETS));
        (key_id, secret_key)
    };
    for (name, half, other) in [
        ("accessKeyId", &key_id, &secret_key),
        ("secretAccessKey", &secret_key, &key_id),
    ] {
        match half {
            Some((_, bad)) if *bad || uc_only => {
                refused.insert(name);
            }
            None if other.is_some() => {
                refused.insert(name);
            }
            _ => {}
        }
    }

    let role = (credentials > 0 && rng.chance(2)).then(|| take(rng, ROLES));
    let external_id = (credentials > 0 && rng.chance(credentials)).then(|| take(rng, EXTERNAL_IDS));
    let auth_endpoint = (credentials > 0 && rng.chance(credentials)).then(|| rng.pick(ADDRESSES));
    if role.as_ref().is_some_and(|(_, bad)| *bad || uc_only)
        || (role.is_none() && (external_id.is_some() || auth_endpoint.is_some()))
    {
        refused.insert("roleArn");
    }
    if external_id.as_ref().is_some_and(|(_, bad)| *bad) {
        refused.insert("externalId");
    }
    if let Some(a) = auth_endpoint
        && (a.blank() || uc_only)
    {
        refused.insert("authEndpoint");
    }

    let storage = [
        ("region", region.map(|(v, _)| v)),
        ("endpoint", endpoint.map(|a| parse(a.json))),
        ("authEndpoint", auth_endpoint.map(|a| parse(a.json))),
        ("accessKeyId", key_id.map(|(v, _)| v)),
        ("secretAccessKey", secret_key.map(|(v, _)| v)),
        ("roleArn", role.map(|(v, _)| v)),
        ("externalId", external_id.map(|(v, _)| v)),
        ("identity", identity.map(Value::from)),
    ];
    for (key, value) in storage {
        if let Some(v) = value {
            fields.insert(key.into(), v);
        }
    }

    if rng.chance(2) {
        let mut columns = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..rng.below(4) {
            let (column, bad) = take(rng, COLUMNS);
            if clean && seen.contains(&column.to_string()) {
                continue;
            }
            if bad || !seen.insert(column.to_string()) {
                refused.insert("partitionBy");
            }
            columns.push(column);
        }
        fields.insert("partitionBy".into(), Value::Array(columns));
    }

    for (key, pool) in [
        ("compression", COMPRESSIONS),
        ("inFlight", IN_FLIGHTS),
        ("maxObjectBytes", OBJECT_BYTES),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
        ("fileIntervalSecs", FILE_INTERVALS),
    ] {
        if let Some((v, bad)) = field(rng, 2, pool) {
            fields.insert(key.into(), v);
            if bad {
                refused.insert(key);
            }
        }
    }

    Case {
        dest,
        routed: rng.chance(2),
        refused: Some(refused),
    }
}

/// Text cut from what breaks URL, size and name parsers.
const PIECES: &[&str] = &[
    "s3://",
    "s3a://",
    "https://",
    "http://",
    "lake",
    "web",
    "main",
    ".",
    "..",
    "/",
    "?o=1",
    "#",
    "[",
    "]",
    "[::1]",
    "::",
    "@",
    ":",
    ":0",
    ":99999",
    " ",
    "\t",
    "%",
    "%zz",
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "\0",
    "é",
    "😀",
    "\u{202e}",
    "xn--",
    "1",
    "1.5",
    "-",
    "e9",
    "GiB",
    "KB",
    "TiB",
    "B",
    ",",
    "_",
    "(",
    ")",
    "99999999999999999999",
    "\\",
    "\"",
];

/// A destination whose fields hold random text and values of any type.
fn garbage(rng: &mut Rng) -> Case {
    let mut dest = json!({ "id": LAKE, "type": "delta" });
    let fields = dest.as_object_mut().unwrap();
    let address: Pool = &[(r#""https://s3.us-west-2.amazonaws.com""#, false)];
    let declared: Pool = &[(r#""trust""#, false)];
    let density = 1 + rng.below(6);
    for (key, pool) in [
        ("location", LOCATIONS),
        ("region", REGIONS),
        ("endpoint", address),
        ("authEndpoint", address),
        ("accessKeyId", SECRETS),
        ("secretAccessKey", SECRETS),
        ("roleArn", ROLES),
        ("externalId", EXTERNAL_IDS),
        ("identity", declared),
        ("compression", COMPRESSIONS),
        ("inFlight", IN_FLIGHTS),
        ("maxObjectBytes", OBJECT_BYTES),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
        ("fileIntervalSecs", FILE_INTERVALS),
    ] {
        if rng.chance(2) {
            fields.insert(key.into(), garbage_value(rng, PIECES, pool, density));
        }
    }
    if rng.chance(2) {
        let columns = (0..rng.below(4))
            .map(|_| garbage_value(rng, PIECES, COLUMNS, density))
            .collect();
        fields.insert("partitionBy".into(), Value::Array(columns));
    }
    if rng.chance(2) {
        let mut uc = serde_json::Map::new();
        for (key, pool) in [
            ("workspace", WORKSPACES),
            ("table", TABLES),
            ("clientId", CLIENT_IDS),
            ("clientSecret", SECRETS),
        ] {
            if !rng.chance(8) {
                uc.insert(key.into(), garbage_value(rng, PIECES, pool, density));
            }
        }
        if rng.chance(16) {
            uc.insert(random_text(rng, PIECES), random_value(rng, PIECES));
        }
        fields.insert("unityCatalog".into(), Value::Object(uc));
    }
    if rng.chance(16) {
        fields.insert(random_text(rng, PIECES), random_value(rng, PIECES));
    }
    Case {
        dest,
        routed: rng.chance(2),
        refused: None,
    }
}

/// A self-signed certificate, for the declared identity to trust.
fn authority() -> String {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
    params.self_signed(&key).unwrap().pem()
}

/// The example bundle with `case`'s destination in it, the identity it may name, and, when it is routed, a route
/// delivering to it.
fn bundle(case: &Case, authority: &str) -> String {
    let mut file = parse(example_json());
    file["destinations"].as_array_mut().unwrap().push(case.dest.clone());
    file["identities"] = json!([{ "id": DECLARED_IDENTITY, "authorities": authority }]);
    if case.routed {
        let catch_all = file["routes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|r| r["id"] == "catch-all")
            .unwrap();
        catch_all["deliver"][0]["to"].as_array_mut().unwrap().push(LAKE.into());
    }
    file.to_string()
}

/// What a worker's egress is handed for the accepted destination, held to the rules it was accepted under.
fn built(case: &Case, system: &super::LoadedSystem) -> Result<(), String> {
    let dest = (system.topology.destinations.iter())
        .find(|d| d.name == LAKE)
        .ok_or("the accepted destination is not in the topology")?;
    let DestinationKind::Delta(c) = &dest.kind else {
        return Err(format!("built {:?}, not a Delta destination", dest.kind));
    };
    let has_uc = case.dest.get("unityCatalog").is_some();
    match &c.unity_catalog {
        Some(uc) if has_uc && c.location.is_empty() => {
            let parts: Vec<_> = uc.table.split('.').collect();
            if parts.len() != 3 || parts.iter().any(|p| p.trim().is_empty()) || uc.client_id.trim().is_empty() {
                return Err(format!("built a Unity Catalog table it should have refused: {uc:?}"));
            }
            if c.storage.region.as_deref().is_none_or(|r| r.trim().is_empty()) {
                return Err("built a Unity Catalog table with no region".to_string());
            }
            if c.storage.access_key.is_some() || c.storage.role.is_some() || c.storage.auth_endpoint.is_some() {
                return Err(format!(
                    "built storage credentials beside Unity Catalog: {:?}",
                    c.storage
                ));
            }
        }
        None if !has_uc && c.location.starts_with("s3") && c.location == c.location.trim() => {}
        _ => {
            return Err(format!(
                "built location {:?} and unityCatalog {:?}",
                c.location, c.unity_catalog
            ));
        }
    }
    let in_range = (1..=MAX_SENDERS).contains(&c.in_flight)
        && (ObjectBytes::MIN..=ObjectBytes::MAX).contains(&c.max_object_bytes)
        && (BufferBytes::MIN..=BufferBytes::OBJECT_MAX).contains(&c.buffer_bytes)
        && (1..=120).contains(&c.stall_after.as_secs())
        && (5..=240).contains(&c.file_interval.as_secs());
    if !in_range {
        return Err(format!("built limits out of range: {c:?}"));
    }
    let named = case.dest.get("identity").is_some();
    if c.identity.is_some() != named || !system.topology.identities.is_empty() != named {
        return Err(format!("named identity {named}, built {:?}", c.identity));
    }
    // What egress and the worker's memory budget read from the kind.
    let _ = (dest.kind.payload(), dest.kind.id_field());
    if dest.kind.stall_after() != c.stall_after || dest.kind.buffer_bytes() != c.buffer_bytes {
        return Err("the kind reports other limits than its config".to_string());
    }
    let compiled = system
        .compile_topology()
        .map_err(|e| format!("the accepted bundle does not compile: {e:?}"))?;
    if case.routed && !compiled.records.contains_key(&dest.id) {
        return Err("a routed Delta destination has no Parquet record".to_string());
    }
    Ok(())
}

/// Every option of a Delta destination, by its name in the file.
const OPTIONS: [&str; 21] = [
    "location",
    "unityCatalog",
    "unityCatalog.workspace",
    "unityCatalog.table",
    "unityCatalog.clientId",
    "unityCatalog.clientSecret",
    "partitionBy",
    "region",
    "endpoint",
    "authEndpoint",
    "accessKeyId",
    "secretAccessKey",
    "roleArn",
    "externalId",
    "identity",
    "compression",
    "inFlight",
    "maxObjectBytes",
    "bufferBytes",
    "stallAfterSecs",
    "fileIntervalSecs",
];

/// The options `case` sets.
fn options_set(case: &Case) -> Vec<String> {
    let mut set: Vec<String> = case.dest.as_object().unwrap().keys().cloned().collect();
    if let Some(Value::Object(uc)) = case.dest.get("unityCatalog") {
        set.extend(uc.keys().map(|k| format!("unityCatalog.{k}")));
    }
    set
}

/// Runs `generate` over the seeds (`fuzz_loader::run`), marking what each case reaches besides its refusals.
fn run(seeds: std::ops::Range<u64>, generate: fn(&mut Rng) -> Case, expected: Vec<String>) {
    let authority = authority();
    let bundle = |case: &Case| bundle(case, &authority);
    fuzz_loader::run(
        seeds,
        generate,
        &OPTIONS,
        expected,
        bundle,
        |case, verdict, coverage| {
            for option in options_set(case) {
                coverage.reach(option);
            }
            let mode = match (case.dest.get("location"), case.dest.get("unityCatalog")) {
                (Some(_), None) => "location",
                (None, Some(_)) => "unityCatalog",
                _ => "location and unityCatalog both or neither",
            };
            coverage.reach(match verdict {
                Verdict::Shape => "refused by shape".to_string(),
                Verdict::Refused => format!("refused with {mode}"),
                Verdict::Accepted => format!("accepted with {mode}"),
            });
        },
    );
}

#[test]
fn known_delta_configs_get_the_verdict_check_delta_gives() {
    // Every field the rules refuse at, which is every option but the two no value of the right type breaks.
    let refusals = (OPTIONS.iter())
        .filter(|o| !matches!(**o, "unityCatalog" | "compression"))
        .map(|o| format!("refused at {o}"));
    let verdicts = [
        "accepted with location",
        "accepted with unityCatalog",
        "refused with location",
        "refused with unityCatalog",
        "refused with location and unityCatalog both or neither",
    ];
    run(1..1_500, known, refusals.chain(verdicts.map(String::from)).collect());
}

#[test]
fn random_delta_configs_never_panic_and_every_refusal_names_a_field() {
    let verdicts = ["refused by shape", "refused with location", "refused with unityCatalog"];
    run(1..3_000, garbage, verdicts.map(String::from).to_vec());
}
