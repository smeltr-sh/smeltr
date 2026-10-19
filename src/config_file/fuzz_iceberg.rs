//! Random Iceberg destination configs through the config loader.
//!
//! Two generators feed one check. `known` picks every field from a pool whose verdict is written beside each value,
//! so the fields the loader must refuse are known in advance and compared with what it refused. `garbage` puts
//! random text and values of the wrong JSON type in the same fields, where only the loader's conduct is checked.
//!
//! Either way the loader must not panic, must name a field in each refusal, and must build an Iceberg destination's
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
use crate::plan::topology::{CatalogSignIn, DestinationKind, MAX_SENDERS};

/// The destination's id in every generated file.
const TABLE: &str = "table";

/// The identity the generated file declares. Any other name is one it does not.
const DECLARED_IDENTITY: &str = "trust";

/// An address a credential is sent to: the catalog, the tokenUrl, the endpoint and the authEndpoint. Plain http to
/// another host is the admin's to choose: the form warns, and the loader takes it.
struct Address {
    json: &'static str,
    /// A whole http:// or https:// URL with a host.
    web: bool,
    https: bool,
    /// Its host names an AWS region, which a signed catalog call is signed for.
    aws_region: bool,
}

impl Address {
    fn blank(&self) -> bool {
        parse(self.json).as_str().unwrap().trim().is_empty()
    }

    /// Whether a clean case may take it where a whole URL is wanted.
    fn fine(&self) -> bool {
        self.web
    }
}

const fn address(json: &'static str, web: bool, https: bool, aws_region: bool) -> Address {
    Address {
        json,
        web,
        https,
        aws_region,
    }
}

const ADDRESSES: &[Address] = &[
    address(r#""https://catalog.example.com""#, true, true, false),
    address(r#"" https://catalog.example.com/api ""#, true, true, false),
    address(r#""HTTPS://glue.us-east-1.amazonaws.com/iceberg""#, true, true, true),
    address(
        r#""https://s3tables.us-gov-west-1.amazonaws.com/iceberg""#,
        true,
        true,
        true,
    ),
    address(r#""https://amazonaws.com""#, true, true, false),
    address(r#""http://localhost:8181""#, true, false, false),
    address(r#""http://127.0.0.1:8181""#, true, false, false),
    address(r#""http://[::1]:8181""#, true, false, false),
    address(r#""http://catalog.internal:8181""#, true, false, false),
    address(r#""catalog.example.com""#, false, false, false),
    address(r#""ftp://catalog.example.com""#, false, false, false),
    address(r#""https://""#, false, true, false),
    address(r#""""#, false, false, false),
    address(r#""  ""#, false, false, false),
];

const WAREHOUSES: Pool = &[
    (r#""s3://warehouse/""#, false),
    (r#"" analytics ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

/// Levels joined by single dots.
const NAMESPACES: Pool = &[
    (r#""logs""#, false),
    (r#""logs.web""#, false),
    (r#"" logs . web ""#, false),
    (r#""ünï.a b""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#""logs..web""#, true),
    (r#""logs. .web""#, true),
    (r#"".logs""#, true),
    (r#""logs.""#, true),
];

const TABLES: Pool = &[
    (r#""web""#, false),
    (r#"" web ""#, false),
    (r#""Web Logs""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

/// A partition term, whether it is refused, and the partition field it names.
const TERMS: &[(&str, bool, &str)] = &[
    ("host", false, "host"),
    (" host ", false, "host"),
    ("day(ts)", false, "ts_day"),
    ("ts_day", false, "ts_day"),
    ("hour( ts )", false, "ts_hour"),
    ("bucket(16, id)", false, "id_bucket"),
    ("bucket(8,id)", false, "id_bucket"),
    ("truncate(4, name)", false, "name_trunc"),
    ("http.host", false, "http.host"),
    ("year(event.ts)", false, "event.ts_year"),
    ("", true, ""),
    ("  ", true, ""),
    ("day(ts", true, ""),
    ("bucket(0, id)", true, ""),
    ("bucket(id)", true, ""),
    ("day(a, b)", true, ""),
    ("month()", true, ""),
    ("frobnicate(ts)", true, ""),
    ("a..b", true, ""),
];

/// A credential is refused when its name is blank: no worker could look it up.
const SECRETS: Pool = &[
    (r#""ICEBERG_TOKEN""#, false),
    (r#"{"secret": "iceberg", "version": 3}"#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#"{"secret": " ", "version": 1}"#, true),
];

const CLIENT_IDS: Pool = &[
    (r#""smeltr""#, false),
    (r#"" smeltr ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

const SCOPES: Pool = &[
    (r#""catalog""#, false),
    (r#"" PRINCIPAL_ROLE:ALL ""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
];

/// AWS's Iceberg catalogs, by the service their calls are signed for.
const SIGNING_NAMES: Pool = &[
    (r#""glue""#, false),
    (r#""s3tables""#, false),
    (r#"" glue ""#, false),
    (r#""Glue""#, true),
    (r#""dynamodb""#, true),
    (r#""""#, true),
    (r#""  ""#, true),
];

const REGIONS: Pool = &[(r#""us-west-2""#, false), (r#""""#, true), (r#"" ""#, true)];

const ROLES: Pool = &[
    (r#""arn:aws:iam::111122223333:role/smeltr""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
];

const EXTERNAL_IDS: Pool = &[(r#""ext-1""#, false), (r#""""#, true)];

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
    /// Its partitionBy is refused only for two terms naming one partition field.
    duplicate: bool,
}

impl Fuzzed for Case {
    const SECTION: &'static str = "destinations";
    const ID: &'static str = TABLE;
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

/// How a known case signs in to its catalog. A clean case names only the fields of one.
#[derive(Clone, Copy, PartialEq)]
enum SignIn {
    None,
    Bearer,
    Client,
    Aws,
}

/// A destination of known values, and the fields among them the loader must refuse.
fn known(rng: &mut Rng) -> Case {
    let mut dest = json!({ "id": TABLE, "type": "iceberg" });
    let fields = dest.as_object_mut().unwrap();
    let mut refused = BTreeSet::new();
    // A clean case keeps every value inside its own field's rules, and its fields inside the rules between them, so
    // whole configs are accepted often enough to check what the loader builds from them.
    let clean = rng.chance(2);
    let take = |rng: &mut Rng, pool: Pool| -> (Value, bool) {
        let good: Vec<_> = pool.iter().filter(|(_, bad)| !clean || !bad).collect();
        let (json, bad) = rng.pick(&good);
        (parse(json), *bad)
    };
    let fine = |a: &&Address| !clean || a.fine();

    let identity = match rng.below(4) {
        0 => Some(DECLARED_IDENTITY),
        1 if !clean => Some("nobody"),
        _ => None,
    };
    if identity.is_some_and(|i| i != DECLARED_IDENTITY) {
        refused.insert("identity");
    }

    // An identity is offered only over TLS.
    let catalogs: Vec<_> = (ADDRESSES.iter().filter(fine))
        .filter(|a| !clean || identity.is_none() || a.https)
        .collect();
    let catalog = *rng.pick(&catalogs);
    if catalog.blank() || !catalog.web || (identity.is_some() && !catalog.https) {
        refused.insert("catalog");
    }

    let sign_in = *rng.pick(&[SignIn::None, SignIn::Bearer, SignIn::Client, SignIn::Aws]);
    // Present always in a clean case of the sign-in it belongs to, never in another; one time in three otherwise.
    let named = |rng: &mut Rng, of: SignIn| if clean { sign_in == of } else { rng.chance(3) };
    let token = named(rng, SignIn::Bearer).then(|| take(rng, SECRETS));
    let client_id = named(rng, SignIn::Client).then(|| take(rng, CLIENT_IDS));
    let client_secret = named(rng, SignIn::Client).then(|| take(rng, SECRETS));
    let token_url = (named(rng, SignIn::Client) && rng.chance(2)).then(|| {
        let urls: Vec<_> = ADDRESSES.iter().filter(fine).collect();
        *rng.pick(&urls)
    });
    let scope = (named(rng, SignIn::Client) && rng.chance(2)).then(|| take(rng, SCOPES));
    let signing_name = named(rng, SignIn::Aws).then(|| take(rng, SIGNING_NAMES));

    if token.as_ref().is_some_and(|(_, bad)| *bad) || (token.is_some() && client_id.is_some()) {
        refused.insert("token");
    }
    if client_id.as_ref().is_some_and(|(_, bad)| *bad) || (client_id.is_none() && client_secret.is_some()) {
        refused.insert("clientId");
    }
    if client_secret.as_ref().is_some_and(|(_, bad)| *bad) || (client_secret.is_none() && client_id.is_some()) {
        refused.insert("clientSecret");
    }
    if let Some(url) = token_url
        && (url.blank() || !url.web || client_id.is_none())
    {
        refused.insert("tokenUrl");
    }
    if scope.as_ref().is_some_and(|(_, bad)| *bad) || (scope.is_some() && token_url.is_none() && client_id.is_none()) {
        refused.insert("scope");
    }
    if signing_name.as_ref().is_some_and(|(_, bad)| *bad)
        || (signing_name.is_some() && (token.is_some() || client_id.is_some()))
    {
        refused.insert("signingName");
    }

    // A clean case signed for AWS names a region wherever its catalog's address names none.
    let region_needed = clean && signing_name.is_some() && !catalog.aws_region;
    let region = (region_needed || rng.chance(3)).then(|| take(rng, REGIONS));
    if region.as_ref().is_some_and(|(_, bad)| *bad)
        || (region.is_none() && signing_name.is_some() && !catalog.aws_region)
    {
        refused.insert("region");
    }

    let endpoint = rng.chance(3).then(|| {
        let ends: Vec<_> = ADDRESSES.iter().filter(fine).collect();
        *rng.pick(&ends)
    });
    if let Some(a) = endpoint
        && (a.blank() || !a.web)
    {
        refused.insert("endpoint");
    }
    let (key_id, secret_key) = if rng.chance(2) {
        (Some(take(rng, SECRETS)), Some(take(rng, SECRETS)))
    } else if clean {
        (None, None)
    } else {
        (
            rng.chance(3).then(|| take(rng, SECRETS)),
            rng.chance(3).then(|| take(rng, SECRETS)),
        )
    };
    for (name, half, other) in [
        ("accessKeyId", &key_id, &secret_key),
        ("secretAccessKey", &secret_key, &key_id),
    ] {
        if half.as_ref().is_some_and(|(_, bad)| *bad) || (half.is_none() && other.is_some()) {
            refused.insert(name);
        }
    }
    let role = rng.chance(3).then(|| take(rng, ROLES));
    let external_id = (role.is_some() || !clean)
        .then(|| rng.chance(3).then(|| take(rng, EXTERNAL_IDS)))
        .flatten();
    let auth_endpoint = (role.is_some() || !clean)
        .then(|| {
            rng.chance(3).then(|| {
                let ends: Vec<_> = ADDRESSES.iter().filter(fine).collect();
                *rng.pick(&ends)
            })
        })
        .flatten();
    if role.as_ref().is_some_and(|(_, bad)| *bad)
        || (role.is_none() && (external_id.is_some() || auth_endpoint.is_some()))
    {
        refused.insert("roleArn");
    }
    if external_id.as_ref().is_some_and(|(_, bad)| *bad) {
        refused.insert("externalId");
    }
    if let Some(a) = auth_endpoint
        && (a.blank() || !a.web)
    {
        refused.insert("authEndpoint");
    }

    let (warehouse, namespace, table) = (
        rng.chance(2).then(|| take(rng, WAREHOUSES)),
        take(rng, NAMESPACES),
        take(rng, TABLES),
    );
    for (key, value) in [
        ("warehouse", &warehouse),
        ("namespace", &Some(namespace.clone())),
        ("table", &Some(table.clone())),
    ] {
        if value.as_ref().is_some_and(|(_, bad)| *bad) {
            refused.insert(key);
        }
    }

    let sets = [
        ("catalog", Some(parse(catalog.json))),
        ("warehouse", warehouse.map(|(v, _)| v)),
        ("namespace", Some(namespace.0)),
        ("table", Some(table.0)),
        ("token", token.map(|(v, _)| v)),
        ("clientId", client_id.map(|(v, _)| v)),
        ("clientSecret", client_secret.map(|(v, _)| v)),
        ("tokenUrl", token_url.map(|a| parse(a.json))),
        ("scope", scope.map(|(v, _)| v)),
        ("signingName", signing_name.map(|(v, _)| v)),
        ("identity", identity.map(Value::from)),
        ("region", region.map(|(v, _)| v)),
        ("endpoint", endpoint.map(|a| parse(a.json))),
        ("authEndpoint", auth_endpoint.map(|a| parse(a.json))),
        ("accessKeyId", key_id.map(|(v, _)| v)),
        ("secretAccessKey", secret_key.map(|(v, _)| v)),
        ("roleArn", role.map(|(v, _)| v)),
        ("externalId", external_id.map(|(v, _)| v)),
    ];
    for (key, value) in sets {
        if let Some(v) = value {
            fields.insert(key.into(), v);
        }
    }

    let (mut unreadable, mut duplicate) = (false, false);
    if rng.chance(2) {
        let mut terms = Vec::new();
        let mut names = BTreeSet::new();
        for _ in 0..rng.below(4) {
            let picks: Vec<_> = TERMS.iter().filter(|(_, bad, _)| !clean || !bad).collect();
            let (term, bad, name) = **rng.pick(&picks);
            // Two terms naming one partition field are refused.
            if clean && names.contains(name) {
                continue;
            }
            if bad {
                unreadable = true;
            } else if !names.insert(name) {
                duplicate = true;
            }
            terms.push(Value::from(term));
        }
        fields.insert("partitionBy".into(), Value::Array(terms));
    }
    if unreadable || duplicate {
        refused.insert("partitionBy");
    }

    for (key, pool) in [
        ("compression", COMPRESSIONS),
        ("inFlight", IN_FLIGHTS),
        ("maxObjectBytes", OBJECT_BYTES),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
        ("fileIntervalSecs", FILE_INTERVALS),
    ] {
        if rng.chance(2) {
            let (v, bad) = take(rng, pool);
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
        duplicate: duplicate && !unreadable,
    }
}

/// Text cut from what breaks URL, size, name and partition term parsers.
const PIECES: &[&str] = &[
    "https://",
    "http://",
    "s3://",
    "catalog",
    "glue",
    "s3tables",
    ".amazonaws.com",
    "us-east-1",
    "logs",
    "web",
    ".",
    "..",
    "/",
    "?a=1",
    "#",
    "[::1]",
    "@",
    ":",
    ":0",
    ":99999",
    " ",
    "\t",
    "%",
    "localhost",
    "127.0.0.1",
    "\0",
    "é",
    "😀",
    "\u{202e}",
    "1",
    "-",
    "GiB",
    "KB",
    "TiB",
    ",",
    "_",
    "(",
    ")",
    "day",
    "bucket",
    "truncate",
    "99999999999999999999",
    "\\",
    "\"",
];

/// A destination whose fields hold random text and values of any type.
fn garbage(rng: &mut Rng) -> Case {
    let mut dest = json!({ "id": TABLE, "type": "iceberg" });
    let fields = dest.as_object_mut().unwrap();
    let addresses: Vec<&str> = ADDRESSES.iter().map(|a| a.json).collect();
    let address: Pool = &[
        (r#""https://catalog.example.com""#, false),
        (r#""http://localhost:8181""#, false),
    ];
    let declared: Pool = &[(r#""trust""#, false)];
    let terms: Vec<String> = TERMS.iter().map(|(t, ..)| Value::from(*t).to_string()).collect();
    let density = 1 + rng.below(6);
    for (key, pool) in [
        ("catalog", address),
        ("warehouse", WAREHOUSES),
        ("namespace", NAMESPACES),
        ("table", TABLES),
        ("token", SECRETS),
        ("clientId", CLIENT_IDS),
        ("clientSecret", SECRETS),
        ("tokenUrl", address),
        ("scope", SCOPES),
        ("signingName", SIGNING_NAMES),
        ("identity", declared),
        ("region", REGIONS),
        ("endpoint", address),
        ("authEndpoint", address),
        ("accessKeyId", SECRETS),
        ("secretAccessKey", SECRETS),
        ("roleArn", ROLES),
        ("externalId", EXTERNAL_IDS),
        ("compression", COMPRESSIONS),
        ("inFlight", IN_FLIGHTS),
        ("maxObjectBytes", OBJECT_BYTES),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
        ("fileIntervalSecs", FILE_INTERVALS),
    ] {
        // The fields every table names are there most of the time, so the rules past their absence are reached.
        let required = matches!(key, "catalog" | "namespace" | "table");
        if (required && !rng.chance(8)) || rng.chance(3) {
            let value = if pool == address && !rng.chance(density) {
                parse(rng.pick(&addresses))
            } else {
                garbage_value(rng, PIECES, pool, density)
            };
            fields.insert(key.into(), value);
        }
    }
    if rng.chance(2) {
        let terms = (0..rng.below(4))
            .map(|_| {
                if rng.chance(density) {
                    Value::from(random_text(rng, PIECES))
                } else {
                    parse(rng.pick(&terms))
                }
            })
            .collect();
        fields.insert("partitionBy".into(), Value::Array(terms));
    }
    if rng.chance(16) {
        fields.insert(random_text(rng, PIECES), random_value(rng, PIECES));
    }
    Case {
        dest,
        routed: rng.chance(2),
        refused: None,
        duplicate: false,
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
        catch_all["deliver"][0]["to"].as_array_mut().unwrap().push(TABLE.into());
    }
    file.to_string()
}

/// The string a field of the generated destination holds, trimmed.
fn text(case: &Case, key: &str) -> Option<String> {
    case.dest.get(key).and_then(Value::as_str).map(|s| s.trim().to_string())
}

/// What a worker's egress is handed for the accepted destination, held to the rules it was accepted under.
fn built(case: &Case, system: &super::LoadedSystem) -> Result<(), String> {
    let dest = (system.topology.destinations.iter())
        .find(|d| d.name == TABLE)
        .ok_or("the accepted destination is not in the topology")?;
    let DestinationKind::Iceberg(c) = &dest.kind else {
        return Err(format!("built {:?}, not an Iceberg destination", dest.kind));
    };
    // Every name is sent as it is read: a space around one names another warehouse, table or client.
    let names_ok = Some(c.catalog.clone()) == text(case, "catalog")
        && c.warehouse == text(case, "warehouse")
        && Some(c.table.clone()) == text(case, "table")
        && Some(c.namespace.join("."))
            == text(case, "namespace").map(|n| n.split('.').map(str::trim).collect::<Vec<_>>().join("."))
        && c.namespace.iter().all(|l| !l.is_empty() && l.trim() == l);
    if !names_ok {
        return Err(format!(
            "built catalog {:?}, warehouse {:?}, namespace {:?}, table {:?}",
            c.catalog, c.warehouse, c.namespace, c.table
        ));
    }
    let terms: Vec<String> = (case
        .dest
        .get("partitionBy")
        .and_then(Value::as_array)
        .into_iter()
        .flatten())
    .map(|t| t.as_str().unwrap().to_string())
    .collect();
    if c.partition_by != terms {
        return Err(format!("built partitionBy {:?}", c.partition_by));
    }
    let sign_in_ok = match &c.sign_in {
        CatalogSignIn::SigV4 { signing_name } => {
            Some(signing_name.clone()) == text(case, "signingName")
                && ["glue", "s3tables"].contains(&signing_name.as_str())
        }
        CatalogSignIn::Bearer(_) => case.dest.get("token").is_some() && case.dest.get("signingName").is_none(),
        CatalogSignIn::ClientCredentials {
            token_url,
            client_id,
            scope,
            ..
        } => {
            case.dest.get("signingName").is_none()
                && Some(client_id.clone()) == text(case, "clientId")
                && *token_url == text(case, "tokenUrl")
                && *scope == text(case, "scope")
        }
        CatalogSignIn::None => ["token", "clientId", "clientSecret", "signingName"]
            .iter()
            .all(|k| case.dest.get(*k).is_none()),
    };
    if !sign_in_ok {
        return Err(format!("built sign-in {:?}", c.sign_in));
    }
    let access = [
        "region",
        "endpoint",
        "authEndpoint",
        "accessKeyId",
        "secretAccessKey",
        "roleArn",
        "externalId",
    ];
    if c.storage.is_some() != access.iter().any(|k| case.dest.get(*k).is_some()) {
        return Err(format!("built storage {:?}", c.storage));
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
        return Err("a routed Iceberg destination has no Parquet record".to_string());
    }
    Ok(())
}

/// Every option of an Iceberg destination, by its name in the file.
const OPTIONS: [&str; 25] = [
    "catalog",
    "warehouse",
    "namespace",
    "table",
    "partitionBy",
    "token",
    "clientId",
    "clientSecret",
    "tokenUrl",
    "scope",
    "signingName",
    "identity",
    "region",
    "endpoint",
    "authEndpoint",
    "accessKeyId",
    "secretAccessKey",
    "roleArn",
    "externalId",
    "compression",
    "inFlight",
    "maxObjectBytes",
    "bufferBytes",
    "stallAfterSecs",
    "fileIntervalSecs",
];

/// How the accepted or refused destination signs in to its catalog, by the fields it names.
fn sign_in(case: &Case) -> &'static str {
    let has = |k: &str| case.dest.get(k).is_some();
    match (has("signingName"), has("token"), has("clientId") || has("clientSecret")) {
        (true, false, false) => "aws",
        (false, true, false) => "bearer",
        (false, false, true) => "client credentials",
        (false, false, false) => "none",
        _ => "mixed",
    }
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
            for option in case.dest.as_object().unwrap().keys() {
                coverage.reach(option.clone());
            }
            if case.duplicate {
                coverage.reach("refused at partitionBy for a duplicate");
            }
            coverage.reach(match verdict {
                Verdict::Shape => "refused by shape".to_string(),
                Verdict::Refused => format!("refused signing in by {}", sign_in(case)),
                Verdict::Accepted => format!("accepted signing in by {}", sign_in(case)),
            });
        },
    );
}

#[test]
fn known_iceberg_configs_get_the_verdict_check_iceberg_gives() {
    // Every field the rules refuse at, which is every option but compression, whose every value is one.
    let refusals = (OPTIONS.iter())
        .filter(|o| **o != "compression")
        .map(|o| format!("refused at {o}"));
    let verdicts = ["none", "bearer", "client credentials", "aws"]
        .into_iter()
        .flat_map(|s| {
            [
                format!("accepted signing in by {s}"),
                format!("refused signing in by {s}"),
            ]
        })
        .chain(["refused signing in by mixed", "refused at partitionBy for a duplicate"].map(String::from));
    run(1..2_000, known, refusals.chain(verdicts).collect());
}

#[test]
fn random_iceberg_configs_never_panic_and_every_refusal_names_a_field() {
    let verdicts = [
        "refused by shape",
        "refused signing in by none",
        "refused signing in by mixed",
    ];
    run(1..3_000, garbage, verdicts.map(String::from).to_vec());
}
