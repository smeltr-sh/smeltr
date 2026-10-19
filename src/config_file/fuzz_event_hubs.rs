//! Random Event Hubs destination configs through the config loader, each signed in one of the ways it can be.
//!
//! Two generators feed one check. `known` picks every field from a pool whose verdict is written beside each value,
//! so the fields the loader must refuse are known in advance and compared with what it refused. `garbage` puts
//! random text and values of the wrong JSON type in the same fields, where only the loader's conduct is checked.
//!
//! Either way the loader must not panic, must name a field in each refusal, and must build, from each file it
//! accepts, the Kafka destination an Event Hubs destination runs as. Nothing here reaches a network.
//!
//! Seeded through `crate::fuzz`: a failure names its seed and the destination it generated, and `Coverage` fails
//! the run when an option, or a refusal at it, was never reached.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::dto::{BufferBytes, StallAfter};
use super::fuzz_loader::{self, Fuzzed, Pool, Verdict, garbage_value, parse, random_text, random_value};
use super::test_fixtures::example_json;
use crate::fuzz::Rng;
use crate::plan::config::Payload;
use crate::plan::topology::{Compression, DestinationKind, KafkaService, SecretRef};

/// The destination's id in every generated file.
const HUB: &str = "hub";

/// A namespace is a name, a host name or `host:port`, with no scheme, path or list.
const NAMESPACES: Pool = &[
    (r#""acme""#, false),
    (r#"" acme ""#, false),
    (r#""acme.servicebus.usgovcloudapi.net""#, false),
    (r#""acme.servicebus.chinacloudapi.cn""#, false),
    (r#""eh.internal:9093""#, false),
    (r#""localhost:9092""#, false),
    (r#""127.0.0.1:9092""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#""sb://acme.servicebus.windows.net/""#, true),
    (r#""https://acme.servicebus.windows.net""#, true),
    (r#""acme/logs""#, true),
    (r#""a.servicebus.windows.net,b.servicebus.windows.net""#, true),
];

const EVENT_HUBS: Pool = &[
    (r#""alerts""#, false),
    (r#""insights-logs-signinlogs""#, false),
    (r#"" alerts ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

/// A credential is refused when its name is blank: no worker could look it up.
const SECRETS: Pool = &[
    (r#""EVENTHUBS_CONNECTION_STRING""#, false),
    (r#"{"secret": "eventhubs", "version": 3}"#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#"{"secret": " ", "version": 1}"#, true),
];

/// A service principal's tenant or client id is refused when blank.
const PRINCIPAL_IDS: Pool = &[
    (r#""c-1""#, false),
    (r#""72f988bf-86f1-41af-91ab-2d7cd011db47""#, false),
    (r#"" 72f988bf-86f1-41af-91ab-2d7cd011db47 ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
];

/// A user-assigned managed identity's client id is refused when blank or not a GUID, which the metadata service's
/// query carries.
const MANAGED_IDENTITY_IDS: Pool = &[
    (r#""72f988bf-86f1-41af-91ab-2d7cd011db47""#, false),
    (r#"" 72F988BF-86F1-41AF-91AB-2D7CD011DB47 ""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#""writer""#, true),
    (r#""c&resource=https://other""#, true),
];

/// Where a client secret is exchanged for a token: a whole URL naming no path.
const AUTH_ENDPOINTS: Pool = &[
    (r#""https://login.microsoftonline.us""#, false),
    (r#""https://login.microsoftonline.us/""#, false),
    (r#""http://localhost:8080""#, false),
    (r#""""#, true),
    (r#""  ""#, true),
    (r#""login.microsoftonline.us""#, true),
    (r#""http://login.example""#, false),
    (r#""https://login.microsoftonline.com/t-1""#, true),
    (r#""https://login.microsoftonline.us?x=1""#, true),
];

const FORMATS: Pool = &[(r#""json""#, false), (r#""text""#, false), (r#""bytes""#, false)];

/// Refused when blank, or beside the `json` format, which reads no field.
const FIELDS: Pool = &[
    (r#""message""#, false),
    (r#""_raw""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
];

/// Event Hubs takes gzip, and no other codec.
const COMPRESSIONS: Pool = &[
    (r#""none""#, false),
    (r#""gzip""#, false),
    (r#""snappy""#, true),
    (r#""lz4""#, true),
    (r#""zstd""#, true),
];

const BUFFER_BYTES: Pool = &[
    ("4194304", false),
    (r#""64 MiB""#, false),
    (r#""1 GiB""#, false),
    ("4194303", true),
    ("0", true),
    (r#""2 GiB""#, true),
];

const STALL_AFTERS: Pool = &[("1", false), ("30", false), ("120", false), ("0", true), ("121", true)];

/// One generated destination, and what the loader must make of it.
struct Case {
    dest: Value,
    /// Whether it is sent rows, so compiling it checks what its payload reads.
    routed: bool,
    /// The fields the loader must refuse, when `known` generated it.
    refused: Option<BTreeSet<&'static str>>,
}

impl Fuzzed for Case {
    const SECTION: &'static str = "destinations";
    const ID: &'static str = HUB;
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
    let mut dest = json!({ "id": HUB, "type": "eventHubs" });
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

    for (key, pool) in [("namespace", NAMESPACES), ("defaultEventHub", EVENT_HUBS)] {
        let (v, bad) = take(rng, pool);
        fields.insert(key.into(), v);
        if bad {
            refused.insert(key);
        }
    }
    sign_in(rng, clean, fields, &mut refused);

    let format = rng.chance(2).then(|| take(rng, FORMATS).0);
    let reads_field = format.as_ref().is_some_and(|f| f != "json");
    if let Some(f) = format {
        fields.insert("format".into(), f);
    }
    // A field beside `json` is refused, so a clean case names one only where it is read.
    if rng.chance(2) && (reads_field || !clean) {
        let (v, bad) = take(rng, FIELDS);
        fields.insert("field".into(), v);
        if bad || !reads_field {
            refused.insert("field");
        }
    }

    for (key, pool) in [
        ("compression", COMPRESSIONS),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
    ] {
        if rng.chance(2) {
            let (v, bad) = take(rng, pool);
            fields.insert(key.into(), v);
            if bad {
                refused.insert(key);
            }
        }
    }

    // Routing a `bytes` destination asks the bundle's sources to declare its field, which is no field of this one.
    let routed = rng.chance(2) && !reads_bytes(&dest);
    Case {
        dest,
        routed,
        refused: Some(refused),
    }
}

/// How the destination signs in: a connection string, a service principal, one of the host's managed identities by
/// its client id, or none, as the host's own. An unclean case may name part of a principal, two credentials, or an
/// authEndpoint with no secret.
fn sign_in(
    rng: &mut Rng,
    clean: bool,
    fields: &mut serde_json::Map<String, Value>,
    refused: &mut BTreeSet<&'static str>,
) {
    let take = |rng: &mut Rng,
                fields: &mut serde_json::Map<String, Value>,
                refused: &mut BTreeSet<&'static str>,
                key: &'static str,
                pool: Pool| {
        let good: Vec<_> = pool.iter().filter(|(_, bad)| !clean || !bad).collect();
        let (json, bad) = rng.pick(&good);
        fields.insert(key.into(), parse(json));
        if *bad {
            refused.insert(key);
        }
    };
    let principal = match rng.below(4) {
        0 => {
            take(rng, fields, refused, "connectionString", SECRETS);
            false
        }
        1 => {
            take(rng, fields, refused, "tenantId", PRINCIPAL_IDS);
            take(rng, fields, refused, "clientId", PRINCIPAL_IDS);
            take(rng, fields, refused, "clientSecret", SECRETS);
            if rng.chance(2) {
                take(rng, fields, refused, "authEndpoint", AUTH_ENDPOINTS);
            }
            true
        }
        2 => {
            take(rng, fields, refused, "managedIdentityClientId", MANAGED_IDENTITY_IDS);
            false
        }
        _ => false,
    };
    if clean {
        return;
    }
    if !fields.contains_key("managedIdentityClientId") && rng.chance(4) {
        take(rng, fields, refused, "managedIdentityClientId", MANAGED_IDENTITY_IDS);
        if principal || fields.contains_key("connectionString") {
            refused.insert("managedIdentityClientId");
        }
    }
    if principal && rng.chance(3) {
        let missing = *rng.pick(&["tenantId", "clientId", "clientSecret"]);
        fields.remove(missing);
        refused.insert(missing);
    }
    if principal && !fields.contains_key("managedIdentityClientId") && rng.chance(3) {
        take(rng, fields, refused, "connectionString", SECRETS);
        refused.insert("connectionString");
    }
    if !principal && rng.chance(3) {
        take(rng, fields, refused, "authEndpoint", AUTH_ENDPOINTS);
        refused.insert("clientSecret");
    }
    if fields.contains_key("authEndpoint") && !fields.contains_key("clientSecret") {
        refused.insert("clientSecret");
    }
}

/// Text cut from what breaks host, size and name parsers.
const PIECES: &[&str] = &[
    "sb://",
    "https://",
    "http://",
    "login.microsoftonline.us",
    "acme",
    ".servicebus.windows.net",
    ".",
    "..",
    "/",
    ":",
    ":0",
    ":9093",
    ":99999",
    ",",
    "@",
    "[::1]",
    " ",
    "\t",
    "%",
    "localhost",
    "\0",
    "é",
    "😀",
    "\u{202e}",
    "1",
    "-",
    "MiB",
    "GiB",
    "KB",
    "99999999999999999999",
    "\\",
    "\"",
    "$ConnectionString",
    "Endpoint=sb://",
    ";",
    "=",
];

/// A destination whose fields hold random text and values of any type.
fn garbage(rng: &mut Rng) -> Case {
    let mut dest = json!({ "id": HUB, "type": "eventHubs" });
    let fields = dest.as_object_mut().unwrap();
    let density = 1 + rng.below(6);
    for (key, pool) in [
        ("namespace", NAMESPACES),
        ("defaultEventHub", EVENT_HUBS),
        ("format", FORMATS),
        ("field", FIELDS),
        ("compression", COMPRESSIONS),
        ("bufferBytes", BUFFER_BYTES),
        ("stallAfterSecs", STALL_AFTERS),
    ] {
        if !rng.chance(8) {
            fields.insert(key.into(), garbage_value(rng, PIECES, pool, density));
        }
    }
    // Mostly one way of signing in, so a file is accepted often enough; now and then a field of another.
    let principal = rng.chance(2);
    for (key, pool, chosen) in [
        ("connectionString", SECRETS, !principal),
        ("tenantId", PRINCIPAL_IDS, principal),
        ("clientId", PRINCIPAL_IDS, principal),
        ("clientSecret", SECRETS, principal),
        ("authEndpoint", AUTH_ENDPOINTS, principal),
        ("managedIdentityClientId", MANAGED_IDENTITY_IDS, false),
    ] {
        if if chosen { !rng.chance(8) } else { rng.chance(16) } {
            fields.insert(key.into(), garbage_value(rng, PIECES, pool, density));
        }
    }
    if rng.chance(16) {
        fields.insert(random_text(rng, PIECES), random_value(rng, PIECES));
    }
    let routed = rng.chance(2) && !reads_bytes(&dest);
    Case {
        dest,
        routed,
        refused: None,
    }
}

/// Whether `dest` writes a `bytes` field of each source record.
fn reads_bytes(dest: &Value) -> bool {
    dest.get("format")
        .and_then(Value::as_str)
        .is_some_and(|f| f.trim() == "bytes")
}

/// The example bundle with `case`'s destination in it and, when it is routed, a route delivering to it.
fn bundle(case: &Case) -> String {
    let mut file = parse(example_json());
    file["destinations"].as_array_mut().unwrap().push(case.dest.clone());
    if case.routed {
        let catch_all = file["routes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|r| r["id"] == "catch-all")
            .unwrap();
        catch_all["deliver"][0]["to"].as_array_mut().unwrap().push(HUB.into());
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
        .find(|d| d.name == HUB)
        .ok_or("the accepted destination is not in the topology")?;
    let DestinationKind::Kafka(k) = &dest.kind else {
        return Err(format!("built {:?}, not a Kafka destination", dest.kind));
    };
    if k.service != KafkaService::EventHubs || k.identity.is_some() {
        return Err(format!("built service {:?}, identity {:?}", k.service, k.identity));
    }
    let namespace = text(case, "namespace").unwrap_or_default();
    let expected_brokers = super::load::event_hubs_brokers(&namespace);
    if k.brokers != expected_brokers {
        return Err(format!("namespace {namespace:?} built brokers {:?}", k.brokers));
    }
    // A credential crosses the network only inside TLS; the emulator, on this host, has none.
    let literal = |key: &str| k.props.literal.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
    let protocol = literal("security.protocol");
    let loopback = k.brokers.split(',').all(super::load::on_loopback);
    let expected_protocol = if loopback { "SASL_PLAINTEXT" } else { "SASL_SSL" };
    if protocol != Some(expected_protocol) {
        return Err(format!("brokers {:?} built protocol {protocol:?}", k.brokers));
    }
    // The credential the destination names is the one it signs in with; naming none is the host's identity.
    let (mechanism, secret) = if case.dest.get("connectionString").is_some() {
        ("PLAIN", Some("sasl.password"))
    } else if case.dest.get("clientSecret").is_some() {
        ("OAUTHBEARER", Some("sasl.oauthbearer.client.secret"))
    } else {
        ("OAUTHBEARER", None)
    };
    if literal("sasl.mechanism") != Some(mechanism) {
        return Err(format!(
            "built mechanism {:?}, not {mechanism}",
            literal("sasl.mechanism")
        ));
    }
    match (k.props.credentials.as_slice(), secret) {
        ([(key, SecretRef::WorkerLocal(name) | SecretRef::LeaderStored { name, .. })], Some(secret))
            if key == secret && !name.trim().is_empty() => {}
        ([], None) if literal("sasl.oauthbearer.metadata.authentication.type") == Some("azure_imds") => {}
        (other, _) => return Err(format!("built credentials {other:?}")),
    }
    // A managed identity named by its client id is the one the metadata service is asked for, and only then.
    let asked =
        literal("sasl.oauthbearer.config").and_then(|c| c.split('&').find_map(|pair| pair.strip_prefix("client_id=")));
    if asked.map(str::to_string) != text(case, "managedIdentityClientId").filter(|_| secret.is_none()) {
        return Err(format!("built the managed identity {asked:?}"));
    }
    // An Entra token is for the namespace, and a principal's comes from its tenant.
    if mechanism == "OAUTHBEARER" {
        let host = match k.brokers.rsplit_once(':') {
            Some((h, port)) if !port.ends_with(']') => h,
            _ => k.brokers.as_str(),
        };
        let audience = literal("sasl.oauthbearer.scope").or(literal("sasl.oauthbearer.config"));
        if !audience.is_some_and(|a| a.contains(&format!("https://{host}"))) {
            return Err(format!("built audience {audience:?} for brokers {:?}", k.brokers));
        }
        if let Some(url) = literal("sasl.oauthbearer.token.endpoint.url") {
            let tenant = text(case, "tenantId").unwrap_or_default();
            if !url.ends_with(&format!("/{tenant}/oauth2/v2.0/token")) {
                return Err(format!("built token endpoint {url:?}"));
            }
            // Named or not, the sign-in is at an authority: unnamed, the one in the namespace's cloud.
            let authority = text(case, "authEndpoint").map(|a| a.trim_end_matches('/').to_string());
            let authority = authority.unwrap_or_else(|| crate::io::entra_authority(host).to_string());
            if !url.starts_with(&format!("{authority}/")) {
                return Err(format!("built token endpoint {url:?}, not at {authority}"));
            }
        }
    }
    if Some(k.default_topic.clone())
        != case
            .dest
            .get("defaultEventHub")
            .and_then(Value::as_str)
            .map(String::from)
    {
        return Err(format!("built default topic {:?}", k.default_topic));
    }
    if !matches!(k.compression, Compression::None | Compression::Gzip) {
        return Err(format!("built compression {:?}", k.compression));
    }
    let format = text(case, "format");
    let payload_ok = matches!(
        (format.as_deref(), &k.payload),
        (None | Some("json"), Payload::Record)
            | (Some("text"), Payload::Text { .. })
            | (Some("bytes"), Payload::Bytes { .. })
    );
    if !payload_ok {
        return Err(format!("format {format:?} built payload {:?}", k.payload));
    }
    let in_range = (BufferBytes::MIN..=BufferBytes::MAX).contains(&k.buffer_bytes)
        && (StallAfter::MIN..=StallAfter::MAX).contains(&k.stall_after.as_secs());
    if !in_range {
        return Err(format!("built limits out of range: {k:?}"));
    }
    // What egress and the worker's memory budget read from the kind.
    let _ = (dest.kind.payload(), dest.kind.id_field());
    if dest.kind.stall_after() != k.stall_after || dest.kind.buffer_bytes() != k.buffer_bytes {
        return Err("the kind reports other limits than its config".to_string());
    }
    system
        .compile_topology()
        .map_err(|e| format!("the accepted bundle does not compile: {e:?}"))?;
    Ok(())
}

/// Every option of an Event Hubs destination, by its name in the file.
const OPTIONS: [&str; 13] = [
    "namespace",
    "defaultEventHub",
    "connectionString",
    "tenantId",
    "clientId",
    "clientSecret",
    "authEndpoint",
    "managedIdentityClientId",
    "format",
    "field",
    "compression",
    "bufferBytes",
    "stallAfterSecs",
];

/// Runs `generate` over the seeds (`fuzz_loader::run`), marking what each case reaches besides its refusals.
fn run(seeds: std::ops::Range<u64>, generate: fn(&mut Rng) -> Case, expected: Vec<String>) {
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
            coverage.reach(match verdict {
                Verdict::Shape => "refused by shape",
                Verdict::Refused => "refused",
                Verdict::Accepted => "accepted",
            });
        },
    );
}

#[test]
fn known_event_hubs_configs_get_the_verdict_the_rules_give() {
    // Every field the rules refuse at, which is every option but `format`, whose every value is one.
    let refusals = (OPTIONS.iter())
        .filter(|o| **o != "format")
        .map(|o| format!("refused at {o}"));
    let verdicts = ["accepted", "refused"];
    run(1..3_000, known, refusals.chain(verdicts.map(String::from)).collect());
}

#[test]
fn random_event_hubs_configs_never_panic_and_every_refusal_names_a_field() {
    let verdicts = ["refused by shape", "refused", "accepted"];
    run(1..3_000, garbage, verdicts.map(String::from).to_vec());
}
