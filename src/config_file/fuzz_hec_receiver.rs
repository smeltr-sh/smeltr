//! Random HEC receiver configs through the config loader.
//!
//! Two generators feed one check. `known` picks every field from a pool whose verdict is written beside each value,
//! so the fields the loader must refuse are known in advance and compared with what it refused. `garbage` puts
//! random text and values of the wrong JSON type in the same fields, where only the loader's conduct is checked.
//!
//! Either way the loader must not panic, must name a field in each refusal, and must build a HEC receiver's runtime
//! config from each file it accepts. Where that receiver presents a certificate, the worker's own check must refuse
//! each one it could not serve. Nothing here opens a socket.
//!
//! Seeded through `crate::fuzz`: a failure names its seed and the source it generated, and `Coverage` fails the run
//! when an option, or a refusal at it, was never reached.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use super::dto::MaxRequestBytes;
use super::fuzz_loader::{self, Fuzzed, Pool, Verdict, garbage_value, parse, random_text, random_value};
use super::test_fixtures::example_json;
use crate::fuzz::Rng;
use crate::heartbeat::SecretDelivery;
use crate::io::line_breaker::PER_LINE;
use crate::plan::topology::{HeldSecrets, Receiver, SecretRef, SourceKind, SourceMeta, Transport};

/// The source's id in every generated file.
const HEC: &str = "hec";

const PORTS: Pool = &[("8088", false), ("1", false), ("65535", false), ("0", true)];

const MAX_REQUEST_BYTES: Pool = &[
    ("2048", false),
    ("10485760", false),
    ("67108864", false),
    ("0", true),
    ("2047", true),
    ("67108865", true),
];

/// A token's name, and a sourcetype's.
const NAMES: Pool = &[
    (r#""ci""#, false),
    (r#""fluent bit""#, false),
    (r#""é""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
];

const SECRETS: Pool = &[
    (r#"{"secret": "hook", "version": 1}"#, false),
    (r#""HEC_TOKEN""#, false),
    (r#"{"secret": " ", "version": 1}"#, true),
    (r#""""#, true),
];

const LINE_BREAKERS: Pool = &[
    (r#""([\\r\\n]+)""#, false),
    (r#""</e>(\\s*)""#, false),
    (r#""([\\r\\n]+)\\d{4}-""#, false),
    (r#""""#, true),
    (r#"" ""#, true),
    (r#""(""#, true),
    (r#""</e>""#, true),
    (r#""(a*)""#, true),
];

/// A sender a receiver admits by its certificate's fingerprint.
const FINGERPRINTS: Pool = &[
    (
        r#""sha-256:00:07:0E:15:1C:23:2A:31:38:3F:46:4D:54:5B:62:69:70:77:7E:85:8C:93:9A:A1:A8:AF:B6:BD:C4:CB:D2:D9""#,
        false,
    ),
    // Lowercase, as some tools print one.
    (
        r#""sha-1:00:0b:16:21:2c:37:42:4d:58:63:6e:79:84:8f:9a:a5:b0:bb:c6:d1""#,
        false,
    ),
    (r#""00:0b:16:21:2c:37:42:4d:58:63:6e:79:84:8f:9a:a5:b0:bb:c6:d1""#, true),
    (r#""md5:00:0b:16:21:2c:37:42:4d:58:63:6e:79:84:8f:9a:a5""#, true),
    (
        r#""sha-256:00:0b:16:21:2c:37:42:4d:58:63:6e:79:84:8f:9a:a5:b0:bb:c6:d1""#,
        true,
    ),
    (
        r#""sha-1:+0:0b:16:21:2c:37:42:4d:58:63:6e:79:84:8f:9a:a5:b0:bb:c6:d1""#,
        true,
    ),
    (r#""""#, true),
];

/// A host a receiver admits senders issued to.
const SENDER_NAMES: Pool = &[
    (r#""host.example.com""#, false),
    (r#""*.example.com""#, false),
    (r#"" syslog-1 ""#, false),
    (r#""*.""#, true),
    (r#""a b""#, true),
    (r#""""#, true),
];

/// The TLS settings a generated receiver may set, and so what serving TLS under them comes to.
struct Serving {
    /// It names a certificate to present.
    serves: bool,
    /// It names authorities, which admit a sender whose certificate chains to them.
    vouches: bool,
}

const SERVINGS: &[Serving] = &[
    Serving {
        serves: true,
        vouches: false,
    },
    Serving {
        serves: true,
        vouches: true,
    },
    // Authorities alone: no certificate to present.
    Serving {
        serves: false,
        vouches: true,
    },
];

/// Stands for the authority's PEM in a generated source, which `bundle` fills in: a case is generated from its seed
/// alone.
const AUTHORITY: &str = "@authority";

/// What a worker may hold for the certificate a receiver names, and whether it refuses to present it.
const HELD: [(&str, bool); 4] = [
    ("a certificate for serving", false),
    // Extended Key Usage of client authentication alone.
    ("a client-only certificate", true),
    // Key Usage that leaves out the digital signature a handshake is signed with.
    ("a certificate that cannot sign", true),
    // A stored secret that is not a certificate entry.
    ("a value with no certificate", true),
];

/// The chain beside each of [`HELD`], issued once for every seed.
static CHAINS: std::sync::LazyLock<[Option<String>; 4]> = std::sync::LazyLock::new(|| {
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};

    let issue = |edit: fn(&mut CertificateParams)| {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        edit(&mut params);
        Some(params.self_signed(&key).unwrap().pem())
    };
    [
        issue(|_| {}),
        issue(|p| p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth]),
        issue(|p| p.key_usages = vec![KeyUsagePurpose::KeyEncipherment]),
        None,
    ]
});

/// One generated source, and what the loader must make of it.
struct Case {
    source: Value,
    /// The fields the loader must refuse, when `known` generated it.
    refused: Option<BTreeSet<&'static str>>,
    /// What the worker holds for the certificate it names, of [`HELD`].
    held: usize,
}

impl Fuzzed for Case {
    const SECTION: &'static str = "sources";
    const ID: &'static str = HEC;
    const NOUN: &'static str = "source";

    fn item(&self) -> &Value {
        &self.source
    }

    fn refused(&self) -> Option<&BTreeSet<&'static str>> {
        self.refused.as_ref()
    }

    fn built(&self, system: &super::LoadedSystem) -> Result<(), String> {
        built(self, system)
    }
}

/// A source of known values, and the fields among them the loader must refuse.
fn known(rng: &mut Rng) -> Case {
    let mut source = json!({ "id": HEC, "type": "hecReceiver" });
    let fields = source.as_object_mut().unwrap();
    let mut refused = BTreeSet::new();
    // A clean case keeps every value inside its field's rules, so whole configs are accepted often enough to check
    // what the loader builds from them.
    let clean = rng.chance(2);
    let take = |rng: &mut Rng, pool: Pool| -> (Value, bool) {
        let good: Vec<_> = pool.iter().filter(|(_, bad)| !clean || !bad).collect();
        let (json, bad) = rng.pick(&good);
        (parse(json), *bad)
    };

    let (port, bad) = take(rng, PORTS);
    fields.insert("port".into(), port);
    if bad {
        refused.insert("port");
    }
    if rng.chance(2) {
        let (v, bad) = take(rng, MAX_REQUEST_BYTES);
        fields.insert("maxRequestBytes".into(), v);
        if bad {
            refused.insert("maxRequestBytes");
        }
    }

    let serving = match rng.below(3) {
        0 => None,
        1 if !clean && rng.chance(4) => Some(Err(" ")),
        _ => {
            let fit: Vec<_> = SERVINGS.iter().filter(|s| !clean || s.serves).collect();
            Some(Ok(*rng.pick(&fit)))
        }
    };
    let vouches = match serving {
        Some(Ok(serving)) => {
            if serving.serves {
                fields.insert("certificate".into(), "receiver".into());
            } else {
                refused.insert("certificate");
            }
            if serving.vouches && !clean && rng.chance(4) {
                fields.insert("trustedAuthorities".into(), "not a certificate".into());
                refused.insert("trustedAuthorities");
            } else if serving.vouches {
                fields.insert("trustedAuthorities".into(), AUTHORITY.into());
            }
            serving.vouches
        }
        Some(Err(blank)) => {
            fields.insert("certificate".into(), blank.into());
            refused.insert("certificate");
            false
        }
        None => false,
    };

    // Senders named only where the receiver serves TLS, as naming them is what makes it serve TLS.
    let mut pinned = false;
    if matches!(serving, Some(Ok(_))) {
        for (key, pool) in [("senderFingerprints", FINGERPRINTS), ("senderNames", SENDER_NAMES)] {
            if !rng.chance(3) {
                continue;
            }
            let listed: Vec<_> = (0..1 + rng.below(2)).map(|_| take(rng, pool)).collect();
            if listed.iter().any(|(_, bad)| *bad) {
                refused.insert(key);
            }
            fields.insert(key.into(), Value::Array(listed.into_iter().map(|(v, _)| v).collect()));
            pinned |= key == "senderFingerprints";
        }
        // Names narrow what authorities vouch for, so without them none is admitted.
        if fields.contains_key("senderNames") && !vouches {
            if clean {
                fields.remove("senderNames");
            } else {
                refused.insert("senderNames");
            }
        }
    }
    let vouches = vouches || pinned;

    // A clean case admits someone: a token, or a sender its authorities vouch for or it names by fingerprint.
    let least = usize::from(clean && !vouches);
    let mut tokens = serde_json::Map::new();
    for _ in 0..least + rng.below(3) {
        let (name, _) = take(rng, NAMES);
        let (secret, _) = take(rng, SECRETS);
        let mut token = json!({ "token": secret });
        if rng.chance(2) {
            token["acknowledgement"] = Value::Bool(rng.chance(2));
        }
        // A name taken twice is one token, the last of them, so what is refused is read off what is left.
        tokens.insert(name.as_str().unwrap().to_string(), token);
    }
    let blank_secret = |t: &Value| match &t["token"] {
        Value::String(s) => s.trim().is_empty(),
        named => named["secret"].as_str().unwrap().trim().is_empty(),
    };
    if (tokens.is_empty() && !vouches) || tokens.iter().any(|(n, t)| n.trim().is_empty() || blank_secret(t)) {
        refused.insert("tokens");
    }
    if !tokens.is_empty() || rng.chance(3) {
        fields.insert("tokens".into(), Value::Object(tokens));
    }

    if rng.chance(2) {
        let mut sourcetypes = serde_json::Map::new();
        for _ in 0..rng.below(4) {
            let (name, _) = take(rng, NAMES);
            let line_breaker = rng.chance(4).then(|| take(rng, LINE_BREAKERS));
            let sourcetype = match &line_breaker {
                Some((pattern, _)) => json!({ "lineBreaker": pattern }),
                None => json!({}),
            };
            sourcetypes.insert(name.as_str().unwrap().to_string(), sourcetype);
        }
        let bad_pattern = |t: &Value| {
            (t.get("lineBreaker")).is_some_and(|p| LINE_BREAKERS.iter().any(|(json, bad)| *bad && parse(json) == *p))
        };
        if sourcetypes.iter().any(|(n, t)| n.trim().is_empty() || bad_pattern(t)) {
            refused.insert("sourcetypes");
        }
        fields.insert("sourcetypes".into(), Value::Object(sourcetypes));
    }

    Case {
        source,
        refused: Some(refused),
        held: rng.below(HELD.len()),
    }
}

/// Text cut from what breaks address, size, name and pattern parsers.
const PIECES: &[&str] = &[
    "127.0.0.1",
    "::",
    "::1",
    "[",
    "]",
    "0",
    "8088",
    "99999",
    ":",
    ".",
    " ",
    "\t",
    "\n",
    "\0",
    "é",
    "😀",
    "\u{202e}",
    "(",
    ")",
    "*",
    "\\",
    "\\s",
    "\\d{4}",
    "[\\r\\n]+",
    "+",
    "?",
    "|",
    "{",
    "}",
    "</e>",
    "\"",
    "hook",
    "-",
    "_",
    "99999999999999999999",
];

/// A source whose fields hold random text and values of any type.
fn garbage(rng: &mut Rng) -> Case {
    let mut source = json!({ "id": HEC, "type": "hecReceiver" });
    let fields = source.as_object_mut().unwrap();
    let density = 1 + rng.below(6);
    for (key, pool) in [("port", PORTS), ("maxRequestBytes", MAX_REQUEST_BYTES)] {
        // A port is there most of the time, so the rules past its absence are reached.
        if (key == "port" && !rng.chance(8)) || rng.chance(3) {
            fields.insert(key.into(), garbage_value(rng, PIECES, pool, density));
        }
    }
    for (key, sound) in [("certificate", "receiver"), ("trustedAuthorities", AUTHORITY)] {
        if rng.chance(2) {
            let value = match rng.chance(density) {
                true => random_value(rng, PIECES),
                false => sound.into(),
            };
            fields.insert(key.into(), value);
        }
    }
    for (key, pool) in [("senderFingerprints", FINGERPRINTS), ("senderNames", SENDER_NAMES)] {
        if rng.chance(3) {
            let value = match rng.chance(density) {
                true => random_value(rng, PIECES),
                false => Value::Array(
                    (0..rng.below(3))
                        .map(|_| garbage_value(rng, PIECES, pool, density))
                        .collect(),
                ),
            };
            fields.insert(key.into(), value);
        }
    }
    let name = |rng: &mut Rng| match rng.chance(density) {
        true => random_text(rng, PIECES),
        false => parse(rng.pick(NAMES).0).as_str().unwrap().to_string(),
    };
    // Tokens most of the time, so the rules past their absence are reached.
    if !rng.chance(4) {
        let mut tokens = serde_json::Map::new();
        for _ in 0..rng.below(4) {
            let mut token = json!({ "token": garbage_value(rng, PIECES, SECRETS, density) });
            if rng.chance(2) {
                token["acknowledgement"] = match rng.chance(density) {
                    true => random_value(rng, PIECES),
                    false => Value::Bool(rng.chance(2)),
                };
            }
            if rng.chance(16) {
                token[random_text(rng, PIECES)] = random_value(rng, PIECES);
            }
            tokens.insert(name(rng), token);
        }
        let tokens = match rng.chance(20) {
            true => random_value(rng, PIECES),
            false => Value::Object(tokens),
        };
        fields.insert("tokens".into(), tokens);
    }
    if rng.chance(2) {
        let mut sourcetypes = serde_json::Map::new();
        for _ in 0..rng.below(4) {
            let mut sourcetype = json!({});
            if rng.chance(2) {
                sourcetype["lineBreaker"] = garbage_value(rng, PIECES, LINE_BREAKERS, density);
            }
            if rng.chance(16) {
                sourcetype[random_text(rng, PIECES)] = random_value(rng, PIECES);
            }
            sourcetypes.insert(name(rng), sourcetype);
        }
        fields.insert("sourcetypes".into(), Value::Object(sourcetypes));
    }
    if rng.chance(16) {
        fields.insert(random_text(rng, PIECES), random_value(rng, PIECES));
    }
    Case {
        source,
        refused: None,
        held: rng.below(HELD.len()),
    }
}

/// PEM for an authority to trust. A certificate a receiver presents is a list entry the bundle names, so the
/// loader reads no PEM for it.
struct Pems {
    authority: String,
}

impl Pems {
    fn new() -> Pems {
        Pems {
            authority: rcgen::generate_simple_self_signed(vec!["smeltr-test".to_string()])
                .unwrap()
                .cert
                .pem(),
        }
    }
}

/// The example bundle with `case`'s source in it, its authorities filled in where it stands for them.
fn bundle(case: &Case, pems: &Pems) -> String {
    let mut file = parse(example_json());
    let mut source = case.source.clone();
    if source.get("trustedAuthorities").and_then(Value::as_str) == Some(AUTHORITY) {
        source["trustedAuthorities"] = pems.authority.clone().into();
    }
    file["sources"].as_array_mut().unwrap().push(source);
    file.to_string()
}

/// The credential a token's JSON names, as a worker looks it up.
fn secret(token: &Value) -> SecretRef {
    match &token["token"] {
        Value::String(var) => SecretRef::WorkerLocal(var.clone()),
        stored => SecretRef::LeaderStored {
            name: stored["secret"].as_str().unwrap().to_string(),
            version: stored["version"].as_i64().unwrap(),
        },
    }
}

/// What a worker opens for the accepted receiver, held to the rules it was accepted under.
fn built(case: &Case, system: &super::LoadedSystem) -> Result<(), String> {
    let source = (system.topology.sources.iter())
        .find(|s| s.name == HEC)
        .ok_or("the accepted receiver is not in the topology")?;
    let SourceKind::Receiver(r) = &source.kind else {
        return Err(format!("built {:?}, not a receiver", source.kind));
    };
    let Receiver::Hec(hec) = &r.receiver else {
        return Err(format!("built {:?}, not a HEC receiver", r.receiver));
    };
    let field = |key: &str| case.source.get(key);

    // Every address, of either family, until the worker that runs it names its own.
    let port = field("port").and_then(Value::as_u64).unwrap();
    let listen = std::net::SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), port as u16);
    let listens = source.kind.listens().map(|l| (l.transport, l.address));
    if r.listen != listen || listens != Some((Transport::Tcp, listen)) {
        return Err(format!("built listen {}, listens {listens:?}, not {listen}", r.listen));
    }

    let tokens = (field("tokens").and_then(Value::as_object).into_iter().flatten()).collect::<Vec<_>>();
    let expected: Vec<(String, SecretRef)> = tokens.iter().map(|(n, t)| (n.to_string(), secret(t))).collect();
    if r.tokens != expected {
        return Err(format!("built tokens {:?}, not {expected:?}", r.tokens));
    }
    let acknowledging: BTreeSet<String> = (tokens.iter())
        .filter(|(_, t)| t.get("acknowledgement").and_then(Value::as_bool) == Some(true))
        .map(|(n, _)| n.to_string())
        .collect();
    if hec.acknowledging != acknowledging {
        return Err(format!(
            "built acknowledging {:?}, not {acknowledging:?}",
            hec.acknowledging
        ));
    }

    // A sourcetype broken per line, by name or by default, needs no line breaker.
    let sourcetypes: BTreeMap<String, String> = (field("sourcetypes").and_then(Value::as_object).into_iter().flatten())
        .filter_map(|(name, t)| Some((name.clone(), t.get("lineBreaker")?.as_str()?.to_string())))
        .filter(|(_, pattern)| pattern != PER_LINE)
        .collect();
    let built: BTreeMap<String, String> = (hec.sourcetypes.iter())
        .map(|(name, breaker)| (name.clone(), breaker.0.as_str().to_string()))
        .collect();
    if built != sourcetypes {
        return Err(format!("built sourcetypes {built:?}, not {sourcetypes:?}"));
    }

    let named = field("certificate").and_then(Value::as_str);
    if r.tls.as_ref().and_then(|t| t.certificate.as_deref()) != named {
        return Err(format!("named the certificate {named:?}, built {:?}", r.tls));
    }
    let listed = |key: &str| -> Vec<String> {
        (field(key).and_then(Value::as_array).into_iter().flatten())
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    };
    let senders = r.tls.as_ref().map(|t| t.senders.clone()).unwrap_or_default();
    let fingerprints: Vec<_> = (listed("senderFingerprints").iter())
        .map(|f| crate::io::senders::Fingerprint::parse(f).unwrap())
        .collect();
    let names: Vec<_> = listed("senderNames").iter().map(|n| n.trim().to_string()).collect();
    if senders.fingerprints != fingerprints || senders.names != names {
        return Err(format!("built senders {senders:?}, not {fingerprints:?} and {names:?}"));
    }
    let max = field("maxRequestBytes")
        .and_then(Value::as_u64)
        .unwrap_or(MaxRequestBytes::DEFAULT);
    if r.max_request_bytes as u64 != max {
        return Err(format!("built maxRequestBytes {}, not {max}", r.max_request_bytes));
    }
    if source.kind.meta() != SourceMeta::HEC_RECEIVER.with_encoding(source.kind.encoding()) {
        return Err(format!("built meta {:?}", source.kind.meta()));
    }
    system
        .compile_topology()
        .map_err(|e| format!("the accepted bundle does not compile: {e:?}"))?;

    if let Some(name) = named {
        let (what, refuses) = HELD[case.held];
        let mut held = HeldSecrets::default();
        held.take([SecretDelivery {
            name: name.to_string(),
            version: 1,
            value: "key".to_string(),
            chain: CHAINS[case.held].clone(),
        }]);
        // A value with no certificate is held as a secret, so the certificate goes missing rather than unusable.
        // The tokens are missing too, which is not what is asked here.
        let mut errors: Vec<String> = (system.topology.missing_secrets(&held).into_iter())
            .filter(|e| e.contains("the certificate"))
            .collect();
        errors.extend(system.topology.certificate_errors(&held));
        if errors.is_empty() == refuses {
            return Err(format!("a worker holding {what} came to {errors:?}"));
        }
    }
    Ok(())
}

/// What a worker makes of the certificate the accepted receiver names, where it names one.
fn presents(case: &Case) -> Option<String> {
    case.source.get("certificate")?;
    Some(held_label(HELD[case.held]))
}

/// What a worker holding one of [`HELD`] reaches.
fn held_label((what, refuses): (&str, bool)) -> String {
    format!(
        "a worker holding {what} {}",
        if refuses { "refuses it" } else { "serves it" }
    )
}

/// Every option of a HEC receiver, by its name in the file.
const OPTIONS: [&str; 10] = [
    "port",
    "certificate",
    "trustedAuthorities",
    "senderFingerprints",
    "senderNames",
    "maxRequestBytes",
    "tokens",
    "acknowledgement",
    "sourcetypes",
    "lineBreaker",
];

/// The options `case`'s source sets, a token's and a sourcetype's own among them.
fn options(case: &Case) -> Vec<&'static str> {
    let set = |key: &str| case.source.get(key).is_some();
    let inner = |key: &str, of: &str| {
        (case.source.get(key).and_then(Value::as_object).into_iter().flatten()).any(|(_, v)| v.get(of).is_some())
    };
    let mut set: Vec<_> = OPTIONS[..7].iter().copied().filter(|o| set(o)).collect();
    if inner("tokens", "acknowledgement") {
        set.push("acknowledgement");
    }
    if case.source.get("sourcetypes").is_some() {
        set.push("sourcetypes");
    }
    if inner("sourcetypes", "lineBreaker") {
        set.push("lineBreaker");
    }
    set
}

/// How a sender reaches the accepted or refused receiver: plain, or over TLS, and how it is admitted.
fn admits(case: &Case) -> &'static str {
    let set = |key: &str| case.source.get(key).is_some();
    let tls = ["certificate", "trustedAuthorities", "senderFingerprints", "senderNames"]
        .into_iter()
        .any(set);
    let tokens = (case.source.get("tokens").and_then(Value::as_object)).is_some_and(|t| !t.is_empty());
    let vouches = set("trustedAuthorities") || set("senderFingerprints");
    match (tls, tokens, vouches) {
        (false, true, _) => "a token in the clear",
        (true, true, _) => "a token over TLS",
        (true, false, true) => "a certificate",
        (_, false, _) => "nobody",
    }
}

/// Runs `generate` over the seeds (`fuzz_loader::run`), marking what each case reaches besides its refusals.
fn run(seeds: std::ops::Range<u64>, generate: fn(&mut Rng) -> Case, expected: Vec<String>) {
    let pems = Pems::new();
    let bundle = |case: &Case| bundle(case, &pems);
    fuzz_loader::run(
        seeds,
        generate,
        &OPTIONS,
        expected,
        bundle,
        |case, verdict, coverage| {
            for option in options(case) {
                coverage.reach(option);
            }
            coverage.reach(match verdict {
                Verdict::Shape => "refused by shape".to_string(),
                Verdict::Refused => format!("refused admitting {}", admits(case)),
                Verdict::Accepted => format!("accepted admitting {}", admits(case)),
            });
            if let (Verdict::Accepted, Some(presents)) = (verdict, presents(case)) {
                coverage.reach(presents);
            }
        },
    );
}

#[test]
fn known_hec_receiver_configs_get_the_verdict_the_rules_give() {
    // Every field the rules refuse at: a token's and a sourcetype's own are refused at the map holding them.
    let refusals = OPTIONS[..7]
        .iter()
        .chain(&["sourcetypes"])
        .map(|o| format!("refused at {o}"));
    let verdicts = ["a token in the clear", "a token over TLS", "a certificate"]
        .into_iter()
        .flat_map(|a| [format!("accepted admitting {a}"), format!("refused admitting {a}")])
        .chain(["refused admitting nobody".to_string()]);
    let held = HELD.map(held_label);
    run(1..2_000, known, refusals.chain(verdicts).chain(held).collect());
}

#[test]
fn random_hec_receiver_configs_never_panic_and_every_refusal_names_a_field() {
    let verdicts = [
        "refused by shape",
        "refused admitting nobody",
        "refused admitting a token in the clear",
    ];
    run(1..3_000, garbage, verdicts.map(String::from).to_vec());
}
