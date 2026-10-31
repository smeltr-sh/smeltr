//! A certificate ordered from Pebble, Let's Encrypt's test authority (`just live-up`), over each challenge.
//!
//! Pebble validates by connecting back to the host these tests run on, as `host.docker.internal`, on the ports its
//! config names: 5001 for TLS-ALPN-01, which is where the leader under test serves, and 5002 for HTTP-01. One
//! leader at a time, since the ports are fixed: `just check-live` runs this suite on one thread.
//!
//! DNS-01 is ordered from a second Pebble that asks challtestsrv for every name. The order is placed through the
//! library rather than a leader process, against a stand-in for Cloudflare's API that publishes into challtestsrv.

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use crate::live;

/// The name Pebble reaches this host by, and the name the certificate carries.
const NAME: &str = "host.docker.internal";
const TLS_PORT: u16 = 5001;
const HTTP_PORT: u16 = 5002;

/// A running leader, its console captured, killed on drop.
struct Leader {
    child: Child,
    console: Arc<Mutex<String>>,
    /// Where the tests reach it: loopback, with the `Host` header saying the public name, so the name the session
    /// arrived over is the one the certificate carries.
    base: String,
    origin: String,
    password: String,
    agent: ureq::Agent,
    cookie: String,
    csrf: String,
}

impl Drop for Leader {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("smeltr-acme-live-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A leader serving TLS on every address at the port Pebble connects to, with no worker inside it: the worker would
/// reach it by loopback, which no public certificate carries.
fn serve(dir: &std::path::Path) -> Leader {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_smeltr"));
    cmd.current_dir(dir);
    for var in ["SMELTR_RECOVER_ADMIN", "SMELTR_CONFIG_FILE"] {
        cmd.env_remove(var);
    }
    let origin = format!("https://{NAME}:{TLS_PORT}");
    cmd.env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "0")
        .env("SMELTR_GUI_BIND", "0.0.0.0")
        .env("SMELTR_GUI_PORT", TLS_PORT.to_string())
        .env("SMELTR_PUBLIC_ORIGIN", &origin)
        .env("SMELTR_ACME_HTTP_PORT", HTTP_PORT.to_string())
        .env("SMELTR_LEADER_DATA_DIR", dir.join("leader").to_str().unwrap())
        .env(
            "SMELTR_LEADER_SECRET_KEY",
            dir.join("keys/sealing.key").to_str().unwrap(),
        );
    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr");
    let console = Arc::new(Mutex::new(String::new()));
    let sink = console.clone();
    let pipe = child.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut held = sink.lock().unwrap();
            held.push_str(&line);
            held.push('\n');
        }
    });
    // The leader's own certificate at first, then Pebble's, which chains to a root this process does not trust:
    // what is served is read off the API rather than checked at the socket.
    let tls = ureq::tls::TlsConfig::builder().disable_verification(true).build();
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .tls_config(tls)
        .http_status_as_error(false)
        .timeout_global(Some(std::time::Duration::from_secs(180)))
        .build()
        .into();
    let mut leader = Leader {
        child,
        console,
        base: format!("https://127.0.0.1:{TLS_PORT}"),
        origin,
        password: String::new(),
        agent,
        cookie: String::new(),
        csrf: String::new(),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let said = leader.console.lock().unwrap().clone();
        if said.contains("one-time password:") && std::net::TcpStream::connect(("127.0.0.1", TLS_PORT)).is_ok() {
            leader.password = said
                .lines()
                .find_map(|l| l.split_once("one-time password:"))
                .unwrap()
                .1
                .trim()
                .to_string();
            break;
        }
        assert!(
            !leader.child.try_wait().is_ok_and(|exited| exited.is_some()),
            "the leader exited: {said}"
        );
        assert!(std::time::Instant::now() < deadline, "the leader did not serve: {said}");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    // The bootstrap password has to be replaced before anything else is allowed, and replacing it ends the session.
    leader.sign_in();
    let chosen = "a perfectly ordinary passphrase";
    let (status, said) = leader.call(
        "PUT",
        "/api/auth/password",
        Some(serde_json::json!({ "username": "admin", "newPassword": chosen })),
    );
    assert_eq!(status, 200, "{said}");
    leader.password = chosen.to_string();
    leader.sign_in();
    leader
}

impl Leader {
    fn sign_in(&mut self) {
        let body = serde_json::json!({ "username": "admin", "password": self.password }).to_string();
        let response = self
            .agent
            .post(format!("{}/api/auth/login", self.base))
            .header("host", format!("{NAME}:{TLS_PORT}"))
            .header("origin", &self.origin)
            .header("content-type", "application/json")
            .send(&body)
            .expect("the login call was answered");
        assert_eq!(
            response.status(),
            200,
            "sign-in refused: {}",
            response.into_body().read_to_string().unwrap()
        );
        self.cookie = response
            .headers()
            .get("set-cookie")
            .expect("a session cookie")
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        let dto: serde_json::Value = response.into_body().read_json().unwrap();
        self.csrf = dto["csrf"].as_str().expect("a csrf token").to_string();
    }

    /// `method path`, with the session, as the GUI would send it. The status and the body.
    fn call(&self, method: &str, path: &str, body: Option<serde_json::Value>) -> (u16, serde_json::Value) {
        let request = ureq::http::Request::builder()
            .method(method)
            .uri(format!("{}{path}", self.base))
            .header("host", format!("{NAME}:{TLS_PORT}"))
            .header("origin", &self.origin)
            .header("cookie", &self.cookie)
            .header("x-smeltr-csrf", &self.csrf)
            .header("content-type", "application/json")
            .body(body.map(|b| b.to_string()).unwrap_or_default())
            .unwrap();
        let response = self
            .agent
            .run(request)
            .unwrap_or_else(|e| panic!("{method} {path}: {e}"));
        let status = response.status().as_u16();
        let text = response.into_body().read_to_string().unwrap();
        let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        (status, json)
    }
}

/// The roots the leader is told to trust: minica's, which signs Pebble's own certificate, and the one Pebble issues
/// under, which is new at every start and read off its management port.
fn roots(directory: &str) -> String {
    // The management port is the directory's plus a thousand, as `compose.live.yaml` publishes both Pebbles.
    let (host, port) = directory
        .trim_end_matches("/dir")
        .rsplit_once(':')
        .expect("a directory URL with a port");
    let management = format!("{host}:{}", port.parse::<u16>().unwrap() + 1000);
    let tls = ureq::tls::TlsConfig::builder().disable_verification(true).build();
    let agent: ureq::Agent = ureq::Agent::config_builder().tls_config(tls).build().into();
    let issuing = agent
        .get(format!("{management}/roots/0"))
        .call()
        .expect("Pebble's issuing root")
        .into_body()
        .read_to_string()
        .unwrap();
    format!("{}\n{issuing}", include_str!("live/pebble/minica.pem"))
}

#[test]
fn pebble_issues_over_tls_alpn_01_and_renews_over_http_01() {
    let Some(directory) = live::pebble("pebble_issues_over_tls_alpn_01_and_renews_over_http_01") else {
        return;
    };
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let dir = scratch("pebble");
    let leader = serve(&dir);
    let roots = roots(&directory);

    let settings = |challenge: &str| {
        serde_json::json!({
            "enabled": true,
            "directory": directory,
            "contact": "",
            "names": [NAME],
            "challenge": challenge,
            "authority": roots,
            "eabKeyId": "",
            "eabHmac": null,
        })
    };
    let (status, saved) = leader.call("PUT", "/api/settings/tls/acme", Some(settings("tls-alpn-01")));
    assert_eq!(status, 200, "{saved}");
    assert_eq!(saved["challenge"], "tls-alpn-01");
    assert_eq!(saved["httpPort"], HTTP_PORT);
    assert!(saved["issued"].is_null());

    // The first order is under another root than the leader's own, so it begins a rotation rather than serving at
    // once. Nobody is enrolled to wait for, and the next read of the certificates finishes it.
    let (status, ordered) = leader.call("POST", "/api/settings/tls/acme/order", None);
    assert_eq!(status, 200, "{ordered}");
    assert_eq!(
        ordered["source"], "own",
        "nothing served before the fleet holds the root"
    );
    assert_eq!(ordered["rotation"]["installs"], "supplied", "{ordered}");
    let (status, certificates) = leader.call("GET", "/api/settings/tls", None);
    assert_eq!(status, 200, "{certificates}");
    assert_eq!(certificates["source"], "acme", "{certificates}");
    assert!(certificates["rotation"].is_null(), "{certificates}");
    assert!(
        certificates["served"]["names"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n == NAME)
    );
    assert_eq!(certificates["restartRequired"], false);
    let issuing = certificates["authority"]["fingerprint"].as_str().unwrap().to_string();
    let first = certificates["served"]["fingerprint"].as_str().unwrap().to_string();
    let (_, acme) = leader.call("GET", "/api/settings/tls/acme", None);
    assert_eq!(acme["issued"], first);
    assert!(acme["lastError"].is_null(), "{acme}");
    assert!(acme["renewAfterMillis"].as_i64().unwrap() < acme["notAfterMillis"].as_i64().unwrap());

    // A renewal over the other challenge: the same root, so it is served at once.
    let (status, saved) = leader.call("PUT", "/api/settings/tls/acme", Some(settings("http-01")));
    assert_eq!(status, 200, "{saved}");
    assert_eq!(saved["issued"], first, "changing the challenge forgets nothing");
    let (status, renewed) = leader.call("POST", "/api/settings/tls/acme/order", None);
    assert_eq!(status, 200, "{renewed}");
    assert_eq!(renewed["source"], "acme", "{renewed}");
    assert!(renewed["rotation"].is_null(), "{renewed}");
    assert_eq!(renewed["authority"]["fingerprint"], issuing);
    assert_ne!(renewed["served"]["fingerprint"], first);
    // The listener is gone with the order.
    assert!(std::net::TcpStream::connect(("127.0.0.1", HTTP_PORT)).is_err());

    // A name this host does not hold: Pebble cannot reach it, and the page is told so.
    let mut elsewhere = settings("tls-alpn-01");
    elsewhere["names"] = serde_json::json!([NAME, "elsewhere.invalid"]);
    let (status, _) = leader.call("PUT", "/api/settings/tls/acme", Some(elsewhere));
    assert_eq!(status, 200);
    let (status, refused) = leader.call("POST", "/api/settings/tls/acme/order", None);
    assert_eq!(status, 400, "{refused}");
    let (_, acme) = leader.call("GET", "/api/settings/tls/acme", None);
    assert!(acme["lastError"].as_str().is_some_and(|e| !e.is_empty()), "{acme}");
    assert_eq!(
        acme["issued"], renewed["served"]["fingerprint"],
        "what is served is still the renewal"
    );
    assert!(
        acme["renewAfterMillis"].is_i64(),
        "saving other names leaves the renewal where it was: the page orders them itself"
    );
    let (_, certificates) = leader.call("GET", "/api/settings/tls", None);
    assert_eq!(
        certificates["served"]["fingerprint"], renewed["served"]["fingerprint"],
        "still serving the renewal"
    );
}

/// Cloudflare's API as a DNS-01 order calls it, on loopback, publishing into challtestsrv: every name is in the one
/// zone `example.test`.
/// Also every record it added, by name and value.
async fn cloudflare_into(management: String) -> (String, Arc<Mutex<Vec<(String, String)>>>) {
    use axum::extract::Path;
    use axum::routing::{delete, get, post};

    async fn challtestsrv(management: &str, what: &str, body: serde_json::Value) {
        let response = reqwest::Client::new()
            .post(format!("{management}/{what}"))
            .body(body.to_string())
            .send()
            .await
            .expect("challtestsrv answers");
        assert!(response.status().is_success(), "challtestsrv refused {what}");
    }
    let ok =
        |result: serde_json::Value| serde_json::json!({ "success": true, "errors": [], "result": result }).to_string();
    // Record id to its name: challtestsrv clears a name, not a value.
    let records: Arc<Mutex<std::collections::HashMap<String, String>>> = Default::default();
    let added: Arc<Mutex<Vec<(String, String)>>> = Default::default();
    let (adding, removing, every) = (records.clone(), records, added.clone());
    let (add_at, remove_at) = (management.clone(), management);
    let app = axum::Router::new()
        .route(
            "/zones",
            get(
                move |axum::extract::RawQuery(query): axum::extract::RawQuery| async move {
                    let zone = query.unwrap_or_default() == "name=example.test";
                    ok(serde_json::json!(if zone {
                        vec![serde_json::json!({ "id": "z1", "name": "example.test" })]
                    } else {
                        vec![]
                    }))
                },
            ),
        )
        .route(
            "/zones/z1/dns_records",
            post(move |body: String| async move {
                let asked: serde_json::Value = serde_json::from_str(&body).unwrap();
                let name = asked["name"].as_str().unwrap().to_string();
                challtestsrv(
                    &add_at,
                    "set-txt",
                    serde_json::json!({ "host": format!("{name}."), "value": asked["content"] }),
                )
                .await;
                every
                    .lock()
                    .unwrap()
                    .push((name.clone(), asked["content"].as_str().unwrap().to_string()));
                let mut held = adding.lock().unwrap();
                let id = format!("r{}", held.len() + 1);
                held.insert(id.clone(), name);
                ok(serde_json::json!({ "id": id }))
            }),
        )
        .route(
            "/zones/z1/dns_records/:id",
            delete(move |Path(id): Path<String>| async move {
                let name = removing
                    .lock()
                    .unwrap()
                    .remove(&id)
                    .expect("a record this stand-in added");
                challtestsrv(
                    &remove_at,
                    "clear-txt",
                    serde_json::json!({ "host": format!("{name}.") }),
                )
                .await;
                ok(serde_json::json!({ "id": id }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (api, added)
}

/// challtestsrv as the zone's one nameserver.
struct Challtestsrv(std::net::SocketAddr);

#[async_trait::async_trait]
impl smeltr::leader::acme_dns::Nameservers for Challtestsrv {
    async fn all_answer(&self, record: &smeltr::leader::acme_dns::Record) -> Result<bool, String> {
        smeltr::leader::acme_dns::answers(self.0, record).await
    }
}

#[test]
fn pebble_issues_over_dns_01_and_every_record_is_removed() {
    use smeltr::leader::{acme, acme_dns, tls};

    let Some(pebble) = live::pebble_dns("pebble_issues_over_dns_01_and_every_record_is_removed") else {
        return;
    };
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let tls_dir = scratch("pebble-dns").join("tls");
    // A wildcard too: only DNS-01 can prove one, and it is proved at the name beneath it.
    let names = vec!["leader.example.test".to_string(), "*.example.test".to_string()];
    crate::harness::leader_tls::listing(&tls_dir);
    tls::ensure(
        &tls_dir,
        names.clone(),
        std::time::Duration::from_secs(365 * 24 * 60 * 60),
        &[],
    )
    .unwrap();
    // No socket here: what is on disk is taken as served, as a leader's socket would have taken it.
    tls::socket_took(&tls_dir, tls::disk_generation(&tls_dir));
    let settings = acme::Settings {
        enabled: true,
        directory: pebble.directory.clone(),
        contact: String::new(),
        names: names.clone(),
        challenge: acme::Challenge::Dns01,
        authority: roots(&pebble.directory),
        eab_key_id: String::new(),
        eab_hmac: None,
        dns_provider: acme_dns::Kind::Cloudflare,
        cloudflare_token: Some("a token".to_string()),
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (api, added) = cloudflare_into(pebble.management.clone()).await;
        let publishing = acme_dns::Publishing::new(
            Box::new(acme_dns::Cloudflare::new(&api, "a token").unwrap()),
            Box::new(Challtestsrv(pebble.dns)),
        );
        let issuer = acme::Directory::new(settings.clone(), &tls_dir);
        let issued = acme::order(&tls_dir, &settings, &issuer, &publishing, &names, &[])
            .await
            .expect("Pebble issues over DNS-01");
        let pem = String::from_utf8(issued.chain.clone()).unwrap();
        let carried = tls::describe(pem.as_bytes(), "the certificate").unwrap().names;
        for name in &names {
            assert!(carried.contains(name), "{name} is not among {carried:?}");
        }
        // One record for each name, at the name the authority asks, and none left once the order is over.
        let added = added.lock().unwrap().clone();
        let mut asked: Vec<String> = added.iter().map(|(name, _)| name.clone()).collect();
        asked.sort();
        assert_eq!(
            asked,
            ["_acme-challenge.example.test", "_acme-challenge.leader.example.test"]
        );
        for (name, value) in added {
            let record = acme_dns::Record {
                zone: "example.test".to_string(),
                zone_id: String::new(),
                name,
                value,
                id: String::new(),
            };
            assert_eq!(
                acme_dns::answers(pebble.dns, &record).await,
                Ok(false),
                "{} is still published",
                record.name
            );
        }
    });
}
