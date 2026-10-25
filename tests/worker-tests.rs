//! The control-plane loop end to end: the built binary as leader (TLS, its
//! own authority) and as a worker subprocess -- enrollment, heartbeat, a
//! git-fetched bundle, real Kafka records. Needs `KAFKA_TEST_BROKERS`.

use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use smeltr::format::encoding::Encoding;
use smeltr::io::kafka::{KafkaDestination, KafkaSource};
use smeltr::io::{Destination, OutgoingRecord, OwnedRecord, Source};
use smeltr::leader::registry::Registry;
use smeltr::leader::sqlite_store::SqliteStore;
use smeltr::leader::{AppState, router};
use smeltr::plan::topology::{KafkaDestinationConfig, KafkaSourceConfig, StartPosition};
use std::borrow::Cow;

/// Hand `rows` to `dest` as one batch.
fn hand(dest: &mut dyn Destination, rows: &[OutgoingRecord<'_>]) {
    let rows: Vec<OwnedRecord> = rows.iter().map(OwnedRecord::from).collect();
    dest.deliver(0, Cow::Owned(rows)).unwrap();
}

/// A destination reports librdkafka's statistics through a handle; these tests do not read them.
fn kafka_metrics() -> smeltr::metrics::MetricsHandle {
    smeltr::metrics::MetricsHandle::new(std::sync::Arc::new(smeltr::metrics::Metrics::new()), "test")
}

use crate::live;

fn live_brokers(test_name: &str) -> Option<String> {
    live::kafka(test_name)
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// How many leaders may bootstrap at once, as in `gate-tests`: each creates its first account with a
/// deliberately expensive Argon2id hash, and these tests run a worker subprocess alongside every one of them.
/// Enough at the same time and the slowest misses its deadline for no reason but the others.
const CONCURRENT_STARTS: usize = 2;

static STARTS_FREE: std::sync::Mutex<usize> = std::sync::Mutex::new(CONCURRENT_STARTS);
static A_START_FINISHED: std::sync::Condvar = std::sync::Condvar::new();

/// Held while a leader is bootstrapping. Dropping it lets the next one begin.
struct StartPermit;

impl StartPermit {
    fn acquire() -> StartPermit {
        let mut free = STARTS_FREE.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            free = A_START_FINISHED.wait(free).unwrap_or_else(|e| e.into_inner());
        }
        *free -= 1;
        StartPermit
    }
}

impl Drop for StartPermit {
    fn drop(&mut self) {
        *STARTS_FREE.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        A_START_FINISHED.notify_one();
    }
}

/// Sign in as the administrator the leader bootstrapped itself with, and choose a password, since a session
/// holding one it did not choose may do nothing else. Returns the cookie an operator route takes.
///
/// The one-time password is on the console, which is the only place it is ever written.
fn operator_session(agent: &ureq::Agent, base: &str, console: &Arc<std::sync::Mutex<String>>) -> String {
    install_crypto_provider();
    let _permit = StartPermit::acquire();
    // A minute, as `gate-tests` gives it. Thirty seconds is tight for an Argon2id bootstrap on a machine already
    // running the rest of the suite, and a leader that needs longer than a minute is telling you something.
    let deadline = Instant::now() + Duration::from_secs(60);
    let password = loop {
        let said = console.lock().unwrap().clone();
        if let Some((_, rest)) = said.split_once("one-time password:") {
            break rest.lines().next().unwrap().trim().to_string();
        }
        assert!(Instant::now() < deadline, "no password was printed:\n{said}");
        std::thread::sleep(Duration::from_millis(100));
    };

    // The password is printed before the socket is bound -- the console says the leader got that far, not that it
    // is reachable -- so the first request below would race it. A connection is the readiness signal, as it is in
    // `gate-tests`.
    let address = base.trim_start_matches("http://").trim_start_matches("https://");
    while std::net::TcpStream::connect(address).is_err() {
        assert!(
            Instant::now() < deadline,
            "the leader printed a password but never listened"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    let sign_in = |password: &str| -> (String, String) {
        let body = serde_json::json!({ "username": "admin", "password": password }).to_string();
        let response = agent
            .post(format!("{base}/api/auth/login"))
            .header("origin", base)
            .header("content-type", "application/json")
            .send(&body)
            .expect("the login call was answered");
        let cookie = response
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
        (cookie, dto["csrf"].as_str().expect("a csrf token").to_string())
    };

    let (cookie, csrf) = sign_in(&password);
    let chosen = "a perfectly ordinary passphrase";
    let body = serde_json::json!({
        "username": "admin",
        "newPassword": chosen,
    })
    .to_string();
    agent
        .post(format!("{base}/api/auth/password"))
        .header("origin", base)
        .header("cookie", &cookie)
        .header("x-smeltr-csrf", &csrf)
        .header("content-type", "application/json")
        .send(&body)
        .expect("the password change was answered");
    sign_in(chosen).0
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(unique(&format!("smeltr-worker-test-{name}")));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Same shape as `tests/leader-tests.rs`'s own `start_leader_tls` --
/// duplicated rather than shared, since each `tests/*.rs` file compiles as
/// its own independent crate. Returns the leader's base URL and its
/// certificate authority's PEM bytes, exactly what a worker's bootstrap
/// config would be handed out of band.
fn start_leader_tls() -> (
    String,
    Vec<u8>,
    std::path::PathBuf,
    axum_server::Handle<std::net::SocketAddr>,
) {
    install_crypto_provider();

    let data_dir = temp_dir("leader-data");
    let returned_data_dir = data_dir.clone();
    let materials = smeltr::leader::tls::ensure(
        &data_dir.join("tls"),
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
        Duration::from_secs(60 * 60),
        &[],
    )
    .expect("an authority and a certificate it signed");
    // What `main` says once it has built the socket from these files: it has what is on disk.
    let tls_dir = data_dir.join("tls");
    smeltr::leader::tls::socket_took(&tls_dir, smeltr::leader::tls::disk_generation(&tls_dir));
    let ca_pem = materials.ca_pem.clone();
    let returned_ca_pem = ca_pem.clone();
    let cert_pem = std::fs::read(&materials.server_cert_path).unwrap();
    let key_pem = std::fs::read(&materials.server_key_path).unwrap();

    // The caller's way to stop this leader. `axum_server` holds its own clone, so a caller that never shuts down can
    // drop theirs; without one, a test that means to take the leader away only stops talking to it.
    let handle = axum_server::Handle::new();
    let server_handle = handle.clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = SqliteStore::open(
                std::path::Path::new(":memory:"),
                &temp_dir("leader-key").join("sealing.key"),
            )
            .unwrap();
            let state = AppState {
                store: Arc::new(store),
                registry: Arc::new(Registry::default()),
                throughput: Default::default(),
                captures: Arc::new(smeltr::leader::capture::Captures::default()),
                entitlements: Arc::new(smeltr::leader::Entitlements::default()),
                verify_limiter: Arc::new(smeltr::leader::VerifyLimiter::default()),
                expiries: Default::default(),
                deploy_checks: Default::default(),
                data_dir,
                origin_hosts: Vec::new(),
                remote: Default::default(),
                updates: Default::default(),
                directives: Default::default(),
            };
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem, key_pem)
                .await
                .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr.port()).unwrap();
            axum_server::tls_rustls::from_tcp_rustls(listener, tls)
                .unwrap()
                .handle(server_handle)
                .serve(router(state).into_make_service())
                .await
                .unwrap();
        });
    });
    let port = rx.recv().expect("leader thread reports its bound port");
    (
        format!("https://127.0.0.1:{port}"),
        returned_ca_pem,
        returned_data_dir,
        handle,
    )
}

/// What `main` installs before anything else, for the agents this test process builds itself. Every test asks,
/// since which one runs first is the harness's choice.
fn install_crypto_provider() {
    static INSTALL_CRYPTO_PROVIDER: Once = Once::new();
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install rustls crypto provider");
    });
}

/// Blocks until nothing answers on `port`, so a test that took its leader away is testing that rather than a race
/// against a listener still winding down.
fn wait_until_port_refuses(port: u16, within: Duration) {
    let addr: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("port {port} still accepts connections {within:?} after the leader was shut down");
}

/// The leader is on this host, which a proxy in the test's environment cannot reach, so the agent takes none.
fn admin_agent(ca_pem: &[u8]) -> ureq::Agent {
    install_crypto_provider();
    let roots = vec![
        ureq::tls::Certificate::from_pem(ca_pem)
            .expect("the leader's authority")
            .to_owned(),
    ];
    let tls = ureq::tls::TlsConfig::builder()
        .root_certs(ureq::tls::RootCerts::Specific(std::sync::Arc::new(roots)))
        .build();
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(smeltr::heartbeat::REQUEST_BUDGET))
        .tls_config(tls)
        .proxy(None);
    ureq::Agent::new_with_config(config.build())
}

/// Builds a `config_file::bundle` zip with one real Kafka source (reading
/// `input_topic`) routed straight through to one real Kafka destination
/// (writing `output_topic`) -- no parsing, a pure passthrough, since this
/// test only cares whether records make the whole round trip, not what
/// the pipeline does to them.
fn passthrough_bundle(
    brokers: &str,
    input_topic: &str,
    output_topic: &str,
    group_id: &str,
) -> smeltr::config_file::FileConfig {
    let json = serde_json::json!({
        "version": 1,
        "sources": [{
            "id": "in",
            "type": "kafka",
            "brokers": brokers,
            "topics": [input_topic],
            "groupId": group_id,
        }],
        "destinations": [{
            "id": "out",
            "type": "kafka",
            "brokers": brokers,
            "defaultTopic": output_topic,
        }],
        "pipelines": [{
            "id": "passthrough",
            "name": "Passthrough",
            "steps": [],
        }],
        "routes": [{
            "id": "route",
            "source": "in",
            "condition": null,
            "pipeline": "passthrough",

            "deliver": [{"to": ["out"]}],
        }],
    })
    .to_string();
    smeltr::config_file::load_str(&json).expect("test bundle JSON is valid");
    serde_json::from_str(&json).expect("test bundle JSON parses")
}

/// Land a bundle on `main` for one group and deploy it, as approving a change does. A group is the files under `groups/<id>/`, so this also creates the group.
fn deploy_to_group(data_dir: &std::path::Path, group_id: &str, file: &smeltr::config_file::FileConfig) -> String {
    let repo = smeltr::leader::git::repo_path(data_dir);
    smeltr::leader::git::init_bare_repo(&repo).expect("a repository");
    smeltr::leader::git::ensure_main_commit(&repo).expect("main");
    let parent = smeltr::leader::git::main_head(&repo).expect("main's head");
    let edits: Vec<smeltr::leader::git::FileEdit> = smeltr::config_file::bundle_files(file)
        .expect("the bundle's files")
        .into_iter()
        .map(|(path, content)| smeltr::leader::git::FileEdit {
            path: format!("groups/{group_id}/{path}"),
            content: Some(content),
        })
        .collect();
    match smeltr::leader::git::commit_edits(
        &repo,
        "main",
        &parent,
        Some(&parent),
        &edits,
        &format!("deploy {group_id}"),
        &smeltr::leader::store::Actor::system(),
    )
    .expect("commit")
    {
        smeltr::leader::git::Committed::At(commit) => {
            smeltr::leader::git::deploy_commit(data_dir, &commit, &smeltr::leader::store::Actor::system())
                .expect("deploy");
            commit
        }
        other => panic!(
            "the deploy did not land: {}",
            match other {
                smeltr::leader::git::Committed::Unchanged => "nothing changed",
                _ => "main moved",
            }
        ),
    }
}

fn drain_at_least(source: &mut KafkaSource, want: usize, overall_budget: Duration) -> Vec<smeltr::io::SourceRecord> {
    let deadline = Instant::now() + overall_budget;
    let mut out = Vec::new();
    while out.len() < want && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let mut batch = source.poll_batch(want - out.len(), 10_000_000, remaining).unwrap();
        out.append(&mut batch);
    }
    out
}

/// `smeltr-leader --standalone`: one real `smeltr-leader` subprocess
/// (`[leader] enabled` and `[worker] enabled`, the actual built binary) with its own
/// embedded worker, no second process at all -- an admin deploys a
/// bundle exactly as they would against any other leader, and the
/// embedded worker picks it up over the same real HTTP(S)+git path an
/// external worker uses.
///
/// The group is the leader's own, and standalone startup has already seeded it (design doc, Control plane: a worker
/// names a
/// group and the leader has one for it to name) -- deploying below fills it in, the same as an admin deploying to
/// any other group.
#[test]
fn standalone_leader_runs_its_own_embedded_worker() {
    let Some(brokers) = live_brokers("standalone_leader_runs_its_own_embedded_worker") else {
        return;
    };

    let data_dir = temp_dir("standalone-data");
    let port = {
        // Reserve a free port, then release it immediately for the
        // subprocess to bind -- a small TOCTOU race, acceptable for a
        // test spawning an external process that needs a fixed port up
        // front.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let mut leader = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_WORKER_GROUP", "standalone")
        .env("SMELTR_LEADER_DATA_DIR", &data_dir)
        .env("SMELTR_GUI_PORT", port.to_string())
        // `smeltr` looks for a `smeltr.toml` in its *working directory*
        // (`startup_config::find_config_file`), and a test inherits the
        // cwd it was launched from -- the repo root, where a developer's
        // own gitignored `smeltr.toml` may well be sitting. That file
        // would silently reconfigure this subprocess (a `tls = false` in
        // it, say, means the `ca.pem` wait below never succeeds and this
        // test times out for reasons found nowhere in this file). Running
        // it from its own empty data dir means the test sees only the
        // configuration it sets explicitly, on any machine.
        .current_dir(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr --standalone");

    let ca_path = data_dir.join("tls").join("ca.pem");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ca_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let ca_pem = std::fs::read(&ca_path).expect("leader created its authority in time");
    let base = format!("https://127.0.0.1:{port}");
    let agent = admin_agent(&ca_pem);

    // The group the leader's own worker was told to run, seeded into `main`'s first commit at startup (design doc,
    // Control plane). Deploying below puts a configuration in it.
    let group_id = "standalone";

    // Polled, not called once: `ca.pem` existing does not mean the server is listening. `setup_leader` writes
    // the TLS files, opens the store and spawns the embedded worker, and only then binds the port -- so there is a
    // window where the file the loop above waits for is there and a connection is still refused. The sibling
    // standalone test polls for the same reason.
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && agent.get(format!("{base}/api/cluster")).call().is_err() {
        std::thread::sleep(Duration::from_millis(100));
    }

    let input_topic = unique("standalone-in");
    let output_topic = unique("standalone-out");
    let kafka_group = unique("standalone-group");
    let bundle = passthrough_bundle(&brokers, &input_topic, &output_topic, &kafka_group);
    deploy_to_group(&data_dir, group_id, &bundle);

    // Give the embedded worker a moment to pick up the new commit and
    // redeploy before producing.
    std::thread::sleep(Duration::from_secs(4));

    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            acks: smeltr::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.clone(),
            default_topic: input_topic,
            service: smeltr::plan::topology::KafkaService::Kafka,
            identity: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap();
    hand(&mut dest, &[OutgoingRecord::new(b"hello-from-standalone-mode")]);
    dest.flush().unwrap();

    let mut verify_source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![output_topic],
            group_id: unique("standalone-verify"),
            identity: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let records = drain_at_least(&mut verify_source, 1, Duration::from_secs(20));

    let _ = leader.kill();
    let output = leader.wait_with_output();
    if records.is_empty()
        && let Ok(output) = output
    {
        eprintln!(
            "smeltr --standalone stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "smeltr --standalone stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert_eq!(
        records.len(),
        1,
        "the embedded standalone worker should have forwarded exactly one record"
    );
    let output_text = String::from_utf8_lossy(records[0].bytes().unwrap());
    assert!(
        output_text.contains("hello-from-standalone-mode"),
        "unexpected output: {output_text}"
    );
}

/// Check in before running (design doc, Control plane): a worker runs nothing until a leader has answered a
/// heartbeat, whatever
/// its local clone holds. The clone buys speed, not independence -- a worker back after time away must not move
/// production data through the configuration it last held, and the leader is the only thing that can say what to run
/// now. Proven here for real: enroll and run a worker against a live leader, kill both, then start a fresh worker
/// process (same `state_dir`, same now-dead `SMELTR_LEADER_URL`) and confirm it forwards nothing.
#[test]
fn a_restarted_worker_runs_nothing_until_the_leader_answers() {
    let Some(brokers) = live_brokers("a_restarted_worker_runs_nothing_until_the_leader_answers") else {
        return;
    };

    let (base, ca_pem, data_dir, leader) = start_leader_tls();
    let agent = admin_agent(&ca_pem);

    let group_id = "offline-test";
    let input_topic = unique("offline-test-in");
    let output_topic = unique("offline-test-out");
    let kafka_group = unique("offline-test-group");
    let bundle = passthrough_bundle(&brokers, &input_topic, &output_topic, &kafka_group);
    deploy_to_group(&data_dir, group_id, &bundle);

    let code: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(serde_json::json!({}))
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    let enroll_code = code["code"]["value"].as_str().unwrap();

    let ca_path = temp_dir("offline-worker-ca").join("leader-ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();
    let state_dir = temp_dir("offline-worker-state");

    let spawn_worker = |enroll_code: Option<&str>| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_smeltr"));
        cmd.env("SMELTR_WORKER_ENABLED", "1")
            .env("SMELTR_LEADER_URL", &base)
            .env("SMELTR_LEADER_CA", ca_path.to_str().unwrap())
            .env("SMELTR_WORKER_GROUP", group_id)
            .env("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap())
            .env("SMELTR_WORKER_THREAD_LIMIT", "1")
            .env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", "500")
            // Isolated from any `smeltr.toml` in the cwd this test
            // inherited -- see the leader spawn above for why.
            .current_dir(&state_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(code) = enroll_code {
            cmd.env("SMELTR_WORKER_ENROLLMENT_CODE", code);
        }
        cmd.spawn().expect("spawn smeltr in its worker role")
    };

    // First run: enrolls for real, fetches the bundle, and persists its
    // credential + git clone under `state_dir` -- proving the round trip
    // works at all before we ever pull the leader out from under it.
    let mut worker = spawn_worker(Some(enroll_code));
    std::thread::sleep(Duration::from_secs(5));

    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            acks: smeltr::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.clone(),
            default_topic: input_topic.clone(),
            service: smeltr::plan::topology::KafkaService::Kafka,
            identity: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap();
    hand(&mut dest, &[OutgoingRecord::new(b"hello-before-leader-goes-away")]);
    dest.flush().unwrap();

    let mut verify_source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: brokers.clone(),
            topics: vec![output_topic.clone()],
            group_id: unique("offline-verify"),
            identity: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let first_records = drain_at_least(&mut verify_source, 1, Duration::from_secs(20));
    assert_eq!(
        first_records.len(),
        1,
        "first run should have forwarded exactly one record before the leader goes away"
    );

    // Now take both processes down -- the leader for good, the worker
    // just so its *restart* is what we're testing, not merely a still
    // running process that never lost its in-memory deployment.
    let _ = worker.kill();
    let _ = worker.wait();

    // The leader goes away for real. Dropping the client only stops this test talking to it; the leader runs on a
    // thread of its own, and a worker that can still reach it is not the worker this test is about.
    drop(agent);
    leader.shutdown();
    let port = base
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .expect("the leader's base URL ends in its port");
    wait_until_port_refuses(port, Duration::from_secs(10));

    // Restart the worker, same `state_dir`, no enrollment code this time
    // (it already has a persisted credential) and pointed at the exact
    // same `SMELTR_LEADER_URL`, which is now unreachable. Its clone still
    // holds the bundle it ran a moment ago, which is exactly the state
    // that must not be enough to start on.
    let mut worker = spawn_worker(None);
    std::thread::sleep(Duration::from_secs(5));

    hand(&mut dest, &[OutgoingRecord::new(b"hello-after-leader-is-gone")]);
    dest.flush().unwrap();

    // Nothing should come out. The window is generous on purpose: a worker that was going to start from its cache
    // would have done so within a second, and the first worker's consumer group is not even involved, since nothing
    // is consuming.
    let leaked = drain_at_least(&mut verify_source, 1, Duration::from_secs(20));

    let _ = worker.kill();
    let output = worker.wait_with_output();
    let mut said = String::new();
    if let Ok(output) = output {
        said = String::from_utf8_lossy(&output.stdout).into_owned();
        if !leaked.is_empty() {
            eprintln!("worker stdout:\n{said}");
            eprintln!("worker stderr:\n{}", String::from_utf8_lossy(&output.stderr));
        }
    }

    let seen: Vec<String> = leaked
        .iter()
        .map(|r| String::from_utf8_lossy(r.bytes().unwrap()).into_owned())
        .collect();
    assert!(
        !seen.iter().any(|r| r.contains("hello-after-leader-is-gone")),
        "a worker whose leader never answered must forward nothing; saw {seen:?}"
    );
    assert!(
        !said.contains("redeploying to commit"),
        "and it should not have deployed anything; said: {said}"
    );
}

/// Soft revocation (`leader::mod`'s own doc: "a revoked credential blocks
/// new... but never forcibly stops an already-running worker"): revoking
/// a worker's credential makes its *next* heartbeat fail (401, proven
/// already in `tests/leader-tests.rs`), but a worker that's already
/// running keeps right on processing records -- forcibly stopping a
/// specific worker process is the infrastructure control plane's job,
/// not this leader's.
#[test]
fn a_revoked_worker_keeps_processing_records_after_revocation() {
    let Some(brokers) = live_brokers("a_revoked_worker_keeps_processing_records_after_revocation") else {
        return;
    };

    let (base, ca_pem, data_dir, _leader) = start_leader_tls();
    let agent = admin_agent(&ca_pem);

    let group_id = "revoke-test";
    let input_topic = unique("revoke-test-in");
    let output_topic = unique("revoke-test-out");
    let kafka_group = unique("revoke-test-group");
    let bundle = passthrough_bundle(&brokers, &input_topic, &output_topic, &kafka_group);
    deploy_to_group(&data_dir, group_id, &bundle);

    let code: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(serde_json::json!({}))
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    let enroll_code = code["code"]["value"].as_str().unwrap();

    let ca_path = temp_dir("revoke-worker-ca").join("leader-ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();
    let state_dir = temp_dir("revoke-worker-state");

    let mut child = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_LEADER_URL", &base)
        .env("SMELTR_LEADER_CA", ca_path.to_str().unwrap())
        .env("SMELTR_WORKER_GROUP", group_id)
        .env("SMELTR_WORKER_ENROLLMENT_CODE", enroll_code)
        .env("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap())
        .env("SMELTR_WORKER_THREAD_LIMIT", "1")
        .env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", "500")
        // Isolated from any `smeltr.toml` in the cwd this test
        // inherited -- see the leader spawn above for why. `smeltr` runs
        // as a worker here purely because SMELTR_LEADER_URL is set.
        .current_dir(&state_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr in its worker role");

    std::thread::sleep(Duration::from_secs(5));

    // The worker persists its own generated id under `state_dir` on its
    // very first run -- the same id it enrolled and now heartbeats with,
    // and the one the leader's revoke endpoint takes.
    let worker_id = std::fs::read_to_string(state_dir.join("worker_id"))
        .expect("worker persisted its own id")
        .trim()
        .to_string();

    let revoked: serde_json::Value = agent
        .post(format!("{base}/api/workers/{worker_id}/revoke"))
        .send_json(serde_json::json!({}))
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(revoked["ok"], true, "{revoked:?}");

    // Long enough for several heartbeat intervals (500ms) to elapse and
    // fail with 401 -- exactly the same "unreachable" path the worker's
    // own offline handling takes, per its own doc.
    std::thread::sleep(Duration::from_secs(2));

    let mut dest = KafkaDestination::new(
        &KafkaDestinationConfig {
            payload: smeltr::plan::config::Payload::Record,
            compression: smeltr::plan::topology::Compression::None,
            acks: smeltr::plan::topology::Acks::All,
            stall_after: std::time::Duration::from_secs(30),
            brokers: brokers.clone(),
            default_topic: input_topic,
            service: smeltr::plan::topology::KafkaService::Kafka,
            identity: None,
            buffer_bytes: 64 * 1024 * 1024,
            props: Default::default(),
            avro: None,
        },
        0,
        kafka_metrics(),
        &Default::default(),
    )
    .unwrap();
    hand(&mut dest, &[OutgoingRecord::new(b"hello-after-revocation")]);
    dest.flush().unwrap();

    let mut verify_source = KafkaSource::new(
        &KafkaSourceConfig {
            service: smeltr::plan::topology::KafkaService::Kafka,
            encoding: Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers,
            topics: vec![output_topic],
            group_id: unique("revoke-verify"),
            identity: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        },
        &Default::default(),
    )
    .unwrap();
    let records = drain_at_least(&mut verify_source, 1, Duration::from_secs(20));

    let _ = child.kill();
    let output = child.wait_with_output();
    if records.is_empty()
        && let Ok(output) = output
    {
        eprintln!("worker stdout:\n{}", String::from_utf8_lossy(&output.stdout));
        eprintln!("worker stderr:\n{}", String::from_utf8_lossy(&output.stderr));
    }

    assert_eq!(
        records.len(),
        1,
        "a revoked worker's already-running process should keep forwarding records"
    );
    let output_text = String::from_utf8_lossy(records[0].bytes().unwrap());
    assert!(
        output_text.contains("hello-after-revocation"),
        "unexpected output: {output_text}"
    );
}

/// The all-in-one deployment: one process, leader *and* worker, coming up
/// with nothing configured.
///
/// There is no seeding step to test any more -- the embedded worker gets
/// its configuration from git like every other worker, and serving a group
/// is not creating one: a group exists only once `main` holds a subtree
/// for it (design doc, Control plane), which nobody has approved yet on a fresh
/// standalone process. What startup owes the embedded worker is a way in
/// -- the repository and an enrollment code it mints for itself -- so it
/// enrolls and heartbeats, and runs nothing until an admin deploys a
/// change for "standalone", which is a state and not a failure.
///
/// Needs no broker, since nothing is deployed.
#[test]
fn standalone_enrolls_its_own_worker() {
    let data_dir = temp_dir("standalone-seed");
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let mut leader = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_WORKER_GROUP", "standalone")
        .env("SMELTR_LEADER_DATA_DIR", &data_dir)
        .env("SMELTR_GUI_PORT", port.to_string())
        // Same reasoning as the test above: run from an empty directory so
        // a developer's own `smeltr.toml` can't reconfigure this process.
        .current_dir(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr --standalone");

    // Drained on a thread of its own: the console carries the one-time password this leader bootstrapped with,
    // and a pipe nobody reads eventually blocks the process writing to it.
    let console = Arc::new(std::sync::Mutex::new(String::new()));
    let sink = console.clone();
    let pipe = leader.stderr.take().expect("stderr is piped");
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut held = sink.lock().unwrap();
            held.push_str(&line);
            held.push('\n');
        }
    });

    let ca_path = data_dir.join("tls").join("ca.pem");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ca_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let ca_pem = std::fs::read(&ca_path).expect("leader created its authority in time");
    let agent = admin_agent(&ca_pem);
    let base = format!("https://127.0.0.1:{port}");

    // The fleet view is an operator route, so this reads it the way an operator would: as the administrator the
    // leader bootstrapped itself with, having chosen a password of its own first.
    let session = operator_session(&agent, &base, &console);

    // Poll: the embedded worker's enrollment happens during startup,
    // concurrently with the server beginning to listen. `workerGroups`
    // stays empty throughout -- nobody has deployed a change for
    // "standalone" -- so enrollment is the one signal to wait on.
    let mut ready = None;
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(mut resp) = agent
            .get(format!("{base}/api/cluster"))
            .header("cookie", &session)
            .call()
            && let Ok(parsed) = resp.body_mut().read_json::<serde_json::Value>()
            && parsed["workers"].as_array().is_some_and(|w| !w.is_empty())
        {
            ready = Some(parsed);
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = leader.kill();
    let _ = leader.wait();
    if ready.is_none() {
        eprintln!("smeltr --standalone console:\n{}", console.lock().unwrap());
    }
    let ready = ready.expect("the standalone process should enroll its embedded worker");
    // A fresh install is a fleet that runs: the group its worker was told to serve is on `main` before any change is
    // deployed, so the first thing this leader reports is not a warning about itself (design doc, Control plane).
    let groups = ready["workerGroups"].as_array().expect("workerGroups is a list");
    assert_eq!(groups.len(), 1, "one seeded group and nothing else: {ready}");
    assert_eq!(groups[0]["id"], "standalone", "the group the worker named: {ready}");
    assert_eq!(groups[0]["deployed"], true, "seeded onto main, not pending: {ready}");
}

/// Discovery and deployment reach `main` on schedules of their own, with no broker involved.
///
/// The commit on a heartbeat is the worker's own answer to what it is running, and it can only give one by having
/// fetched `main` and deployed what it found. Since those are three separate threads now, a worker reporting a
/// commit is the three of them having agreed without any of them waiting on the others.
#[test]
fn a_worker_discovers_main_and_reports_the_commit_it_deployed() {
    let data_dir = temp_dir("standalone-discovers");
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let mut leader = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_WORKER_GROUP", "standalone")
        .env("SMELTR_LEADER_DATA_DIR", &data_dir)
        .env("SMELTR_GUI_PORT", port.to_string())
        .current_dir(&data_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr standalone");

    let console = Arc::new(std::sync::Mutex::new(String::new()));
    let sink = console.clone();
    let pipe = leader.stderr.take().expect("stderr is piped");
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut held = sink.lock().unwrap();
            held.push_str(&line);
            held.push('\n');
        }
    });

    // The authority, not the certificate it signed: a leader reissues its certificate at every start, and what a
    // client is given out of band is what that certificate chains to.
    let ca_path = data_dir.join("tls").join("ca.pem");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ca_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let ca_pem = std::fs::read(&ca_path).expect("leader generated its authority in time");
    let agent = admin_agent(&ca_pem);
    let base = format!("https://127.0.0.1:{port}");
    let session = operator_session(&agent, &base, &console);

    let mut reported = None;
    let mut last_seen = serde_json::Value::Null;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut resp) = agent
            .get(format!("{base}/api/cluster"))
            .header("cookie", &session)
            .call()
            && let Ok(parsed) = resp.body_mut().read_json::<serde_json::Value>()
        {
            last_seen = parsed.clone();
            let commit = parsed["health"]["threads"]
                .as_array()
                .and_then(|threads| threads.iter().find_map(|t| t["commit"].as_str()))
                .map(str::to_string);
            if let Some(commit) = commit {
                reported = Some((
                    commit,
                    parsed["deployedCommit"].as_str().unwrap_or_default().to_string(),
                ));
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = leader.kill();
    let _ = leader.wait();
    if reported.is_none() {
        eprintln!("last /api/cluster: {last_seen:#}");
        eprintln!("smeltr standalone console:\n{}", console.lock().unwrap());
    }
    let (commit, deployed) = reported.expect("the worker should fetch main and report the commit it deployed");
    assert_eq!(commit.len(), 40, "a full commit id, not a ref name: {commit}");
    // What the worker says it runs is what the leader says `main` holds: discovery found it and the deployment
    // took it, on two schedules neither of which is the one that heartbeats.
    assert_eq!(commit, deployed, "the worker converged on the commit `main` points at");
}

/// Children of `pid`, by the parent id in `/proc`.
#[cfg(unix)]
fn children_of(pid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return out;
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{name}/stat"))
            && let Some(ppid) = stat.rsplit(')').next().and_then(|rest| rest.split_whitespace().nth(1))
            && ppid.parse::<u32>() == Ok(pid)
        {
            out.push(name);
        }
    }
    out
}

/// The runtime process under `pid`, which is the one carrying the marker that selected that role.
#[cfg(unix)]
fn runtime_child_of(pid: u32) -> Option<u32> {
    children_of(pid).into_iter().find(|child| {
        std::fs::read(format!("/proc/{child}/environ"))
            .is_ok_and(|env| env.split(|b| *b == 0).any(|entry| entry == b"SMELTR_RUNTIME_CHILD=1"))
    })
}

/// An all-in-one process -- a leader and the worker inside it -- and what a test needs to watch it.
#[cfg(unix)]
struct Standalone {
    process: std::process::Child,
    data_dir: std::path::PathBuf,
    console: Arc<std::sync::Mutex<String>>,
    agent: ureq::Agent,
    base: String,
    session: String,
}

/// Where a standalone's leader listens, and the host each side reaches it by.
#[cfg(unix)]
struct Reach {
    /// `SMELTR_GUI_BIND`.
    bind: &'static str,
    /// The host its own worker calls its leader by, written into `SMELTR_LEADER_URL`; `None` leaves it unset.
    worker: Option<&'static str>,
    /// The host this test calls its leader by.
    test: &'static str,
    /// `SMELTR_WORKER_LISTEN_ADDRESS`, where its worker's receivers listen; `None` leaves it unset.
    receivers: Option<&'static str>,
}

#[cfg(unix)]
impl Reach {
    const LOOPBACK: Reach = Reach {
        bind: "127.0.0.1",
        worker: None,
        test: "127.0.0.1",
        receivers: None,
    };
}

#[cfg(unix)]
impl Standalone {
    /// Starts one on loopback, and signs in to it as its administrator.
    fn start(name: &str) -> Standalone {
        Standalone::start_reaching(name, &Reach::LOOPBACK)
    }

    /// Starts one where `reach` says, and signs in to it as its administrator.
    fn start_reaching(name: &str, reach: &Reach) -> Standalone {
        install_crypto_provider();
        let data_dir = temp_dir(name);
        let port = {
            let listener = std::net::TcpListener::bind((reach.bind.parse::<std::net::IpAddr>().unwrap(), 0)).unwrap();
            listener.local_addr().unwrap().port()
        };

        let mut command = Command::new(env!("CARGO_BIN_EXE_smeltr"));
        if let Some(host) = reach.worker {
            command.env("SMELTR_LEADER_URL", format!("https://{host}:{port}"));
        }
        if let Some(address) = reach.receivers {
            command.env("SMELTR_WORKER_LISTEN_ADDRESS", address);
        }
        let mut process = command
            .env("SMELTR_LEADER_ENABLED", "1")
            .env("SMELTR_WORKER_ENABLED", "1")
            .env("SMELTR_WORKER_GROUP", "standalone")
            .env("SMELTR_LEADER_DATA_DIR", &data_dir)
            .env("SMELTR_GUI_BIND", reach.bind)
            .env("SMELTR_GUI_PORT", port.to_string())
            .current_dir(&data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn smeltr standalone");

        let console = Arc::new(std::sync::Mutex::new(String::new()));
        let sink = console.clone();
        let pipe = process.stderr.take().expect("stderr is piped");
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
                let mut held = sink.lock().unwrap();
                held.push_str(&line);
                held.push('\n');
            }
        });

        // The authority, not the certificate it signed: a leader reissues its certificate at every start, and what a
        // client is given out of band is what that certificate chains to.
        let ca_path = data_dir.join("tls").join("ca.pem");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ca_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
        let ca_pem = std::fs::read(&ca_path).expect("leader generated its authority in time");
        let agent = admin_agent(&ca_pem);
        let base = format!("https://{}:{port}", reach.test);
        let session = operator_session(&agent, &base, &console);
        Standalone {
            process,
            data_dir,
            console,
            agent,
            base,
            session,
        }
    }

    /// Deploys `sources` to its group, each routed unchanged to one HTTP destination at `intake`.
    fn deploy(&self, sources: &[String], intake: &str) -> Result<(), String> {
        let repo = smeltr::leader::git::repo_path(&self.data_dir);
        let parent = smeltr::leader::git::main_head(&repo).ok_or("main has no commit")?;
        let file = |path: &str, content: String| smeltr::leader::git::FileEdit {
            path: format!("groups/standalone/{path}"),
            content: Some(content.into_bytes()),
        };
        let routes: Vec<serde_json::Value> = sources
            .iter()
            .map(|source| {
                let id = serde_json::from_str::<serde_json::Value>(source).unwrap()["id"].clone();
                serde_json::json!({"id": format!("r-{}", id.as_str().unwrap()), "source": id, "pipeline": "p",
                                   "deliver": [{"to": ["d"]}]})
            })
            .collect();
        let edits = [
            file("system.json", r#"{"version":1}"#.into()),
            file("sources.json", format!("[{}]", sources.join(","))),
            file(
                "destinations.json",
                format!(r#"[{{"id":"d","type":"http","url":"{intake}"}}]"#),
            ),
            file("pipelines/p.json", r#"{"id":"p","name":"p","steps":[]}"#.into()),
            file("routes.json", serde_json::Value::from(routes).to_string()),
        ];
        let actor = smeltr::leader::store::Actor::system();
        let smeltr::leader::git::Committed::At(commit) =
            smeltr::leader::git::commit_edits(&repo, "main", &parent, Some(&parent), &edits, "sources", &actor)
                .map_err(|e| e.to_string())?
        else {
            return Err("the commit did not land".to_string());
        };
        smeltr::leader::git::deploy_commit(&self.data_dir, &commit, &actor)
            .map(drop)
            .map_err(|e| e.to_string())
    }

    /// The incarnation its worker reports, once it reports one at all.
    fn incarnation(&self) -> Option<u64> {
        self.worker()?["supervising"]["incarnation"].as_u64()
    }

    /// What its worker last reported, once it has reported a runtime.
    fn worker(&self) -> Option<serde_json::Value> {
        let mut resp = self
            .agent
            .get(format!("{}/api/cluster", self.base))
            .header("cookie", &self.session)
            .call()
            .ok()?;
        let parsed: serde_json::Value = resp.body_mut().read_json().ok()?;
        parsed["health"]["threads"]
            .as_array()?
            .iter()
            .find(|t| t["supervising"]["incarnation"].is_u64())
            .cloned()
    }

    /// The incarnation its worker reports running, and the process running it, once both are there.
    fn running_runtime(&self, within: Duration) -> Option<(u64, u32)> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let (Some(n), Some(pid)) = (self.incarnation(), runtime_child_of(self.process.id())) {
                return Some((n, pid));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        None
    }

    fn said(&self) -> String {
        self.console.lock().unwrap().clone()
    }

    /// Ends it, whatever state the test left it in, and panics with its console when `outcome` is a failure.
    fn finish(mut self, outcome: Result<(), String>) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let Err(why) = outcome {
            panic!("{why}\nsmeltr standalone console:\n{}", self.said());
        }
    }
}

/// A runtime dying is recovered from without touching the worker, and in all-in-one without touching the leader.
///
/// The runtime is killed outright rather than asked, which is the shape every unrecoverable stall ends in: the
/// supervisor notices a process that is gone, starts another, and says so on its next beat. The leader is in this
/// same process, so it answering afterward is the other half of what is being asserted.
#[test]
#[cfg(unix)]
fn a_runtime_that_dies_is_replaced_without_restarting_the_worker_or_the_leader() {
    let standalone = Standalone::start("runtime-recovers");
    let outcome = (|| {
        let (first, doomed) = standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;

        // Killed outright: no drain, no acknowledgment, nothing the supervisor can ask for.
        unsafe { libc::kill(doomed as libc::pid_t, libc::SIGKILL) };

        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            // The leader is in this process too, and it just answered.
            if standalone.incarnation().is_some_and(|now| now > first) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(format!(
            "the worker never replaced the runtime it lost (still incarnation {first})"
        ))
    })();
    standalone.finish(outcome);
}

/// A commit that loads but will not start costs nothing that was running: the worker starts again what it ran
/// before, reports why the new commit did not start, and stays there rather than stopping it to try again.
#[test]
#[cfg(unix)]
fn a_commit_that_will_not_start_leaves_the_worker_on_what_it_ran_before() {
    let standalone = Standalone::start("restores-last-good");
    let outcome = (|| {
        let (first, _) = standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;
        let good = standalone.worker().ok_or("no worker")?["commit"]
            .as_str()
            .ok_or("the worker reported no commit")?
            .to_string();

        // Loads, and fails only when the destination's client is built: the CA file is on the worker's host.
        let repo = smeltr::leader::git::repo_path(&standalone.data_dir);
        let parent = smeltr::leader::git::main_head(&repo).ok_or("main has no commit")?;
        let file = |path: &str, content: &str| smeltr::leader::git::FileEdit {
            path: format!("groups/standalone/{path}"),
            content: Some(content.as_bytes().to_vec()),
        };
        let edits = [
            file("system.json", r#"{"version":1}"#),
            file("sources.json", r#"[{"id":"m","type":"metrics","bucketSeconds":1}]"#),
            file(
                "destinations.json",
                r#"[{"id":"d","type":"kafka","brokers":"127.0.0.1:1","defaultTopic":"t",
                     "properties":{"security.protocol":"SSL","ssl.ca.location":"/nonexistent/ca.pem"}}]"#,
            ),
            file("pipelines/p.json", r#"{"id":"p","name":"p","steps":[]}"#),
            file(
                "routes.json",
                r#"[{"id":"r","source":"m","pipeline":"p","deliver":[{"to":["d"]}]}]"#,
            ),
        ];
        let actor = smeltr::leader::store::Actor::system();
        let smeltr::leader::git::Committed::At(bad) =
            smeltr::leader::git::commit_edits(&repo, "main", &parent, Some(&parent), &edits, "will not start", &actor)
                .map_err(|e| e.to_string())?
        else {
            return Err("the commit did not land".to_string());
        };
        smeltr::leader::git::deploy_commit(&standalone.data_dir, &bad, &actor).map_err(|e| e.to_string())?;

        let deadline = Instant::now() + Duration::from_secs(60);
        let restored = loop {
            let worker = standalone.worker();
            if let Some(w) = &worker
                && w["commit"].as_str() == Some(good.as_str())
                && w["supervising"]["incarnation"].as_u64().is_some_and(|n| n > first + 1)
                && w["supervising"]["phase"].as_str() == Some("running")
            {
                break w.clone();
            }
            if Instant::now() > deadline {
                return Err(format!("the worker never went back to {good}: {worker:#?}"));
            }
            std::thread::sleep(Duration::from_millis(250));
        };
        let failure = restored["supervising"]["failure"].as_str().unwrap_or_default();
        if !failure.contains("ssl.ca.location") {
            return Err(format!("the worker should say why {bad} did not start: {restored:#}"));
        }

        // Not tried again on a timer: each try would stop the deployment that works.
        let settled = restored["supervising"]["incarnation"].as_u64();
        std::thread::sleep(Duration::from_secs(5));
        if standalone.incarnation() != settled {
            return Err(format!(
                "the worker stopped what it went back to: now {:?}",
                standalone.incarnation()
            ));
        }
        Ok(())
    })();
    standalone.finish(outcome);
}

/// SIGTERM ends a worker's runtime before it ends the worker: the drain, the last commit and the leave from each
/// consumer group all happen while the supervisor waits.
///
/// A supervisor that went first left its runtime to find out from a closed pipe. In a container the supervisor is
/// the first process, and its exit takes every other process in the container with it, so that runtime got no
/// drain at all.
#[test]
#[cfg(unix)]
fn a_signaled_worker_stops_its_runtime_before_it_exits() {
    let mut standalone = Standalone::start("signaled-worker");
    let outcome = (|| {
        let (_, runtime) = standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;

        unsafe { libc::kill(standalone.process.id() as libc::pid_t, libc::SIGTERM) };

        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = standalone.process.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("the worker did not exit within a minute of SIGTERM".to_string());
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        // Asked with signal 0, which delivers nothing: a runtime still there -- an orphan draining alone, or one
        // nobody reaped -- answers it.
        if unsafe { libc::kill(runtime as libc::pid_t, 0) } == 0 {
            return Err(format!("the runtime ({runtime}) outlived the worker that ran it"));
        }
        if !status.success() {
            return Err(format!("the worker exited {status} on SIGTERM"));
        }
        if !standalone.said().contains("worker.stopped_for_exit") {
            return Err("the worker never said its runtime had stopped".to_string());
        }
        Ok(())
    })();
    standalone.finish(outcome);
}

/// A runtime signaled on its own drains as its supervisor's `Stop` would have it, and is replaced like any runtime
/// that ended. A service manager that signals every process in a unit, as systemd does by default, reaches the
/// runtime this way before its supervisor has asked it anything.
#[test]
#[cfg(unix)]
fn a_signaled_runtime_drains_and_is_replaced() {
    let standalone = Standalone::start("signaled-runtime");
    let outcome = (|| {
        let (first, runtime) = standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;

        unsafe { libc::kill(runtime as libc::pid_t, libc::SIGTERM) };

        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if standalone.incarnation().is_some_and(|now| now > first) {
                return if standalone.said().contains("runtime.stopping_on_signal") {
                    Ok(())
                } else {
                    Err("the runtime ended on SIGTERM without draining".to_string())
                };
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Err(format!(
            "the worker never replaced the runtime that was signaled (still incarnation {first})"
        ))
    })();
    standalone.finish(outcome);
}

/// A service on `ip` that takes every POST, keeps its body, and answers 200, on as many connections as it is sent.
#[cfg(unix)]
fn collector(ip: &str) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind((ip.parse::<std::net::IpAddr>().unwrap(), 0)).unwrap();
    let url = format!("http://{}/intake", listener.local_addr().unwrap());
    let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let kept = bodies.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().map_while(Result::ok) {
            let kept = kept.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                loop {
                    let mut length = 0usize;
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                            length = v.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0; length];
                    if reader.read_exact(&mut body).is_err() {
                        return;
                    }
                    kept.lock().unwrap().push(String::from_utf8_lossy(&body).to_string());
                    if reader
                        .get_mut()
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    (url, bodies)
}

/// One POST to a receiver, its status code. `None` when the connection was refused or cut off.
#[cfg(unix)]
fn post(at: std::net::SocketAddr, token: &str, body: &str) -> Option<u16> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(at).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(90))).unwrap();
    write!(
        stream,
        "POST / HTTP/1.1\r\nHost: r\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer).ok()?;
    answer.get(9..12)?.parse().ok()
}

/// A receiver's port outlives the runtime serving it: a sender that connects while no runtime runs waits in the
/// queue of the socket the supervisor holds, and the runtime that replaces the lost one answers it.
#[test]
#[cfg(unix)]
fn a_receiver_answers_a_request_sent_while_its_runtime_was_being_replaced() {
    // The worker's address, so the socket the supervisor holds is the one its setting names.
    let reach = Reach {
        receivers: Some("127.0.0.1"),
        ..Reach::LOOPBACK
    };
    let standalone = Standalone::start_reaching("receiver-handover", &reach);
    let outcome = (|| {
        standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;
        let (intake, bodies) = collector("127.0.0.1");
        let at = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        // The token is a variable the worker already has, so the test needs no secret of its own.
        standalone.deploy(
            &[format!(
                r#"{{"id":"hook","type":"httpReceiver","port":{},
                     "tokens":{{"t":"SMELTR_WORKER_GROUP"}}}}"#,
                at.port()
            )],
            &intake,
        )?;

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if post(at, "standalone", "before") == Some(200) {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!("the receiver never answered: {:#?}", standalone.worker()));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        if std::net::TcpListener::bind("[::1]:0").is_ok() {
            let elsewhere = std::net::SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), at.port());
            if let Some(status) = post(elsewhere, "standalone", "elsewhere") {
                return Err(format!(
                    "the receiver answered {status} off the worker's listen address"
                ));
            }
        }

        let (first, runtime) = standalone
            .running_runtime(Duration::from_secs(10))
            .ok_or("the worker reported no runtime")?;
        unsafe { libc::kill(runtime as libc::pid_t, libc::SIGKILL) };
        while runtime_child_of(standalone.process.id()) == Some(runtime) {
            std::thread::sleep(Duration::from_millis(10));
        }
        // Sent with no runtime running: it connects, and waits.
        let answered = post(at, "standalone", "during");
        if answered != Some(200) {
            return Err(format!("a request sent between runtimes was answered {answered:?}"));
        }
        // The killed runtime was gone before the request was sent, so the one that answered is its replacement;
        // the leader hears of it on the worker's next beat.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !standalone.incarnation().is_some_and(|now| now > first) {
            if Instant::now() > deadline {
                return Err(format!("the worker never reported a runtime after incarnation {first}"));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        let delivered = bodies.lock().unwrap().join("\n");
        if !delivered.contains("before") || !delivered.contains("during") {
            return Err(format!("the destination took {delivered:?}"));
        }
        Ok(())
    })();
    standalone.finish(outcome);
}

/// A service on `ip` that answers every request with `body`.
#[cfg(unix)]
fn steady(ip: &str, body: &'static str) -> std::net::SocketAddr {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind((ip.parse::<std::net::IpAddr>().unwrap(), 0)).unwrap();
    let at = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().map_while(Result::ok) {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|n| n > 0) && line != "\r\n" {
                line.clear();
            }
            let _ = write!(
                reader.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    at
}

/// Whether this host can listen on IPv6 loopback. A test that needs it says it was skipped when not.
#[cfg(unix)]
fn has_ipv6(test: &str) -> bool {
    let has = std::net::TcpListener::bind("[::1]:0").is_ok();
    if !has {
        eprintln!("{test}: skipped, this host has no IPv6");
    }
    has
}

/// A worker reaches its leader as `reach` says, pulls from a service on `pull`, takes a push from each of `push`
/// on its receiver's default address, and delivers what it read to a service on `sink`.
#[cfg(unix)]
fn a_worker_pulls_and_is_pushed_to(name: &str, reach: &Reach, pull: &str, sink: &str, push: &[&str]) {
    let standalone = Standalone::start_reaching(name, reach);
    let outcome = (|| {
        standalone
            .running_runtime(Duration::from_secs(40))
            .ok_or("the worker never reported a running runtime")?;
        let (intake, bodies) = collector(sink);
        let pulled = steady(pull, "pulled\n");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        // No `SMELTR_WORKER_LISTEN_ADDRESS`: the worker's own default is what is under test.
        standalone.deploy(
            &[
                format!(r#"{{"id":"pulled","type":"httpApi","url":"http://{pulled}/","records":"lines","pollIntervalSecs":1}}"#),
                format!(
                    r#"{{"id":"pushed","type":"httpReceiver","port":{port},"records":"lines",
                         "tokens":{{"t":"SMELTR_WORKER_GROUP"}}}}"#
                ),
            ],
            &intake,
        )?;

        for from in push {
            let at = std::net::SocketAddr::new(from.parse().unwrap(), port);
            let deadline = Instant::now() + Duration::from_secs(60);
            while post(at, "standalone", &format!("pushed over {from}\n")) != Some(200) {
                if Instant::now() > deadline {
                    return Err(format!(
                        "the receiver never answered over {from}: {:#?}",
                        standalone.worker()
                    ));
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let delivered = bodies.lock().unwrap().join("\n");
            if delivered.contains("pulled")
                && push
                    .iter()
                    .all(|from| delivered.contains(&format!("pushed over {from}")))
            {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("the destination took {delivered:?}"));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    })();
    standalone.finish(outcome);
}

#[test]
#[cfg(unix)]
fn a_worker_pulls_and_is_pushed_to_over_ipv4() {
    a_worker_pulls_and_is_pushed_to("ipv4", &Reach::LOOPBACK, "127.0.0.1", "127.0.0.1", &["127.0.0.1"]);
}

/// The leader listens on `[::]`: the test reaches it over IPv4 and its worker over IPv6, and the receiver takes both.
#[test]
#[cfg(unix)]
fn a_worker_pulls_and_is_pushed_to_dual_stack() {
    if !has_ipv6("dual_stack") {
        return;
    }
    let reach = Reach {
        bind: "::",
        worker: Some("[::1]"),
        test: "127.0.0.1",
        receivers: None,
    };
    a_worker_pulls_and_is_pushed_to("dual", &reach, "::1", "127.0.0.1", &["127.0.0.1", "::1"]);
}

/// Every address is IPv6: the leader, what the worker pulls from and delivers to, and its receiver's sender.
#[test]
#[cfg(unix)]
fn a_worker_pulls_and_is_pushed_to_over_ipv6() {
    if !has_ipv6("over_ipv6") {
        return;
    }
    let reach = Reach {
        bind: "::1",
        worker: None,
        test: "[::1]",
        receivers: None,
    };
    a_worker_pulls_and_is_pushed_to("ipv6", &reach, "::1", "::1", &["::1"]);
}

/// A leader that does not listen on `127.0.0.1` is still where its own worker, told nothing, calls it.
#[test]
#[cfg(unix)]
fn a_worker_told_nothing_reaches_a_leader_off_127_0_0_1() {
    // Linux answers on all of 127.0.0.0/8; a host that does not has nothing to test here.
    if std::net::TcpListener::bind("127.0.0.2:0").is_err() {
        eprintln!("off_127_0_0_1: skipped, this host does not listen on 127.0.0.2");
        return;
    }
    let reach = Reach {
        bind: "127.0.0.2",
        worker: None,
        test: "127.0.0.2",
        receivers: None,
    };
    a_worker_pulls_and_is_pushed_to("off-loopback-one", &reach, "127.0.0.1", "127.0.0.1", &["127.0.0.1"]);
}

/// A worker-only process, not an all-in-one one: `SMELTR_LEADER_ENABLED` is unset, so nothing in this process ever
/// reaches `run_leader`'s startup. Every other TLS-over-HTTPS worker test in this file sets both roles and so
/// runs the leader's own startup path in the same process, which installs rustls's process-wide crypto provider --
/// masking a worker-only process that never does. This is the one process where a worker's fetch over HTTPS
/// really is the first thing in the process asking rustls to build a TLS client.
#[test]
fn a_standalone_worker_fetches_over_https_with_no_leader_role_in_its_own_process() {
    let (base, ca_pem, data_dir, _leader) = start_leader_tls();
    let agent = admin_agent(&ca_pem);

    let group_id = "https-fetch-only";
    let bundle: smeltr::config_file::FileConfig = serde_json::from_str(r#"{"version":1}"#).expect("an empty bundle");
    let deployed = deploy_to_group(&data_dir, group_id, &bundle);

    let code: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(serde_json::json!({}))
        .expect("minting an enrollment code succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON");
    let enroll_code = code["code"]["value"].as_str().expect("ok implies an enrollment code");

    let ca_path = temp_dir("https-only-ca").join("leader-ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();
    let state_dir = temp_dir("https-only-state");

    let mut worker = Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_LEADER_URL", &base)
        .env("SMELTR_LEADER_CA", ca_path.to_str().unwrap())
        .env("SMELTR_WORKER_GROUP", group_id)
        .env("SMELTR_WORKER_ENROLLMENT_CODE", enroll_code)
        .env("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap())
        .env("SMELTR_WORKER_THREAD_LIMIT", "1")
        .env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", "500")
        .current_dir(&state_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr in its worker role, with no leader role in this process");

    let console = Arc::new(std::sync::Mutex::new(String::new()));
    let sink = console.clone();
    let pipe = worker.stderr.take().expect("stderr is piped");
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut held = sink.lock().unwrap();
            held.push_str(&line);
            held.push('\n');
        }
    });

    let reported = || -> Option<String> {
        let mut resp = agent.get(format!("{base}/api/health")).call().ok()?;
        let parsed: serde_json::Value = resp.body_mut().read_json().ok()?;
        parsed["threads"]
            .as_array()?
            .iter()
            .find_map(|t| t["commit"].as_str())
            .map(str::to_string)
    };

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut commit = None;
    while Instant::now() < deadline {
        if let Some(seen) = reported() {
            commit = Some(seen);
            break;
        }
        if let Ok(Some(status)) = worker.try_wait() {
            panic!(
                "the worker process exited early with {status:?}.\nworker console:\n{}",
                console.lock().unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let last_health = agent
        .get(format!("{base}/api/health"))
        .call()
        .ok()
        .and_then(|mut r| r.body_mut().read_json::<serde_json::Value>().ok());
    let _ = worker.kill();
    let _ = worker.wait();
    let Some(commit) = commit else {
        panic!(
            "the worker never reported a commit -- its fetch over HTTPS never landed.\nlast /api/health: \
             {last_health:#?}\nworker console:\n{}",
            console.lock().unwrap()
        );
    };
    assert_eq!(
        commit, deployed,
        "the worker's own HTTPS fetch, in a process where nothing else touches rustls first, still lands the \
         commit the leader deployed"
    );
}
