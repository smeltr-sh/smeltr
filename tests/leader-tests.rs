//! `smeltr::leader` over a real socket: a background thread runs the axum
//! server and each test talks to it with blocking `ureq`, the way a worker
//! does. Every worker-facing call goes through the real enroll-then-
//! authenticate flow.

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

use smeltr::heartbeat::{HeartbeatClient, HeartbeatRequest, MetricSampleDto, PartitionRef, ThreadHeartbeat};
use smeltr::leader::registry::Registry;
use smeltr::leader::sqlite_store::SqliteStore;
use smeltr::leader::{AppState, router};

/// A path under the temp directory that no other test in this binary can
/// name, for a `data_dir` or a clone destination.
///
/// A counter, not the clock: these tests run in parallel and share a pid, so
/// `SystemTime::now()` was the only thing keeping two of them apart -- and on
/// Windows it advances in ~15 ms steps, not nanoseconds. Two leaders that
/// started inside one tick got the same `data_dir`, and then raced on
/// `git init --bare` for the same worker group; the loser's group was never
/// created, which surfaced much later as an empty group list.
fn scratch_dir(prefix: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("{prefix}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A leader's state over `store`, as `main` builds it, for a server a test runs itself.
fn app_state(store: SqliteStore, data_dir: std::path::PathBuf) -> AppState {
    AppState {
        store: Arc::new(store),
        registry: Arc::new(Registry::default()),
        throughput: Default::default(),
        captures: Arc::new(smeltr::leader::capture::Captures::default()),
        entitlements: Arc::new(smeltr::leader::Entitlements::default()),
        verify_limiter: Arc::new(smeltr::leader::VerifyLimiter::default()),
        expiries: Default::default(),
        deploy_checks: Default::default(),
        origin_hosts: Vec::new(),
        remote: Default::default(),
        data_dir,
        directives: Default::default(),
    }
}

/// A leader on an OS-assigned port, with an in-memory store and a real
/// temp `data_dir` for git. Returns its base URL.
fn start_leader() -> String {
    start_leader_with_data_dir().0
}

/// `start_leader`, also handing back the `data_dir`, so a test can land a
/// commit on `main` the way a deploy does.
fn start_leader_with_data_dir() -> (String, std::path::PathBuf) {
    let data_dir = scratch_dir("smeltr-leader-test");
    std::fs::create_dir_all(&data_dir).unwrap();
    let returned = data_dir.clone();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = SqliteStore::open(
                std::path::Path::new(":memory:"),
                &scratch_dir("smeltr-leader-key").join("sealing.key"),
            )
            .unwrap();
            let state = app_state(store, data_dir);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr.port()).unwrap();
            axum::serve(listener, router(state)).await.unwrap();
        });
    });
    let port = rx.recv().expect("leader thread reports its bound port");
    (format!("http://127.0.0.1:{port}"), returned)
}

/// A client that connects and never finishes its request's headers is cut off, over the plain listener `main`
/// builds the same way. Without the timer the leader hands hyper, hyper's own header timeout is no timeout, and
/// such a client holds its connection, its task and its descriptor until the leader stops.
#[test]
fn a_client_that_never_finishes_its_headers_is_cut_off() {
    use std::io::{Read, Write};
    let data_dir = scratch_dir("smeltr-leader-test-slow");
    std::fs::create_dir_all(&data_dir).unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = SqliteStore::open(
                std::path::Path::new(":memory:"),
                &scratch_dir("smeltr-leader-slow-key").join("sealing.key"),
            )
            .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            let mut server = axum_server::from_tcp(listener).unwrap();
            smeltr::leader::listen::bound(server.http_builder(), std::time::Duration::from_secs(1));
            server
                .serve(router(app_state(store, data_dir)).into_make_service())
                .await
                .unwrap();
        });
    });
    let port = rx.recv().expect("the server reports its bound port");
    let mut slow = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    slow.write_all(b"GET /api/health HTTP/1.1\r\nHost: leader\r\n").unwrap();
    slow.set_read_timeout(Some(std::time::Duration::from_secs(15))).unwrap();
    let started = std::time::Instant::now();
    let mut answer = Vec::new();
    // The server either closes the connection or answers that it timed out, then closes it.
    slow.read_to_end(&mut answer).expect("the server ends the connection");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "cut off only after {:?}",
        started.elapsed()
    );
    // The listener still serves a client that finishes.
    let mut quick = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    quick
        .write_all(b"GET /api/health HTTP/1.1\r\nHost: leader\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut answer = String::new();
    quick.read_to_string(&mut answer).unwrap();
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
}

/// `start_leader` over TLS, with the certificate the leader really serves: one its own authority signed
/// (`leader::tls`). Returns the base URL and the authority PEM a worker is given.
fn start_leader_tls() -> (String, Vec<u8>) {
    static INSTALL_CRYPTO_PROVIDER: std::sync::Once = std::sync::Once::new();
    INSTALL_CRYPTO_PROVIDER.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install rustls crypto provider");
    });

    let data_dir = scratch_dir("smeltr-leader-test-tls");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Twice, and the authority a worker is given is the one from the first call while the certificate served is
    // the one from the second. Every TLS test below then stands on the property the authority exists for: a
    // certificate minted after a worker was handed `ca.pem` is one that worker still accepts.
    let names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let tls_dir = data_dir.join("tls");
    let validity = std::time::Duration::from_secs(60 * 60);
    let first = smeltr::leader::tls::ensure(&tls_dir, names.clone(), validity, &[]).expect("an authority, created");
    let ca_pem = first.ca_pem.clone();
    let issued_first = std::fs::read(&first.server_cert_path).unwrap();

    let materials = smeltr::leader::tls::ensure(&tls_dir, names, validity, &[]).expect("a certificate, reissued");
    // What `main` says once it has built the socket from these files: it has what is on disk.
    smeltr::leader::tls::socket_took(&tls_dir, smeltr::leader::tls::disk_generation(&tls_dir));
    assert_eq!(materials.ca_pem, ca_pem, "the authority is kept across starts");
    let cert_pem = std::fs::read(&materials.server_cert_path).unwrap();
    assert_ne!(cert_pem, issued_first, "the served certificate is a new one");
    let key_pem = std::fs::read(&materials.server_key_path).unwrap();

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = SqliteStore::open(
                std::path::Path::new(":memory:"),
                &scratch_dir("smeltr-leader-flow-key").join("sealing.key"),
            )
            .unwrap();
            let state = app_state(store, data_dir);
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem, key_pem)
                .await
                .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            tx.send(addr.port()).unwrap();
            axum_server::tls_rustls::from_tcp_rustls(listener, tls)
                .unwrap()
                .serve(router(state).into_make_service())
                .await
                .unwrap();
        });
    });
    let port = rx.recv().expect("leader thread reports its bound port");
    let base = format!("https://127.0.0.1:{port}");
    // The port is reported once the socket is bound, which is before `serve` is running: a connection arriving in
    // that window sits in the backlog and its TLS handshake finds nobody to complete it. Answering one request is
    // what says the server is up, and under a loaded machine that is a wait worth making.
    wait_until_serving(&base, &ca_pem);
    (base, ca_pem)
}

/// Poll until the leader answers over TLS, or panic with how long it was given.
fn wait_until_serving(base: &str, ca_pem: &[u8]) {
    let agent = smeltr::heartbeat::agent_trusting(Some(ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if agent.get(format!("{base}/api/health")).call().is_ok() {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{base} never answered");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn mint_code(base: &str) -> String {
    mint_code_with(&ureq::Agent::new_with_defaults(), base, serde_json::json!({}))
}

fn mint_code_with(agent: &ureq::Agent, base: &str, body: serde_json::Value) -> String {
    let r: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(body)
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    assert!(r["ok"].as_bool().unwrap_or(false), "{r}");
    r["code"]["value"]
        .as_str()
        .expect("ok implies an enrollment code")
        .to_string()
}

#[derive(serde::Deserialize)]
struct EnrollResult {
    ok: bool,
    token: Option<String>,
}

fn try_enroll(base: &str, code: &str, worker_id: &str) -> EnrollResult {
    ureq::post(format!("{base}/api/enroll"))
        .send_json(serde_json::json!({ "code": code, "workerId": worker_id }))
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response")
}

/// Mint a code, then exchange it for a token: a worker's first run.
fn enroll_worker(base: &str, worker_id: &str) -> String {
    let r = try_enroll(base, &mint_code(base), worker_id);
    assert!(r.ok);
    r.token.expect("ok implies a token")
}

fn revoke_worker(base: &str, worker_id: &str) -> MutationResult {
    ureq::post(format!("{base}/api/workers/{worker_id}/revoke"))
        .send_json(serde_json::json!({}))
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response")
}

/// A heartbeat reporting `worker_id` and one thread, answered with its status.
/// The thread is what makes a wrongly accepted claim leave a registry entry to
/// find; `worker_id` is a parameter because the leader now checks it against
/// the token.
fn heartbeat_status(base: &str, token: &str, worker_id: &str) -> u16 {
    match ureq::post(format!("{base}/api/heartbeat"))
        .header("Authorization", &format!("Bearer {token}"))
        .send_json(serde_json::json!({
            "workerId": worker_id,
            "threads": [{
                "threadId": "t0",
                "groupId": "pool",
                "partitionsOwned": [],
                "metrics": [],
            }],
        })) {
        Ok(r) => r.status().as_u16(),
        Err(ureq::Error::StatusCode(c)) => c,
        Err(other) => panic!("unexpected transport error: {other}"),
    }
}

fn heartbeat_one_thread(
    base: &str,
    token: &str,
    worker_id: &str,
    thread_id: &str,
    group_id: &str,
) -> smeltr::heartbeat::HeartbeatResponse {
    heartbeat_one_thread_with_policy(base, token, worker_id, thread_id, group_id, None, None)
}

fn heartbeat_one_thread_with_policy(
    base: &str,
    token: &str,
    worker_id: &str,
    thread_id: &str,
    group_id: &str,
    stop_after_unreachable_secs: Option<u64>,
    commit: Option<&str>,
) -> smeltr::heartbeat::HeartbeatResponse {
    let client = HeartbeatClient::new(base, token, None, true).expect("no authority needed over plain HTTP");
    client
        .send(&HeartbeatRequest {
            heartbeat_interval_ms: None,
            single_worker: Vec::new(),
            progress: Vec::new(),
            streams: None,
            streams_window: None,
            stopping: false,
            authority_fingerprints: Vec::new(),
            takes_authority: true,
            logs: Vec::new(),
            boot_id: 0,
            group_id: "prod".to_string(),
            secret_versions: Vec::new(),
            supervising: None,
            resources: None,
            worker_id: worker_id.to_string(),
            stop_after_unreachable_secs,
            commit: commit.map(str::to_string),
            smeltr_version: String::new(),
            release: None,
            authority_expires_millis: None,
            threads: vec![ThreadHeartbeat {
                thread_id: thread_id.to_string(),
                group_id: group_id.to_string(),
                kind: Some(smeltr::heartbeat::ThreadKind::Reader),
                partitions_owned: vec![PartitionRef {
                    topic: "orders".to_string(),
                    partition: 0,
                }],
                metrics: vec![MetricSampleDto {
                    metric: "smeltr_rows_polled_total".to_string(),
                    labels: vec![],
                    value: smeltr::heartbeat::MetricValueDto::Counter(3),
                }],
            }],
            accepts: Vec::new(),
            took: None,
        })
        .expect("heartbeat succeeds")
}

#[derive(serde::Deserialize)]
struct MutationResult {
    ok: bool,
}

/// Land a bundle on `main` for one group, the way a deploy does. This is also what makes the group exist
/// (`groups/<id>/system.json` is always among `bundle_files`' output) -- creating one and configuring it are HTTP
/// calls this router does not serve (`config_api`/`worker_groups_api`, binary-crate only; see `leader::mod` docs),
/// so the tests here land the commit directly, as `main`'s head is what a worker actually reads. Returns the commit
/// -- what every worker then runs.
fn commit_for_group(data_dir: &std::path::Path, group_id: &str, source_id: Option<&str>) -> String {
    let json = match source_id {
        None => r#"{"version":1}"#.to_string(),
        Some(id) => format!(r#"{{"version":1,"sources":[{{"id":{id:?},"type":"metrics","bucketSeconds":10}}]}}"#),
    };
    smeltr::config_file::load_str(&json).expect("a valid bundle");
    let file: smeltr::config_file::FileConfig = serde_json::from_str(&json).expect("parses");
    let repo = smeltr::leader::git::repo_path(data_dir);
    smeltr::leader::git::init_bare_repo(&repo).expect("a repository");
    smeltr::leader::git::ensure_main_commit(&repo).expect("main");
    let parent = smeltr::leader::git::main_head(&repo).expect("main's head");
    let edits: Vec<smeltr::leader::git::FileEdit> = smeltr::config_file::bundle_files(&file)
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

/// A worker given only the authority reaches a leader whose certificate it has never seen, which is the whole
/// point of the authority: the certificate is signed by it, not handed out with it.
#[test]
fn a_worker_trusting_the_authority_reaches_the_leader_it_signed_for() {
    let (base, ca_pem) = start_leader_tls();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();

    let code = mint_code_with(&agent, &base, serde_json::json!({}));
    let enrolled: EnrollResult = agent
        .post(format!("{base}/api/enroll"))
        .send_json(serde_json::json!({ "code": code, "workerId": "w1" }))
        .expect("enroll request succeeds over the authority's TLS")
        .body_mut()
        .read_json()
        .unwrap();
    let token = enrolled.token.unwrap();

    let client = HeartbeatClient::new(&base, &token, Some(&ca_pem), true)
        .expect("HeartbeatClient accepts the leader's authority");
    client
        .send(&HeartbeatRequest {
            heartbeat_interval_ms: None,
            single_worker: Vec::new(),
            progress: Vec::new(),
            streams: None,
            streams_window: None,
            stopping: false,
            authority_fingerprints: Vec::new(),
            takes_authority: true,
            logs: Vec::new(),
            boot_id: 0,
            group_id: "prod".to_string(),
            secret_versions: Vec::new(),
            supervising: None,
            resources: None,
            worker_id: "w1".to_string(),
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            authority_expires_millis: None,
            threads: vec![ThreadHeartbeat {
                thread_id: "t0".to_string(),
                group_id: "pool".to_string(),
                kind: Some(smeltr::heartbeat::ThreadKind::Reader),
                partitions_owned: vec![],
                metrics: vec![],
            }],
            accepts: Vec::new(),
            took: None,
        })
        .expect("heartbeat verified against the authority succeeds");
}

/// Replacing an authority is only possible without stopping the fleet if a worker can hold two at once: the one
/// its leader runs today and the one it will run tomorrow. Here the leader's own is the second of the pair.
#[test]
fn a_worker_holding_two_authorities_reaches_a_leader_that_runs_either() {
    let (base, ca_pem) = start_leader_tls();

    let elsewhere = smeltr::leader::tls::ensure(
        &scratch_dir("smeltr-leader-test-other-ca").join("tls"),
        vec!["localhost".to_string()],
        std::time::Duration::from_secs(60 * 60),
        &[],
    )
    .expect("an authority this leader does not run");

    let mut both = elsewhere.ca_pem.clone();
    both.extend_from_slice(&ca_pem);
    let agent = smeltr::heartbeat::agent_trusting(Some(&both), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    agent
        .get(format!("{base}/api/health"))
        .call()
        .expect("a worker holding both authorities reaches the leader signed for by one of them");

    let wrong =
        smeltr::heartbeat::agent_trusting(Some(&elsewhere.ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    assert!(
        wrong.get(format!("{base}/api/health")).call().is_err(),
        "holding only the other authority must not be enough"
    );
}

/// A capture rides a second agent, sized for a body a heartbeat's budget could not carry. It is the same client
/// and the same leader, so it trusts the same pinned certificate: an agent built without it would reach a
/// self-signed leader on no call at all.
#[test]
fn a_capture_report_over_tls_trusts_the_same_pinned_cert() {
    let (base, cert_pem) = start_leader_tls();

    let tls_config = ureq::tls::TlsConfig::builder()
        .root_certs(ureq::tls::RootCerts::Specific(std::sync::Arc::new(vec![
            ureq::tls::Certificate::from_pem(&cert_pem).unwrap(),
        ])))
        .build();
    let agent = ureq::Agent::new_with_config(ureq::Agent::config_builder().tls_config(tls_config).build());
    let code = mint_code_with(&agent, &base, serde_json::json!({}));
    let enrolled: EnrollResult = agent
        .post(format!("{base}/api/enroll"))
        .send_json(serde_json::json!({ "code": code, "workerId": "w1" }))
        .expect("enroll request succeeds over pinned TLS")
        .body_mut()
        .read_json()
        .unwrap();

    let client = HeartbeatClient::new(&base, enrolled.token.unwrap(), Some(&cert_pem), true)
        .expect("HeartbeatClient accepts the pinned cert");
    client
        .report_capture(&smeltr::heartbeat::CaptureReport {
            session: "s1".to_string(),
            worker_id: "w1".to_string(),
            batches: vec![smeltr::heartbeat::CaptureBatch {
                columns: vec![smeltr::heartbeat::CaptureColumn {
                    name: "_raw".to_string(),
                    ty: "str".to_string(),
                }],
                rows: vec![smeltr::heartbeat::CapturedRow {
                    values: vec![serde_json::Value::String("x".repeat(3 * 1024 * 1024))],
                    failed: vec![],
                    worker: None,
                }],
            }],
            finished: Some(smeltr::heartbeat::CaptureEnd::Bytes),
            sql: None,
            avro: Vec::new(),
        })
        .expect("a capture over pinned TLS succeeds");
}

#[test]
fn heartbeat_client_without_the_authority_rejects_the_leader() {
    let (base, _ca_pem) = start_leader_tls();
    let client = HeartbeatClient::new(&base, "some-token", None, true).unwrap();
    let result = client.send(&HeartbeatRequest {
        heartbeat_interval_ms: None,
        single_worker: Vec::new(),
        progress: Vec::new(),
        streams: None,
        streams_window: None,
        stopping: false,
        authority_fingerprints: Vec::new(),
        takes_authority: true,
        logs: Vec::new(),
        boot_id: 0,
        group_id: "prod".to_string(),
        secret_versions: Vec::new(),
        supervising: None,
        resources: None,
        worker_id: "w1".to_string(),
        stop_after_unreachable_secs: None,
        commit: None,
        smeltr_version: String::new(),
        release: None,
        authority_expires_millis: None,
        threads: vec![],
        accepts: Vec::new(),
        took: None,
    });
    assert!(
        result.is_err(),
        "a client holding no authority must not trust a leader that runs its own"
    );
}

/// What a worker is running is the worker's own answer. The leader knows where `main` is, but a worker that
/// refused a commit keeps running an older one, so only its own report says which.
#[test]
fn a_heartbeat_reports_the_commit_the_worker_is_running() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");

    heartbeat_one_thread_with_policy(&base, &token, "w1", "t0", "pool", None, Some("c0ffee"));

    let health: HealthResponse = ureq::get(format!("{base}/api/health"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    assert_eq!(health.threads[0].get("commit").and_then(|v| v.as_str()), Some("c0ffee"));
}

/// A worker running nothing reports no commit, and the leader must not fill one in from `main`: "running nothing"
/// and "running what everyone else is" are the two states this field exists to tell apart.
#[test]
fn a_worker_running_nothing_reports_no_commit() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");

    heartbeat_one_thread(&base, &token, "w1", "t0", "pool");

    let health: HealthResponse = ureq::get(format!("{base}/api/health"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    assert_eq!(health.threads[0].get("commit").and_then(|v| v.as_str()), None);
}

/// Two workers in the same group may disagree, since each is its own process with its own `smeltr.toml`.
#[test]
fn two_workers_in_one_group_can_report_different_policies() {
    let base = start_leader();
    let w1 = enroll_worker(&base, "w1");
    let w2 = enroll_worker(&base, "w2");

    heartbeat_one_thread_with_policy(&base, &w1, "w1", "t0", "pool", Some(300), None);
    heartbeat_one_thread_with_policy(&base, &w2, "w2", "t0", "pool", None, None);

    let health: HealthResponse = ureq::get(format!("{base}/api/health"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    let for_worker = |id: &str| {
        health
            .threads
            .iter()
            .find(|t| t.get("workerId").and_then(|v| v.as_str()) == Some(id))
            .and_then(|t| t.get("stopAfterUnreachableSecs"))
            .and_then(|v| v.as_u64())
    };
    assert_eq!(for_worker("w1"), Some(300));
    assert_eq!(for_worker("w2"), None);
}

/// The build a worker names on every request for the repository, which the leader must share to serve it.
fn build_header() -> String {
    format!(
        "http.extraHeader={}: {}",
        smeltr::leader::BUILD_HEADER,
        smeltr::version::commit()
    )
}

/// Clone the running leader the way a worker does, and answer what it deploys.
fn clone_and_read_deployed(base: &str, token: &str, dest: &std::path::Path) -> String {
    let status = git_without_prompting()
        .args([
            "-c",
            &format!("http.extraHeader=Authorization: Bearer {token}"),
            "-c",
            &build_header(),
            "clone",
            "--quiet",
            &format!("{base}/repo"),
            dest.to_str().unwrap(),
        ])
        .status()
        .expect("git clone runs");
    assert!(status.success(), "git clone against the running leader failed");
    fetch_deployed(token, dest)
}

/// Fetch what the leader deploys into an existing clone and answer the commit it names -- exactly what the worker
/// does every heartbeat (`worker::fetch_deployed`).
fn fetch_deployed(token: &str, dest: &std::path::Path) -> String {
    let status = git_without_prompting()
        .args([
            "-c",
            &format!("http.extraHeader=Authorization: Bearer {token}"),
            "-c",
            &build_header(),
            "-C",
            dest.to_str().unwrap(),
            "fetch",
            "--quiet",
            "origin",
            smeltr::leader::git::DEPLOYED_REF,
        ])
        .status()
        .expect("git fetch runs");
    assert!(status.success(), "git fetch against the running leader failed");
    let out = Command::new("git")
        .args(["-C", dest.to_str().unwrap(), "rev-parse", "FETCH_HEAD"])
        .output()
        .expect("rev-parse runs");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Deploying a commit is moving the pointer workers fetch: nothing names it to a worker, and fetching is how the
/// worker learns it. One `main` serves every group, so what a commit for another group costs this one is nothing -- its own
/// subtree is unchanged, which is the worker's comparison to make (`worker::content_id`).
#[test]
fn a_deployed_commit_is_what_a_worker_fetches() {
    let (base, data_dir) = start_leader_with_data_dir();
    let token = enroll_worker(&base, "w1");

    commit_for_group(&data_dir, "prod-west", Some("w1"));
    let east = commit_for_group(&data_dir, "prod-east", Some("e1"));

    let dest = scratch_dir("smeltr-leader-test-fetch");
    assert_eq!(clone_and_read_deployed(&base, &token, &dest), east);
}

#[test]
fn a_second_commit_moves_what_a_worker_fetches() {
    let (base, data_dir) = start_leader_with_data_dir();
    let token = enroll_worker(&base, "w1");

    let first = commit_for_group(&data_dir, "prod-orders", None);
    let dest = scratch_dir("smeltr-leader-test-fetch-twice");
    assert_eq!(clone_and_read_deployed(&base, &token, &dest), first);

    let second = commit_for_group(&data_dir, "prod-orders", Some("s1"));
    assert_ne!(first, second);
    assert_eq!(fetch_deployed(&token, &dest), second, "the fetch sees the new commit");
}

#[derive(serde::Deserialize)]
struct HealthResponse {
    threads: Vec<serde_json::Value>,
    groups: Vec<GroupHealthOut>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupHealthOut {
    group_id: String,
    transform_threads: usize,
    readers: usize,
    egress_threads: usize,
    partitions_owned: usize,
}

#[test]
fn health_rolls_up_threads_by_their_current_group() {
    let base = start_leader();
    let w1 = enroll_worker(&base, "w1");
    let w2 = enroll_worker(&base, "w2");
    heartbeat_one_thread(&base, &w1, "w1", "t0", "pool");
    heartbeat_one_thread(&base, &w1, "w1", "t1", "pool");
    heartbeat_one_thread(&base, &w2, "w2", "t0", "prod-orders");

    let health: HealthResponse = ureq::get(format!("{base}/api/health"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");

    assert_eq!(health.threads.len(), 3);
    let pool = health.groups.iter().find(|g| g.group_id == "pool").unwrap();
    assert_eq!(pool.readers, 2);
    assert_eq!(pool.transform_threads, 0, "these fixtures report readers only");
    assert_eq!(pool.egress_threads, 0);
    assert_eq!(pool.partitions_owned, 2); // one partition reported per thread
    let dedicated = health.groups.iter().find(|g| g.group_id == "prod-orders").unwrap();
    assert_eq!(dedicated.readers, 1);
}

// -- worker authentication ---------------------------------------------

/// A capture report weighs what the leader invited, and the invitation may run to the capture ceiling. The
/// endpoint states its own limit rather than inheriting a framework default a large capture would not fit.
#[test]
fn a_capture_report_larger_than_a_framework_default_body_is_accepted() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");
    let report = smeltr::heartbeat::CaptureReport {
        session: "s1".to_string(),
        worker_id: "w1".to_string(),
        batches: vec![smeltr::heartbeat::CaptureBatch {
            columns: vec![smeltr::heartbeat::CaptureColumn {
                name: "_raw".to_string(),
                ty: "str".to_string(),
            }],
            rows: vec![smeltr::heartbeat::CapturedRow {
                values: vec![serde_json::Value::String("x".repeat(3 * 1024 * 1024))],
                failed: vec![],
                worker: None,
            }],
        }],
        finished: Some(smeltr::heartbeat::CaptureEnd::Bytes),
        sql: None,
        avro: Vec::new(),
    };
    let status = ureq::post(format!("{base}/api/capture-report"))
        .header("Authorization", &format!("Bearer {token}"))
        .send_json(&report)
        .map(|r| r.status().as_u16())
        .unwrap_or_else(|e| match e {
            ureq::Error::StatusCode(c) => c,
            other => panic!("unexpected transport error: {other}"),
        });
    assert_eq!(status, 204, "a capture report past a 2 MiB body was refused");
}

#[test]
fn heartbeat_without_a_token_is_rejected() {
    let base = start_leader();
    let status = ureq::post(format!("{base}/api/heartbeat"))
        .send_json(serde_json::json!({ "workerId": "w1", "threads": [] }))
        .unwrap_err();
    assert!(matches!(status, ureq::Error::StatusCode(401)));
}

#[test]
fn heartbeat_with_a_garbage_token_is_rejected() {
    let base = start_leader();
    let status = ureq::post(format!("{base}/api/heartbeat"))
        .header("Authorization", "Bearer not-a-real-token")
        .send_json(serde_json::json!({ "workerId": "w1", "threads": [] }))
        .unwrap_err();
    assert!(matches!(status, ureq::Error::StatusCode(401)));
}

/// A token names exactly one worker, so a heartbeat reporting a different id is
/// refused rather than recorded. The registry is keyed by `(worker_id,
/// thread_id)` and never evicts, so a credential that could claim any id could
/// grow it without bound -- and could report freshness for a worker that is
/// revoked, or gone.
#[test]
fn a_heartbeat_cannot_claim_another_workers_id() {
    let base = start_leader();
    let w1 = enroll_worker(&base, "w1");
    enroll_worker(&base, "w2");

    assert_eq!(heartbeat_status(&base, &w1, "w1"), 200);
    assert_eq!(heartbeat_status(&base, &w1, "w2"), 403);
    assert_eq!(heartbeat_status(&base, &w1, "invented"), 403);

    let health: HealthResponse = ureq::get(format!("{base}/api/health"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    assert_eq!(health.threads.len(), 1, "only w1's own thread is on the registry");
}

#[test]
fn enrolling_with_the_same_code_twice_fails_the_second_time() {
    let base = start_leader();
    let code = mint_code(&base);
    assert!(try_enroll(&base, &code, "w1").ok);
    assert!(!try_enroll(&base, &code, "w2").ok);
}

/// A valid code cannot take over an existing worker's identity: the
/// original token keeps working and no second one is issued.
#[test]
fn enrolling_an_existing_worker_id_is_refused_and_the_original_keeps_its_token() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");
    let second = try_enroll(&base, &mint_code(&base), "w1");
    assert!(!second.ok);
    assert!(second.token.is_none());
    assert_eq!(heartbeat_status(&base, &token, "w1"), 200);
}

/// What `POST /api/enrollment-codes` answers `body` with.
fn mint_answer(base: &str, body: serde_json::Value) -> serde_json::Value {
    ureq::post(format!("{base}/api/enrollment-codes"))
        .send_json(body)
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response")
}

/// An ephemeral enrollment code brings in as many workers as an autoscaler starts, for as long as it runs, and each one
/// it brings in is ephemeral on the roster.
#[test]
fn an_ephemeral_enrollment_code_has_no_cap_and_no_expiry() {
    let base = start_leader();
    let minted = mint_answer(&base, serde_json::json!({ "kind": "ephemeral" }));
    assert!(minted["ok"].as_bool().unwrap_or(false), "{minted}");
    let code = &minted["code"];
    assert_eq!(code["kind"], "ephemeral");
    assert!(code["maxUses"].is_null(), "{code}");
    // A leader on plain HTTP pins no root, so nothing else bounds it either.
    assert!(code["expiresAtMillis"].is_null(), "{code}");

    let code = code["value"].as_str().unwrap();
    // Past the fifty an enrollment code could once bring in.
    for n in 0..60 {
        assert!(try_enroll(&base, code, &format!("pod-{n}")).ok, "pod-{n}");
    }
    let workers: Vec<smeltr::leader::WorkerOut> = ureq::get(format!("{base}/api/workers"))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(workers.len(), 60);
    assert!(workers.iter().all(|w| w.kind == "ephemeral"));
}

/// A heartbeat from `worker_id`, saying it is stopping or not, answered with its status.
fn beat_status(base: &str, token: &str, worker_id: &str, stopping: bool) -> u16 {
    match ureq::post(format!("{base}/api/heartbeat"))
        .header("Authorization", &format!("Bearer {token}"))
        .send_json(serde_json::json!({
            "workerId": worker_id,
            "stopping": stopping,
            "threads": [{ "threadId": "t0", "groupId": "pool", "partitionsOwned": [], "metrics": [] }],
        })) {
        Ok(r) => r.status().as_u16(),
        Err(ureq::Error::StatusCode(c)) => c,
        Err(other) => panic!("unexpected transport error: {other}"),
    }
}

fn roster(base: &str) -> Vec<smeltr::leader::WorkerOut> {
    ureq::get(format!("{base}/api/workers"))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap()
}

fn health_threads(base: &str) -> Vec<serde_json::Value> {
    let health: serde_json::Value = ureq::get(format!("{base}/api/health"))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    health["threads"].as_array().cloned().unwrap_or_default()
}

/// An ephemeral worker that says it is stopping is gone, and is forgotten on that heartbeat: off the roster, off
/// the registry, and its credential no longer authenticates. A permanent one saying the same is kept, shown
/// stopped, and comes back as itself.
#[test]
fn a_stopping_worker_is_forgotten_only_if_it_is_ephemeral() {
    let base = start_leader();
    let ephemeral = mint_answer(&base, serde_json::json!({ "kind": "ephemeral" }));
    let pod = try_enroll(&base, ephemeral["code"]["value"].as_str().unwrap(), "pod")
        .token
        .unwrap();
    let vm = enroll_worker(&base, "vm");

    for (token, id) in [(&pod, "pod"), (&vm, "vm")] {
        assert_eq!(beat_status(&base, token, id, false), 200);
        assert_eq!(beat_status(&base, token, id, true), 200, "{id}'s notice");
    }

    let kept: Vec<String> = roster(&base).into_iter().map(|w| w.worker_id).collect();
    assert_eq!(kept, ["vm"]);
    let threads = health_threads(&base);
    assert!(threads.iter().all(|t| t["workerId"] == "vm"), "{threads:?}");
    assert!(threads.iter().all(|t| t["stoppedAtMillis"].is_i64()), "{threads:?}");
    assert_eq!(
        beat_status(&base, &pod, "pod", false),
        401,
        "a forgotten worker's credential"
    );

    assert_eq!(beat_status(&base, &vm, "vm", false), 200, "the permanent worker, back");
    assert!(health_threads(&base).iter().all(|t| t["stoppedAtMillis"].is_null()));
}

/// How long an ephemeral worker may be silent before it is forgotten is this leader's to keep, within bounds it
/// states rather than a form restating them.
#[test]
fn how_long_an_ephemeral_worker_may_be_silent_is_a_setting_with_bounds() {
    let base = start_leader();
    let settings = |response: serde_json::Value| {
        assert_eq!(response["ok"], true, "{response}");
        response["settings"]["ephemeralForgetAfterSeconds"].as_u64().unwrap()
    };
    let get = || -> serde_json::Value {
        ureq::get(format!("{base}/api/settings/workers"))
            .call()
            .unwrap()
            .body_mut()
            .read_json()
            .unwrap()
    };
    let put = |seconds: u64| -> serde_json::Value {
        ureq::put(format!("{base}/api/settings/workers"))
            .send_json(serde_json::json!({ "ephemeralForgetAfterSeconds": seconds }))
            .unwrap()
            .body_mut()
            .read_json()
            .unwrap()
    };

    assert_eq!(settings(get()), 600, "ten minutes where nobody has said");
    let bounds = get()["settings"].clone();
    assert_eq!(
        bounds["defaultSeconds"], 600,
        "the default the form marks a change from"
    );
    let (min, max) = (
        bounds["minSeconds"].as_u64().unwrap(),
        bounds["maxSeconds"].as_u64().unwrap(),
    );
    assert_eq!(put(min - 1)["ok"], false);
    assert_eq!(put(max + 1)["ok"], false);
    assert_eq!(settings(put(120)), 120);
    assert_eq!(settings(get()), 120, "kept");
}

/// A cap or a date on an ephemeral enrollment code would be scale-out stopping with nobody watching, so one asked for
/// is refused rather than dropped. A permanent enrollment code keeps its defaults and has no ceiling.
#[test]
fn an_enrollment_codes_bounds_follow_its_kind() {
    let base = start_leader();
    for bounded in [
        serde_json::json!({ "kind": "ephemeral", "maxUses": 3 }),
        serde_json::json!({ "kind": "ephemeral", "lifetimeHours": 24 }),
        serde_json::json!({ "kind": "forever" }),
        serde_json::json!({ "maxUses": 0 }),
        serde_json::json!({ "lifetimeHours": 0 }),
    ] {
        let answer = mint_answer(&base, bounded.clone());
        assert_eq!(answer["ok"], false, "{bounded} was minted: {answer}");
    }

    let defaults = mint_answer(&base, serde_json::json!({}));
    assert_eq!(defaults["code"]["kind"], "permanent");
    assert_eq!(defaults["code"]["maxUses"], 1);

    let wide = mint_answer(&base, serde_json::json!({ "maxUses": 500, "lifetimeHours": 24 * 365 }));
    assert_eq!(wide["ok"], true, "{wide}");
    assert_eq!(wide["code"]["maxUses"], 500);
}

/// A worker whose enrollment answer was lost asks again with the same enrollment code and the same id, and is answered.
/// The credential in the lost answer authenticates nothing afterward. Once the new one has been used it is the
/// worker's, and the same retry is refused.
#[test]
fn an_enrollment_whose_answer_was_lost_can_be_asked_again() {
    let base = start_leader();
    let code = mint_code(&base);

    let lost = try_enroll(&base, &code, "w1");
    let retried = try_enroll(&base, &code, "w1");
    assert!(
        lost.ok && retried.ok,
        "the enrollment code's one use went to this worker, so its retry needs none"
    );
    let (lost, kept) = (lost.token.unwrap(), retried.token.unwrap());
    assert_ne!(lost, kept);

    assert_eq!(
        heartbeat_status(&base, &lost, "w1"),
        401,
        "the credential in the lost answer still authenticates"
    );
    assert_eq!(heartbeat_status(&base, &kept, "w1"), 200);

    assert!(!try_enroll(&base, &code, "w1").ok, "a credential in use was replaced");
    assert_eq!(heartbeat_status(&base, &kept, "w1"), 200);
}

/// Refused as revoked, which is not refused as unknown: a worker holding an enrollment code enrolls again past an
/// unknown credential, and revoking one is an operator saying it stays out.
#[test]
fn a_revoked_worker_can_no_longer_heartbeat() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");
    assert_eq!(heartbeat_status(&base, &token, "w1"), 200);
    assert!(revoke_worker(&base, "w1").ok);
    assert_eq!(heartbeat_status(&base, &token, "w1"), 403);
    assert_eq!(heartbeat_status(&base, "never-issued", "w1"), 401);
}

/// A valid code cannot resurrect a revoked worker id.
#[test]
fn a_revoked_worker_id_cannot_re_enroll() {
    let base = start_leader();
    let token = enroll_worker(&base, "w1");
    assert!(revoke_worker(&base, "w1").ok);
    let again = try_enroll(&base, &mint_code(&base), "w1");
    assert!(!again.ok);
    assert!(again.token.is_none());
    assert_eq!(heartbeat_status(&base, &token, "w1"), 403);

    let workers: Vec<smeltr::leader::WorkerOut> = ureq::get(format!("{base}/api/workers"))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(workers.len(), 1);
    assert!(workers[0].revoked);
}

#[test]
fn list_workers_shows_a_worker_that_enrolled_but_never_heartbeated() {
    let base = start_leader();
    enroll_worker(&base, "w1");

    let workers: Vec<smeltr::leader::WorkerOut> = ureq::get(format!("{base}/api/workers"))
        .call()
        .expect("request succeeds")
        .body_mut()
        .read_json()
        .expect("valid JSON response");
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].worker_id, "w1");
    assert!(!workers[0].revoked);
}

#[test]
fn revoking_an_unknown_worker_fails() {
    let base = start_leader();
    assert!(!revoke_worker(&base, "nope").ok);
}

/// `git`, with credential prompting turned off. An empty `credential.helper` resets the helper list rather than
/// appending to it.
fn git_without_prompting() -> Command {
    let mut cmd = Command::new("git");
    cmd.args(["-c", "credential.helper="]).env("GIT_TERMINAL_PROMPT", "0");
    cmd
}

/// Two groups in one repository, each in its own subtree: a commit for one leaves the other where it was.
#[test]
fn committing_one_group_leaves_another_groups_subtree_alone() {
    let (base, data_dir) = start_leader_with_data_dir();
    let token = enroll_worker(&base, "w1");
    commit_for_group(&data_dir, "prod-west", Some("west-1"));
    commit_for_group(&data_dir, "prod-east", Some("east-1"));
    // West again, after east: east must survive it.
    commit_for_group(&data_dir, "prod-west", Some("west-2"));

    let dest = scratch_dir("smeltr-leader-test-two-groups");
    let status = git_without_prompting()
        .args([
            "-c",
            &format!("http.extraHeader=Authorization: Bearer {token}"),
            "-c",
            &build_header(),
            "clone",
            "--quiet",
            &format!("{base}/repo"),
            dest.to_str().unwrap(),
        ])
        .status()
        .expect("git clone runs");
    assert!(status.success());

    let west = std::fs::read_to_string(dest.join("groups/prod-west/sources.json")).unwrap();
    let east = std::fs::read_to_string(dest.join("groups/prod-east/sources.json")).unwrap();
    assert!(west.contains("west-2"), "west has its latest: {west}");
    assert!(east.contains("east-1"), "east is untouched: {east}");
}

#[test]
fn a_git_clone_without_a_token_is_rejected() {
    let (base, data_dir) = start_leader_with_data_dir();
    commit_for_group(&data_dir, "prod-orders", None);

    let dest = scratch_dir("smeltr-leader-test-clone-unauth");
    let status = git_without_prompting()
        .args(["clone", "--quiet", &format!("{base}/repo"), dest.to_str().unwrap()])
        .status()
        .expect("git clone runs");
    assert!(!status.success(), "an unauthenticated clone must fail");
}
