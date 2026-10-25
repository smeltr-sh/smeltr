//! Bringing a worker in with nothing but a pasted enrollment code.
//!
//! The worker here is the real binary, given only a leader address and an enrollment code. No certificate file reaches
//! it by any other route, so what it ends up trusting is whatever the enrollment code's fingerprint let through.

use std::process::{Command, Stdio};
use std::sync::{Arc, Once, mpsc};
use std::time::{Duration, Instant};

use smeltr::leader::registry::Registry;
use smeltr::leader::sqlite_store::SqliteStore;
use smeltr::leader::{AppState, router};

/// These tests run one at a time.
///
/// Each starts a TLS leader and spawns the real binary, and `tests/gate-tests.rs` bounds its own leader starts for
/// the same reason: a box saturated by one test file makes another miss a readiness deadline for no reason but
/// the load. That limiter cannot see this file, so this one keeps this file from being what starves it. Bounding
/// only the worker processes was measured still failing a full run; bounding the whole test is not.
static ALONE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Held for a whole test. The first line of each, so nothing here overlaps anything else here.
fn alone() -> std::sync::MutexGuard<'static, ()> {
    ALONE.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a leader says once its socket has loaded what is on disk.
///
/// These tests replace certificate files directly, where a running leader would go through a handler that reloads
/// and says so.
fn socket_caught_up(dir: &std::path::Path) {
    smeltr::leader::tls::socket_took(dir, smeltr::leader::tls::disk_generation(dir));
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "smeltr-enrollment-{label}-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A leader serving TLS under an authority of its own, which is the deployment an enrollment code's fingerprint is for.
fn start_leader_tls() -> (String, Vec<u8>) {
    let (base, ca_pem, _) = start_leader_tls_in();
    (base, ca_pem)
}

fn start_leader_tls_in() -> (String, Vec<u8>, std::path::PathBuf) {
    start_leader_tls_following(false)
}

/// [`start_leader_tls_in`], whose socket takes every replacement on disk when `follow` is set, as `main`'s does.
/// Most tests here leave it, and read the socket and the disk parting as what their leader does.
fn start_leader_tls_following(follow: bool) -> (String, Vec<u8>, std::path::PathBuf) {
    static CRYPTO: Once = Once::new();
    CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();
    });

    let data_dir = temp_dir("leader-data");
    let returned_data_dir = data_dir.clone();
    let materials = smeltr::leader::tls::ensure(
        &smeltr::leader::tls::dir_of(&data_dir),
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
        Duration::from_secs(60 * 60),
        &[],
    )
    .expect("an authority and a certificate it signed");
    let ca_pem = materials.ca_pem.clone();
    let tls_dir = smeltr::leader::tls::dir_of(&data_dir);
    let (_, cert_pem, key_pem) = smeltr::leader::tls::to_serve(&tls_dir).unwrap();
    // The list a served certificate is an entry of, as `main` opens it.
    let store = Arc::new(
        SqliteStore::open(
            std::path::Path::new(":memory:"),
            &temp_dir("leader-key").join("sealing.key"),
        )
        .unwrap(),
    );
    smeltr::leader::tls::read_list(&tls_dir, store.clone());
    LISTS.lock().unwrap().push((tls_dir, store.clone()));
    // What `main` says once it has built the socket from these files. `ensure` above replaced what is served, and
    // until this the two have parted -- which before a socket exists is exactly right, and afterward is not.
    socket_caught_up(&smeltr::leader::tls::dir_of(&returned_data_dir));

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let state = AppState {
                store,
                registry: Arc::new(Registry::default()),
                throughput: Default::default(),
                captures: Arc::new(smeltr::leader::capture::Captures::default()),
                entitlements: Arc::new(smeltr::leader::Entitlements::default()),
                verify_limiter: Arc::new(smeltr::leader::VerifyLimiter::default()),
                expiries: Default::default(),
                deploy_checks: Default::default(),
                origin_hosts: Vec::new(),
                remote: Default::default(),
                updates: Default::default(),
                data_dir,
                directives: Default::default(),
            };
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem, key_pem)
                .await
                .unwrap();
            if follow {
                follow_disk(&smeltr::leader::tls::dir_of(&state.data_dir), tls.clone());
            }
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            tx.send(listener.local_addr().unwrap().port()).unwrap();
            axum_server::tls_rustls::from_tcp_rustls(listener, tls)
                .unwrap()
                .serve(router(state).into_make_service())
                .await
                .unwrap();
        });
    });
    (
        format!("https://127.0.0.1:{}", rx.recv().unwrap()),
        ca_pem,
        returned_data_dir,
    )
}

/// The certificate lists of these leaders, by their TLS directory.
static LISTS: std::sync::Mutex<Vec<(std::path::PathBuf, Arc<SqliteStore>)>> = std::sync::Mutex::new(Vec::new());

/// `chain` and `key` as the newest version of the entry `theirs` in the list of the leader serving from `dir`.
fn listed(dir: &std::path::Path, chain: &[u8], key: &[u8], ca: &[u8]) -> smeltr::leader::tls::Entry {
    use smeltr::leader::store::Store;
    let list = LISTS
        .lock()
        .unwrap()
        .iter()
        .find(|(d, _)| d == dir)
        .map(|(_, list)| list.clone())
        .expect("a leader serving from this directory");
    let (chain, key) = (std::str::from_utf8(chain).unwrap(), std::str::from_utf8(key).unwrap());
    let version = match list.add_certificate_version("theirs", chain, key) {
        Ok(version) => version,
        Err(_) => {
            list.create_certificate("theirs", "", chain, key).unwrap();
            1
        }
    };
    smeltr::leader::tls::Entry {
        name: "theirs".to_string(),
        version,
        ca: String::from_utf8(ca.to_vec()).unwrap(),
    }
}

/// The sockets these leaders serve, by their TLS directory, so a replacement on disk reaches the one presenting it.
static SOCKETS: std::sync::Mutex<Vec<(std::path::PathBuf, axum_server::tls_rustls::RustlsConfig)>> =
    std::sync::Mutex::new(Vec::new());

/// Makes `config` take what `dir` holds whenever a path replaces it, as `main` does for the one leader it runs: a
/// cutover a heartbeat decides changes what is served, and what this socket presents has to follow.
fn follow_disk(dir: &std::path::Path, config: axum_server::tls_rustls::RustlsConfig) {
    SOCKETS.lock().unwrap().push((dir.to_path_buf(), config));
    static REGISTERED: Once = Once::new();
    REGISTERED.call_once(|| {
        smeltr::leader::tls::on_reload(|dir| {
            let Some(config) = SOCKETS
                .lock()
                .unwrap()
                .iter()
                .find(|(d, _)| d == dir)
                .map(|(_, c)| c.clone())
            else {
                return;
            };
            // Off this thread: the replacement holds the lock `to_serve` takes.
            let dir = dir.to_path_buf();
            std::thread::spawn(move || {
                let Ok((generation, chain, key)) = smeltr::leader::tls::to_serve(&dir) else {
                    return;
                };
                let took = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(config.reload_from_pem(chain, key));
                if took.is_ok() {
                    smeltr::leader::tls::socket_took(&dir, generation);
                }
            });
        });
    });
}

/// A certificate, key and authority from somebody else's PKI, carrying the names these leaders answer to.
fn external_pki(tag: &str) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let dir = smeltr::leader::tls::dir_of(&temp_dir(tag));
    let materials = smeltr::leader::tls::ensure(
        &dir,
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
        Duration::from_secs(60 * 60),
        &[],
    )
    .expect("their authority");
    let (_, chain, key) = smeltr::leader::tls::to_serve(&dir).unwrap();
    (chain, key, materials.ca_pem)
}

/// Waits until the worker keeps `fingerprint` among the roots it verifies by.
fn holds(state_dir: &std::path::Path, fingerprint: &str, within: Duration) -> bool {
    kept_roots_become(state_dir, within, |roots| roots.contains(&fingerprint))
}

/// Waits until `fingerprint` is the one root the worker verifies by: the bundle handed after a cutover.
fn holds_only(state_dir: &std::path::Path, fingerprint: &str, within: Duration) -> bool {
    kept_roots_become(state_dir, within, |roots| roots == [fingerprint])
}

fn kept_roots_become(state_dir: &std::path::Path, within: Duration, what: impl Fn(&[&str]) -> bool) -> bool {
    let kept = state_dir.join("pinned-ca.pem");
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if std::fs::read(&kept)
            .ok()
            .and_then(|pem| smeltr::leader::tls::describe_each(&pem, "kept").ok())
            .is_some_and(|roots| what(&roots.iter().map(|r| r.fingerprint.as_str()).collect::<Vec<_>>()))
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

/// Waits until no rotation is open.
fn finished(dir: &std::path::Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if smeltr::leader::tls::rotation(dir).unwrap().is_none() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

fn mint(base: &str, ca_pem: &[u8], body: serde_json::Value) -> serde_json::Value {
    let agent = smeltr::heartbeat::agent_trusting(Some(ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let minted: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(body)
        .expect("mint an enrollment code")
        .body_mut()
        .read_json()
        .unwrap();
    assert!(minted["ok"].as_bool().unwrap_or(false), "{minted}");
    minted["code"].clone()
}

/// Spawns the real binary as a worker with only what a pasted enrollment code gives it.
fn spawn_worker(base: &str, code: &str, state_dir: &std::path::Path) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_smeltr"))
        .env("SMELTR_WORKER_ENABLED", "1")
        .env("SMELTR_LEADER_URL", base)
        .env("SMELTR_WORKER_ENROLLMENT_CODE", code)
        .env("SMELTR_WORKER_STATE_DIR", state_dir.to_str().unwrap())
        .env("SMELTR_WORKER_THREAD_LIMIT", "1")
        .env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", "300")
        // No SMELTR_LEADER_CA: the enrollment code is the only thing telling this worker what to accept.
        .env_remove("SMELTR_LEADER_CA")
        // Isolated from any `smeltr.toml` the test inherited the cwd of.
        .current_dir(state_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn smeltr in its worker role")
}

/// Waits for the leader to have heard a heartbeat, rather than for the worker to say it sent one.
///
/// `/api/health` is built from heartbeats received, so an entry there is a request this worker made, over TLS it
/// verified, that the leader accepted. `/api/workers` is the wrong question: it lists every worker that ever
/// enrolled, heartbeating or not, so it reads as success for a worker whose every heartbeat is failing the
/// handshake -- which is exactly the bug that shipped past an earlier version of this helper.
fn heartbeating(base: &str, ca_pem: &[u8], within: Duration) -> bool {
    let agent = smeltr::heartbeat::agent_trusting(Some(ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let health = agent
            .get(format!("{base}/api/health"))
            .call()
            .ok()
            .and_then(|mut response| response.body_mut().read_json::<serde_json::Value>().ok());
        if health.is_some_and(|h| h["threads"].as_array().is_some_and(|t| !t.is_empty())) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

/// Stops the worker and hands back everything it said.
fn stop(mut child: std::process::Child) -> String {
    let mut said = String::new();
    let _ = child.kill();
    let _ = child.wait();
    if let Some(mut stderr) = child.stderr.take() {
        use std::io::Read;
        let _ = stderr.read_to_string(&mut said);
    }
    said
}

/// Runs a worker the leader will refuse for as long as it asks, and gives back what it said on the way out.
///
/// It has to stop by itself. A worker that kept retrying an enrollment code nobody can make work would sit in backoff
/// until somebody noticed, reporting the same line for a fault only an operator can clear.
fn gives_up(base: &str, code: &str, state_dir: &std::path::Path, within: Duration) -> String {
    let mut child = spawn_worker(base, code, state_dir);
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if matches!(child.try_wait(), Ok(Some(_))) {
            let mut said = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                use std::io::Read;
                let _ = stderr.read_to_string(&mut said);
            }
            return said;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let said = stop(child);
    panic!("it was still retrying after {within:?}:\n{said}");
}

/// Runs a worker that is expected not to enroll, and gives back what it complained about.
fn refused(base: &str, code: &str, state_dir: &std::path::Path) -> String {
    let child = spawn_worker(base, code, state_dir);
    // Long enough for a worker that was going to enroll to have done so.
    std::thread::sleep(Duration::from_secs(6));
    stop(child)
}

/// The whole point: an address and an enrollment code, and nothing else. A worker taking an enrollment code keeps the
/// one certificate it names and accepts no other. Past that certificate's own expiry the enrollment code buys a
/// credential for a leader that worker can no longer verify, so the enrollment code ends first.
#[test]
fn an_enrollment_code_never_outlives_the_authority_it_pins_to() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    // The leader here holds an authority good for an hour, and this asks for a day.
    let code = mint(&base, &ca_pem, serde_json::json!({ "lifetimeHours": 24 }));

    let root_expires = smeltr::leader::tls::describe_each(&ca_pem, "the authority")
        .expect("read the authority")
        .iter()
        .map(|root| root.not_after_millis)
        .max()
        .expect("a root");
    let expires = code["expiresAtMillis"].as_i64().expect("an expiry");
    assert!(
        expires <= root_expires,
        "an enrollment code outlived the authority it pins to: {expires} against {root_expires}"
    );
}

/// The workers `/api/health` has threads from.
#[cfg(unix)]
fn heard_from(agent: &ureq::Agent, base: &str) -> Vec<serde_json::Value> {
    agent
        .get(format!("{base}/api/health"))
        .call()
        .ok()
        .and_then(|mut response| response.body_mut().read_json::<serde_json::Value>().ok())
        .and_then(|health| health["threads"].as_array().cloned())
        .unwrap_or_default()
}

/// A worker stopped with SIGTERM tells its leader it is stopping, and its silence afterward is not reported. One
/// killed outright says nothing, and is reported silent: one fleet, the two told apart by whether the worker spoke
/// last.
#[test]
#[cfg(unix)]
fn a_worker_that_says_it_is_stopping_is_not_reported_silent() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let code = mint(&base, &ca_pem, serde_json::json!({ "maxUses": 2 }));
    let value = code["value"].as_str().unwrap();
    let (told_dir, killed_dir) = (temp_dir("stops-saying-so"), temp_dir("killed-outright"));
    let mut told = spawn_worker(&base, value, &told_dir);
    let killed = spawn_worker(&base, value, &killed_dir);

    let outcome = (|| {
        let deadline = Instant::now() + Duration::from_secs(40);
        let id_of = |dir: &std::path::Path| std::fs::read_to_string(dir.join("worker_id")).ok();
        let (told_id, killed_id) = loop {
            if let (Some(told_id), Some(killed_id)) = (id_of(&told_dir), id_of(&killed_dir)) {
                let threads = heard_from(&agent, &base);
                let beating = |id: &str| threads.iter().any(|t| t["workerId"] == id);
                if beating(&told_id) && beating(&killed_id) {
                    break (told_id, killed_id);
                }
            }
            if Instant::now() >= deadline {
                return Err("the two workers were never both heartbeating".to_string());
            }
            std::thread::sleep(Duration::from_millis(200));
        };

        unsafe { libc::kill(killed.id() as libc::pid_t, libc::SIGKILL) };
        unsafe { libc::kill(told.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = told.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err("the signaled worker did not exit within a minute".to_string());
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if !status.success() {
            return Err(format!("the signaled worker exited {status}"));
        }

        let told_threads: Vec<serde_json::Value> = heard_from(&agent, &base)
            .into_iter()
            .filter(|t| t["workerId"] == told_id.as_str())
            .collect();
        if told_threads.is_empty() || told_threads.iter().any(|t| t["stoppedAtMillis"].as_i64().is_none()) {
            return Err(format!(
                "the leader does not hold the worker's notice: {told_threads:?}"
            ));
        }

        // Past what this leader counts as silent: three of its own heartbeat intervals, at the default.
        std::thread::sleep(Duration::from_secs(6));
        let warnings: Vec<serde_json::Value> = agent
            .get(format!("{base}/api/warnings"))
            .call()
            .map_err(|e| e.to_string())?
            .body_mut()
            .read_json()
            .map_err(|e| e.to_string())?;
        let silent: Vec<&str> = warnings
            .iter()
            .filter(|w| w["condition"] == "workerSilent")
            .filter_map(|w| w["subjectId"].as_str())
            .collect();
        if silent != [killed_id.as_str()] {
            return Err(format!(
                "reported silent: {silent:?}; the worker killed outright is {killed_id}, the one that said it was \
                 stopping is {told_id}"
            ));
        }
        Ok(())
    })();

    let said = stop(told);
    let _ = stop(killed);
    if let Err(why) = outcome {
        panic!("{why}\nthe signaled worker said:\n{said}");
    }
}

/// Waits for `child` to exit, and answers its exit code.
#[cfg(unix)]
fn exit_code(child: &mut std::process::Child, within: Duration) -> Result<Option<i32>, String> {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Ok(status.code());
        }
        if Instant::now() >= deadline {
            return Err(format!("it did not exit within {within:?}"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// What a container restarted in its pod finds: the state the pod kept. The leader forgot the ephemeral worker that
/// state names, so its credential is unknown, and the worker enrolls again with the enrollment code it still holds, as
/// a new worker. It exits to be started as that worker, and is.
#[test]
#[cfg(unix)]
fn a_forgotten_ephemeral_worker_enrolls_again_as_a_new_one() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let code = mint(&base, &ca_pem, serde_json::json!({ "kind": "ephemeral" }));
    let value = code["value"].as_str().unwrap();
    let state = temp_dir("pod-state");
    let id = || std::fs::read_to_string(state.join("worker_id")).ok();
    let roster = || -> Vec<String> {
        let workers: Vec<serde_json::Value> = agent
            .get(format!("{base}/api/workers"))
            .call()
            .unwrap()
            .body_mut()
            .read_json()
            .unwrap();
        workers
            .iter()
            .map(|w| w["workerId"].as_str().unwrap().to_string())
            .collect()
    };
    let beating = |worker_id: &str, within: Duration| {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if heard_from(&agent, &base).iter().any(|t| t["workerId"] == worker_id) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    };

    let mut worker = spawn_worker(&base, value, &state);
    let outcome = (|| {
        let deadline = Instant::now() + Duration::from_secs(30);
        let first = loop {
            if let Some(first) = id().filter(|first| beating(first, Duration::from_millis(200))) {
                break first;
            }
            if Instant::now() >= deadline {
                return Err("the worker never heartbeated".to_string());
            }
        };

        // Stopped on a signal, it says so, and the leader forgets it at once.
        unsafe { libc::kill(worker.id() as libc::pid_t, libc::SIGTERM) };
        if exit_code(&mut worker, Duration::from_secs(60))? != Some(0) {
            return Err("the signaled worker did not exit cleanly".to_string());
        }
        if !roster().is_empty() {
            return Err(format!(
                "the leader still holds the worker it should have forgotten: {:?}",
                roster()
            ));
        }

        // Started again on the same state: refused as unknown, it enrolls again and exits to become the new one.
        let _ = stop(std::mem::replace(&mut worker, spawn_worker(&base, value, &state)));
        if exit_code(&mut worker, Duration::from_secs(60))? != Some(75) {
            return Err("the forgotten worker did not exit to start again as a new one".to_string());
        }
        let second = id().ok_or("no worker id was kept")?;
        if second == first {
            return Err(format!("it enrolled again under the id the leader forgot: {first}"));
        }

        let _ = stop(std::mem::replace(&mut worker, spawn_worker(&base, value, &state)));
        if !beating(&second, Duration::from_secs(30)) {
            return Err(format!("the worker never heartbeated as {second}"));
        }
        if roster() != [second.clone()] {
            return Err(format!("the roster is {:?}, not just {second}", roster()));
        }
        Ok(())
    })();

    let said = stop(worker);
    if let Err(why) = outcome {
        panic!("{why}\nthe worker last said:\n{said}");
    }
}

/// An ephemeral enrollment code has no expiry of its own, and still ends with the authority it pins: past that, it
/// would buy a credential for a leader its worker can no longer verify.
#[test]
fn an_ephemeral_enrollment_code_ends_with_the_authority_it_pins_to() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({ "kind": "ephemeral" }));

    let root_expires = smeltr::leader::tls::describe_each(&ca_pem, "the authority")
        .expect("read the authority")
        .iter()
        .map(|root| root.not_after_millis)
        .max()
        .expect("a root");
    assert_eq!(code["expiresAtMillis"].as_i64(), Some(root_expires), "{code}");
    assert!(code["maxUses"].is_null(), "{code}");
}

/// A group id `main` has never deployed -- never created, or created and left on a branch -- names nothing a worker
/// could ever run. Minted anyway, the enrollment code would buy a credential no deployment answers for; refused here
/// costs nothing.
#[test]
fn an_enrollment_code_naming_an_undeployed_group_is_refused() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let response: serde_json::Value = agent
        .post(format!("{base}/api/enrollment-codes"))
        .send_json(serde_json::json!({ "groupId": "west" }))
        .expect("ask to mint an enrollment code")
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(response["ok"], false, "{response}");
    assert!(
        response["errors"][0]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("not a deployed worker group"),
        "{response}"
    );
}

#[test]
fn a_worker_given_only_an_enrollment_code_pins_the_authority_and_enrolls() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let state_dir = temp_dir("worker-state");

    let worker = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    let arrived = heartbeating(&base, &ca_pem, Duration::from_secs(60));
    let said = stop(worker);
    assert!(arrived, "the worker never heartbeated:\n{said}");
    assert!(said.contains("worker.authority_pinned"), "{said}");

    // Kept, so the unverified fetch happens once in this worker's life rather than at every start.
    let kept = std::fs::read(state_dir.join("pinned-ca.pem")).expect("the authority it pinned");
    assert_eq!(kept, ca_pem, "it kept the authority the leader actually serves");
}

/// An enrollment code naming an authority the leader does not serve. Nothing the leader offers can satisfy it, so
/// nothing is trusted and nothing is enrolled -- which is what the fingerprint is for.
#[test]
fn an_enrollment_code_naming_another_authority_is_refused_rather_than_trusted() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let real = code["value"].as_str().unwrap();

    // Someone else's authority, in an enrollment code that is otherwise well formed.
    let elsewhere = smeltr::leader::tls::ensure(
        &smeltr::leader::tls::dir_of(&temp_dir("elsewhere")),
        vec!["localhost".to_string()],
        Duration::from_secs(60 * 60),
        &[],
    )
    .unwrap();
    let theirs = smeltr::leader::tls::describe(&elsewhere.ca_pem, "theirs").unwrap();
    let mine: smeltr::enrollment_code::EnrollmentCode = real.parse().unwrap();
    let forged = smeltr::enrollment_code::EnrollmentCode {
        authority: smeltr::enrollment_code::Authority::Pinned(theirs.fingerprint),
        secret: mine.secret,
    }
    .to_string();

    let state_dir = temp_dir("worker-refused");
    let said = refused(&base, &forged, &state_dir);
    assert!(
        said.contains("did not offer the certificate authority this enrollment code names"),
        "{said}"
    );
    assert!(!said.contains("worker.enrolled"), "it enrolled anyway:\n{said}");
    assert!(
        !state_dir.join("pinned-ca.pem").exists(),
        "it kept an authority it could not match"
    );
}

/// An enrollment code spends once by default, and the second worker presenting it is told why rather than left
/// guessing.
#[test]
fn a_second_worker_cannot_spend_an_enrollment_code_minted_for_one() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let value = code["value"].as_str().unwrap();

    let first = spawn_worker(&base, value, &temp_dir("first-worker"));
    let arrived = heartbeating(&base, &ca_pem, Duration::from_secs(60));
    let said = stop(first);
    assert!(arrived, "{said}");

    let said = gives_up(&base, value, &temp_dir("second-worker"), Duration::from_secs(30));
    assert!(said.contains("already brought in every worker"), "{said}");
    assert!(
        said.contains("worker.enroll_refused"),
        "it treated a spent enrollment code as something to wait out:\n{said}"
    );
}

/// A leader that answers is a leader that is up. Waiting out its answer is waiting for an address to change
/// itself, which is what a first boot against a mistyped URL used to do.
#[test]
fn a_worker_pointed_at_the_wrong_address_stops_rather_than_waiting() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let value = code["value"].as_str().unwrap();

    // Reaches the leader, and the leader has nothing at this path.
    let wrong = format!("{base}/not-the-leader");
    let said = gives_up(&wrong, value, &temp_dir("wrong-address"), Duration::from_secs(30));
    assert!(said.contains("404"), "{said}");
    assert!(
        said.contains("check the address"),
        "it did not say what to look at:\n{said}"
    );
}

/// Minted for a fleet, spent by a fleet.
#[test]
fn an_enrollment_code_minted_for_several_workers_brings_in_that_many() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let code = mint(&base, &ca_pem, serde_json::json!({ "maxUses": 2 }));
    let value = code["value"].as_str().unwrap();

    for label in ["fleet-1", "fleet-2"] {
        let child = spawn_worker(&base, value, &temp_dir(label));
        std::thread::sleep(Duration::from_secs(6));
        let said = stop(child);
        assert!(said.contains("worker.enrolled"), "{label} never enrolled:\n{said}");
    }
    let said = refused(&base, value, &temp_dir("fleet-3"));
    assert!(said.contains("already brought in every worker"), "{said}");
}

/// The whole of a rotation, against the running binary: a real worker takes the incoming root off its heartbeat,
/// the leader signs under it once that worker has said so, and the worker goes on verifying a leader whose
/// certificate now chains to a root it was never restarted to learn.
///
/// The failure this rules out is the silent one. A leader that signed first would leave this worker's heartbeats
/// succeeding on a connection already open while every new handshake was refused, so the assertion that matters is
/// the last: it is still being heard from, after the certificate moved.
#[test]
fn a_worker_takes_a_rotated_authority_without_being_restarted() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_in();
    let dir = smeltr::leader::tls::dir_of(&data_dir);
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let state_dir = temp_dir("rotate");

    let child = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    assert!(heartbeating(&base, &ca_pem, Duration::from_secs(20)), "never enrolled");

    let now_ms = smeltr::leader::warnings::now_millis();
    // A deadline far enough out that nothing here can pass it: what ends this rotation has to be the worker
    // reporting the incoming root, not the clock running out.
    let state =
        smeltr::leader::tls::begin_rotation(&dir, Duration::from_secs(60 * 60), now_ms + 60 * 60 * 1000, now_ms)
            .expect("a rotation");

    let kept = state_dir.join("pinned-ca.pem");
    let took_it = |within: Duration| {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if std::fs::read(&kept)
                .ok()
                .and_then(|pem| smeltr::leader::tls::describe_each(&pem, "kept").ok())
                .is_some_and(|roots| roots.iter().any(|r| r.fingerprint == state.incoming))
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    };
    assert!(
        took_it(Duration::from_secs(20)),
        "the worker never took the incoming authority"
    );

    let cut_over = {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if smeltr::leader::tls::rotation(&dir).unwrap().is_none() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    assert!(
        cut_over,
        "the fleet held the incoming root and the leader never signed under it"
    );

    let served = std::fs::read(dir.join("cert.pem")).unwrap();
    let bundle = std::fs::read(dir.join(smeltr::leader::tls::CA_CERT)).unwrap();
    assert_eq!(
        smeltr::leader::tls::signing_root(&bundle, &served).unwrap().fingerprint,
        state.incoming,
        "the certificate this leader serves is still signed under the root it was rotating from"
    );

    // What the whole shape is for. The certificate moved under a running worker, and it is still being heard from.
    assert!(
        heartbeating(&base, &ca_pem, Duration::from_secs(20)),
        "the worker stopped reaching the leader once it signed under the incoming root"
    );
    stop(child);
}

/// The beat that carries a rotation past its deadline is still a beat from a worker the leader can reach, and it
/// must leave holding the root the leader is about to sign under.
///
/// Answering the cutover first finds no rotation and sends nothing, so the one worker that was certainly reachable
/// is the one stranded -- which is the failure the whole overlap exists to prevent, arriving by the other door.
#[test]
fn the_worker_whose_beat_ends_a_rotation_is_handed_the_authority_first() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_in();
    let dir = smeltr::leader::tls::dir_of(&data_dir);
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let state_dir = temp_dir("deadline");

    let child = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    assert!(heartbeating(&base, &ca_pem, Duration::from_secs(20)), "never enrolled");

    // A deadline already past, so the very next beat ends the rotation.
    let now_ms = smeltr::leader::warnings::now_millis();
    let state = smeltr::leader::tls::begin_rotation(&dir, Duration::from_secs(60 * 60), now_ms - 1000, now_ms)
        .expect("a rotation");

    let cut_over = {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if smeltr::leader::tls::rotation(&dir).unwrap().is_none() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    assert!(cut_over, "a deadline already past did not end the rotation");

    // The worker was there for it, so it holds what this leader now signs under.
    let kept = state_dir.join("pinned-ca.pem");
    let holds = {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if std::fs::read(&kept)
                .ok()
                .and_then(|pem| smeltr::leader::tls::describe_each(&pem, "kept").ok())
                .is_some_and(|roots| roots.iter().any(|r| r.fingerprint == state.incoming))
            {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    };
    assert!(
        holds,
        "the beat that ended the rotation left without the authority it ended it for"
    );
    assert!(
        heartbeating(&base, &ca_pem, Duration::from_secs(20)),
        "the worker stopped reaching the leader after the deadline cutover"
    );
    stop(child);
}

/// The authority is public, and a worker holding an enrollment code has no session to prove anything with.
#[test]
fn the_authority_is_served_to_a_caller_with_no_credential() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let served = agent
        .get(format!("{base}/api/authority"))
        .call()
        .expect("no credential needed")
        .body_mut()
        .read_to_vec()
        .unwrap();
    assert_eq!(served, ca_pem);
}

/// A certificate that reached the disk and was refused by the socket leaves the two describing different leaders.
/// Handing this file out then gives a worker a root its very next request refuses, and the worker reads that
/// refusal as permanent.
#[test]
fn the_authority_is_withheld_while_the_socket_has_not_taken_what_is_on_disk() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_in();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();

    // A real reissue: the disk moves and this leader's socket, which nothing here reloads, keeps what it had.
    smeltr::leader::tls::ensure_own(
        &smeltr::leader::tls::dir_of(&data_dir),
        vec!["localhost".to_string(), "127.0.0.1".to_string()],
        Duration::from_secs(60 * 60),
    )
    .expect("a reissue");
    let refused = agent
        .get(format!("{base}/api/authority"))
        .call()
        .expect_err("a file that vouches for nothing this leader presents");
    // Put back before asserting: these are two numbers for the whole process, shared with every test here.
    socket_caught_up(&smeltr::leader::tls::dir_of(&data_dir));
    assert!(matches!(refused, ureq::Error::StatusCode(503)), "{refused}");

    agent
        .get(format!("{base}/api/authority"))
        .call()
        .expect("served again once the two agree");
}

/// Replacing the authority strands every enrollment code minted under the old one: a worker taking one would pin a
/// certificate this leader no longer offers. The page says so against each, rather than leaving an operator to
/// find out from a worker that will not start.
#[test]
fn an_enrollment_code_says_it_names_an_authority_that_was_replaced_under_it() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_in();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let pinned = mint(&base, &ca_pem, serde_json::json!({}));

    let outstanding = |agent: &ureq::Agent| -> serde_json::Value {
        agent
            .get(format!("{base}/api/enrollment-codes"))
            .call()
            .unwrap()
            .body_mut()
            .read_json::<serde_json::Value>()
            .unwrap()
    };
    assert_eq!(outstanding(&agent)["codes"][0]["namesAuthority"], "served");

    // What an operator does when they bring their own certificate.
    let names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let (chain, key, theirs) = external_pki("their-pki");
    let dir = smeltr::leader::tls::dir_of(&data_dir);
    smeltr::leader::tls::serve(&dir, &listed(&dir, &chain, &key, &theirs), &names, &[])
        .expect("an operator's own certificate");
    socket_caught_up(&smeltr::leader::tls::dir_of(&data_dir));

    // The connection is still the old one's, which is the point: replacing what is served does not end a session.
    assert_eq!(
        outstanding(&agent)["codes"][0]["namesAuthority"],
        "replaced",
        "the enrollment code still names the authority that was replaced"
    );

    // Refused at the point of use, not only described on a page. Spent, it would buy a credential for a leader
    // this worker pins a retired root for: an enrollment that reports success and a worker that never beats.
    let answered: serde_json::Value = agent
        .post(format!("{base}/api/enroll"))
        .send_json(serde_json::json!({ "code": pinned["value"], "workerId": "w-after-the-replacement" }))
        .expect("the enrollment was answered")
        .body_mut()
        .read_json()
        .unwrap();
    assert_eq!(
        answered["ok"], false,
        "the enrollment code bought a credential anyway: {answered}"
    );
    assert_eq!(answered["retry"], false, "a worker was told to keep asking: {answered}");
    assert!(
        answered["error"].as_str().unwrap().contains("no longer signs under"),
        "the refusal does not say what is wrong with the enrollment code: {answered}"
    );
}

/// A worker pointed at a different leader, keeping its state directory. The enrollment code is the later word about
/// what to verify, so the authority kept from the old leader is replaced rather than used against the new one.
#[test]
fn an_enrollment_code_naming_a_different_authority_replaces_the_one_kept() {
    let _alone = alone();
    let (first, first_ca) = start_leader_tls();
    let state_dir = temp_dir("re-pointed");

    let code = mint(&first, &first_ca, serde_json::json!({}));
    let worker = spawn_worker(&first, code["value"].as_str().unwrap(), &state_dir);
    let arrived = heartbeating(&first, &first_ca, Duration::from_secs(60));
    // Stopped whether or not it arrived. `assert!(cond, "{}", stop(w))` evaluates `stop` only when the assert
    // fails, so a passing run would leave the worker alive holding the permit the rest of this file waits on.
    let said = stop(worker);
    assert!(arrived, "it never heartbeated the first leader:\n{said}");
    assert_eq!(
        std::fs::read(state_dir.join("pinned-ca.pem")).unwrap(),
        first_ca,
        "it kept the first leader's authority"
    );

    // Re-provisioned: a new leader, an enrollment code of its own, and everything but the credential left in place.
    std::fs::remove_file(state_dir.join("credential.token")).unwrap();
    let (second, second_ca) = start_leader_tls();
    let code = mint(&second, &second_ca, serde_json::json!({}));
    let worker = spawn_worker(&second, code["value"].as_str().unwrap(), &state_dir);
    let arrived = heartbeating(&second, &second_ca, Duration::from_secs(60));
    let said = stop(worker);

    assert!(arrived, "it never heartbeated the leader it was re-pointed at:\n{said}");
    assert_eq!(
        std::fs::read(state_dir.join("pinned-ca.pem")).unwrap(),
        second_ca,
        "it took the new leader's authority rather than keeping the old one"
    );
}

/// An enrollment code is spent once. An enrolled worker whose unit file still names one -- redacted by a secrets tool,
/// rotated, or minted by a leader of a newer age -- has a credential and needs none of it.
#[test]
fn an_enrolled_worker_starts_with_an_enrollment_code_it_can_no_longer_read() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let state_dir = temp_dir("stale-code");

    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let worker = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    let arrived = heartbeating(&base, &ca_pem, Duration::from_secs(60));
    let said = stop(worker);
    assert!(arrived, "{said}");

    // What a secrets tool leaves behind, which parses as nothing.
    let worker = spawn_worker(&base, "REDACTED", &state_dir);
    std::thread::sleep(Duration::from_secs(4));
    let said = stop(worker);
    assert!(
        !said.contains("refusing to start"),
        "an enrolled worker refused over an enrollment code it never needed:\n{said}"
    );
    assert!(said.contains("worker.started"), "{said}");
}

/// Re-pointed from a leader with its own authority to one a public certificate covers. The enrollment code says the
/// platform's roots, which the kept private authority is not, so the kept file is not the newer word.
#[test]
fn an_enrollment_code_naming_the_platforms_roots_puts_aside_a_kept_private_authority() {
    let _alone = alone();
    let (base, ca_pem) = start_leader_tls();
    let state_dir = temp_dir("system-repoint");

    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let worker = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    let arrived = heartbeating(&base, &ca_pem, Duration::from_secs(60));
    let said = stop(worker);
    assert!(arrived, "{said}");
    assert!(state_dir.join("pinned-ca.pem").exists());

    // Re-provisioned against a leader verified by the platform, with the state directory kept.
    std::fs::remove_file(state_dir.join("credential.token")).unwrap();
    let system_code = smeltr::enrollment_code::EnrollmentCode {
        authority: smeltr::enrollment_code::Authority::System,
        secret: "b".repeat(64),
    }
    .to_string();
    let worker = spawn_worker(&base, &system_code, &state_dir);
    std::thread::sleep(Duration::from_secs(4));
    let said = stop(worker);

    assert!(
        said.contains("worker.authority_replaced"),
        "it kept the private authority against an enrollment code naming the platform's roots:\n{said}"
    );
    // Gone, not merely passed over. An enrollment code is spent once, so the next start has none to say this file is
    // superseded -- and would take it back up, sending every heartbeat under the old leader's authority.
    assert!(
        !state_dir.join("pinned-ca.pem").exists(),
        "the superseded authority is still here for the next start to pick up"
    );
}

/// A rotation is the leader promising its fleet a new root before signing under it, and the promise travels on the
/// heartbeat. While the socket serves a certificate the disk does not describe, `/api/authority` is withheld and no
/// worker can be handed anything -- so a deadline reached in that state would sign under a root the fleet has never
/// been offered, stranding every worker at once. Neither end of a rotation runs there.
#[test]
fn a_rotation_neither_starts_nor_finishes_while_the_socket_and_the_disk_disagree() {
    let _alone = alone();
    let dir = temp_dir("rotation-disagrees").join("tls");
    let names = vec!["localhost".to_string()];
    let a_year = Duration::from_secs(365 * 24 * 60 * 60);
    smeltr::leader::tls::ensure(&dir, names.clone(), a_year, &[]).expect("an authority");
    socket_caught_up(&dir);

    let now_ms = smeltr::leader::warnings::now_millis();
    let deadline = now_ms + 60 * 1000;
    let rotation = smeltr::leader::tls::begin_rotation(&dir, a_year, deadline, now_ms).expect("a rotation");

    // A real reissue moves the disk past what the socket has, with nothing here to reload it.
    smeltr::leader::tls::ensure_own(&dir, names.clone(), a_year).expect("a reissue");
    let held = smeltr::leader::tls::cut_over_if_due(&dir, names.clone(), a_year, None, deadline + 1);
    let refused = smeltr::leader::tls::begin_rotation(&dir, a_year, deadline, now_ms);
    // The lock is what makes the answer binding: a caller can check, a reload can fail underneath it, and the
    // cutover still has to refuse when it gets there.
    let pressed = smeltr::leader::tls::cut_over(&dir, names.clone(), a_year);
    let preflight = smeltr::leader::tls::cut_over_is_possible(&dir);
    // Put back before asserting: two numbers for the whole process, shared with every test in this file.
    socket_caught_up(&dir);

    assert!(
        pressed.is_err(),
        "a cutover promoted a root while every worker was being denied it"
    );
    assert_eq!(
        preflight.expect_err("the preflight agrees").message(),
        pressed.expect_err("and so does the act"),
        "the preflight and the act give different reasons"
    );
    assert!(
        !held.expect("readable"),
        "the deadline signed under a root no worker could have been handed"
    );
    assert!(
        refused.is_err(),
        "a rotation began against a file no handshake presents"
    );
    assert_eq!(
        smeltr::leader::tls::rotation(&dir)
            .expect("readable")
            .map(|r| r.incoming),
        Some(rotation.incoming.clone()),
        "the rotation did not survive being held"
    );

    // Once they agree again the same deadline finishes it.
    assert!(
        smeltr::leader::tls::cut_over_if_due(&dir, names, a_year, None, deadline + 1).expect("readable"),
        "the deadline stopped deciding once the socket caught up"
    );
}

/// Whether the socket has what is on disk and what is on disk are one answer, not two. A reader told the two agree
/// and then left to read the files can be handed a set an upload swapped in between — an authority no handshake
/// presents, which the worker that pinned it reads as permanent.
#[test]
fn what_is_in_force_is_refused_whole_while_the_socket_has_not_taken_it() {
    let _alone = alone();
    let dir = temp_dir("agreed-read").join("tls");
    let names = vec!["localhost".to_string()];
    let a_year = Duration::from_secs(365 * 24 * 60 * 60);
    smeltr::leader::tls::ensure(&dir, names.clone(), a_year, &[]).expect("an authority");
    socket_caught_up(&dir);

    assert!(smeltr::leader::tls::current_if_agreed(&dir).is_ok(), "the two agree");

    smeltr::leader::tls::ensure_own(&dir, names, a_year).expect("a reissue");
    let whole = smeltr::leader::tls::current_if_agreed(&dir);
    // Put back before asserting: two numbers for the whole process, shared with every test in this file.
    socket_caught_up(&dir);

    let whole = whole.expect_err("the socket has not taken it");
    assert!(
        matches!(whole, smeltr::leader::tls::Refused::Conflict(_)),
        "a restart is the remedy, not a retry"
    );
}

/// An enrollment code names its root inside the value an operator pastes, so it cannot be re-pointed at what a rotation
/// is moving to. Once the cutover retires the root it names, a worker spending it fetches the authority, matches
/// nothing and retries against a leader that is answering correctly -- so the cutover withdraws it instead.
#[test]
fn a_cutover_withdraws_the_enrollment_codes_that_named_the_authority_it_retired() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_in();
    let agent = smeltr::heartbeat::agent_trusting(Some(&ca_pem), true, smeltr::heartbeat::REQUEST_BUDGET).unwrap();
    let doomed = mint(&base, &ca_pem, serde_json::json!({}));
    let doomed_id = doomed["id"].as_str().unwrap().to_string();

    let outstanding = |agent: &ureq::Agent| -> serde_json::Value {
        agent
            .get(format!("{base}/api/enrollment-codes"))
            .call()
            .unwrap()
            .body_mut()
            .read_json::<serde_json::Value>()
            .unwrap()
    };
    assert_eq!(
        outstanding(&agent)["codes"][0]["withdrawnAtMillis"],
        serde_json::Value::Null
    );

    let tls_dir = smeltr::leader::tls::dir_of(&data_dir);
    let now_ms = smeltr::leader::warnings::now_millis();
    smeltr::leader::tls::begin_rotation(&tls_dir, Duration::from_secs(60 * 60), now_ms, now_ms).expect("a rotation");

    // What the Enrollment codes page says about it during the overlap. The outgoing root still verifies this leader, so
    // this enrollment code works -- and stops working at the cutover. Neither "fine" nor "dead": an operator told the
    // first withdraws nothing and is stranded, and one told the second withdraws the enrollment code they could still
    // have spent.
    assert_eq!(
        outstanding(&agent)["codes"][0]["namesAuthority"],
        "untilCutover",
        "the page does not say this enrollment code has a window, and how long it is"
    );

    // The page an operator watches a rotation on. It is one of the two places a cutover happens -- this leader has
    // no workers to beat, so the deadline is what ends it -- and the withdrawal rides the same call.
    agent
        .get(format!("{base}/api/warnings"))
        .call()
        .expect("the warnings a rotation is watched on");

    assert!(
        smeltr::leader::tls::rotation(&tls_dir).unwrap().is_none(),
        "the rotation did not finish, so nothing here is about a cutover"
    );
    // What `main` says once its socket has loaded what the cutover wrote, so this leader will mint again.
    socket_caught_up(&tls_dir);
    // Named against the root just promoted. Outstanding beside nothing else: the listing works, so the enrollment code
    // that is missing from it is missing because it was withdrawn.
    let fresh = mint(&base, &ca_pem, serde_json::json!({}));
    let listed = outstanding(&agent);
    let codes = listed["codes"].as_array().unwrap();
    assert_eq!(
        codes.len(),
        1,
        "the enrollment code naming the retired authority still brings a worker in: {listed}"
    );
    assert_eq!(codes[0]["id"], fresh["id"]);
    assert_ne!(codes[0]["id"].as_str().unwrap(), doomed_id);
}

/// A switch of source is a rotation like the leader's own: the uploaded certificate's root reaches a running
/// worker on its beat, what is served moves only once it holds it, and the worker goes on being heard from through
/// the uploaded certificate -- and again through the leader's own when it is withdrawn.
#[test]
fn a_worker_follows_the_certificate_to_an_uploaded_one_and_back() {
    let _alone = alone();
    let (base, ca_pem, data_dir) = start_leader_tls_following(true);
    let dir = smeltr::leader::tls::dir_of(&data_dir);
    let names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let code = mint(&base, &ca_pem, serde_json::json!({}));
    let state_dir = temp_dir("follows");
    let child = spawn_worker(&base, code["value"].as_str().unwrap(), &state_dir);
    assert!(heartbeating(&base, &ca_pem, Duration::from_secs(20)), "never enrolled");

    let (chain, key, theirs) = external_pki("follows-theirs");
    let now_ms = smeltr::leader::warnings::now_millis();
    let entry = listed(&dir, &chain, &key, &theirs);
    let state = smeltr::leader::tls::begin_supplied_rotation(&dir, &entry, &names, &[], now_ms).expect("a switch");
    assert!(
        holds(&state_dir, &state.incoming, Duration::from_secs(20)),
        "the worker never took the uploaded certificate's root"
    );
    assert!(
        finished(&dir, Duration::from_secs(20)),
        "the fleet held the root and the leader never served the uploaded certificate"
    );
    assert_eq!(smeltr::leader::tls::served(&dir).unwrap(), Some(entry));
    // The shortened bundle reaches it on the beat after: a beat made over the uploaded certificate, which only
    // the root it was handed verifies.
    assert!(
        holds_only(&state_dir, &state.incoming, Duration::from_secs(20)),
        "the worker was not handed the bundle without the root the switch retired"
    );
    assert!(
        heartbeating(&base, &theirs, Duration::from_secs(20)),
        "the worker stopped reaching the leader once it served the uploaded certificate"
    );

    // And back. The leader's own is minted first, as a withdrawal always does.
    smeltr::leader::tls::ensure_own(&dir, names.clone(), Duration::from_secs(60 * 60)).expect("the leader's own");
    let back =
        smeltr::leader::tls::begin_own_rotation(&dir, smeltr::leader::warnings::now_millis()).expect("a switch back");
    assert!(
        holds(&state_dir, &back.incoming, Duration::from_secs(20)),
        "the worker never took the leader's own root back"
    );
    assert!(
        finished(&dir, Duration::from_secs(20)),
        "the leader never withdrew the uploaded certificate"
    );
    assert_eq!(smeltr::leader::tls::served(&dir).unwrap(), None);
    assert!(
        holds_only(&state_dir, &back.incoming, Duration::from_secs(20)),
        "the worker was not handed the bundle without the uploaded certificate's root"
    );
    assert!(
        heartbeating(&base, &ca_pem, Duration::from_secs(20)),
        "the worker stopped reaching the leader once it served its own certificate again"
    );
    stop(child);
}
