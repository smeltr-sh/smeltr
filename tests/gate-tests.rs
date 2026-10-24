//! The gate over the fully composed application, against a real socket.
//!
//! Composed by running the binary, not by assembling a router in the test: what the gate has to cover is every
//! route the process actually serves, and a router built here would only cover the ones this file remembered to
//! add. The bug this is written against is a route merged after the layer that was supposed to gate it, which is
//! invisible to anything but the whole thing.

use std::io::BufRead;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const ORIGIN_HOST: &str = "127.0.0.1";

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("smeltr-gate-{label}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A running leader, its console captured, killed on drop.
struct Leader {
    child: Child,
    console: Arc<Mutex<String>>,
    pub base: String,
    pub origin: String,
    /// The one-time password its bootstrap printed.
    pub password: String,
}

impl Drop for Leader {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a leader, wait for it to serve, and read the password it printed.
///
/// Plain HTTP on loopback, which is the one place a session cookie may travel unencrypted, and what lets this
/// test speak to it without a certificate.
/// How many leaders may bootstrap at once.
///
/// Each one creates its first account, which is a deliberately expensive Argon2id hash -- 64 MiB and three passes,
/// chosen so guessing is slow. Twenty of those at once on a four-core box starves them all, and the slowest misses
/// its readiness deadline for no reason but the others. Bounding the overlap costs a little wall clock and removes
/// the whole class of failure.
const CONCURRENT_STARTS: usize = 4;

static STARTS_FREE: std::sync::Mutex<usize> = std::sync::Mutex::new(CONCURRENT_STARTS);
static A_START_FINISHED: std::sync::Condvar = std::sync::Condvar::new();

/// Held from spawning a leader until it answers. Dropping it lets the next one begin.
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

fn start(label: &str) -> Leader {
    start_with(label, &[])
}

/// A leader with extra environment set, for the settings an operator can also put in the store.
fn start_with(label: &str, extra: &[(&str, &str)]) -> Leader {
    let _permit = StartPermit::acquire();
    // Each attempt gets its own directory as well as its own port: a leader that got as far as creating an
    // account wrote one, and a second attempt over the same data prints no password to wait for.
    let mut last = String::new();
    for _ in 0..4 {
        match serve(&scratch(label), extra) {
            Ok(leader) => return leader,
            Err(said) => {
                // The port was free when the OS named it and taken by the time the leader bound it. Rare, and
                // another port is the whole remedy. Recognized by what the leader itself says, since what the
                // OS calls this is a different sentence on every platform. Anything else is a leader that
                // failed to start for its own reasons, and trying again would only hide it.
                assert!(said.contains(BIND_FAILED), "the leader never served:\n{said}");
                last = said;
            }
        }
    }
    panic!("four ports in a row were taken before the leader could bind one:\n{last}");
}

/// What the leader says when it cannot bind: `main.rs` expects on it, so it is the same on every platform,
/// where the operating system's own wording for a taken port is not.
const BIND_FAILED: &str = "bind smeltr listen address";

/// A port the OS says is free. Reserved and released at once, so the leader about to start can bind it.
///
/// Asked for per leader rather than carved out of a range per process: nextest runs each test in a process of
/// its own, side by side, and a range wide enough for one is one the next has already started walking into.
fn free_port() -> u16 {
    std::net::TcpListener::bind((ORIGIN_HOST, 0))
        .expect("the OS names a free port")
        .local_addr()
        .expect("the port it named")
        .port()
}

/// One attempt at a serving leader. `Err` carries what it said, for a caller deciding whether to try again.
fn serve(dir: &std::path::Path, extra: &[(&str, &str)]) -> Result<Leader, String> {
    let port = free_port();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_smeltr"));
    cmd.current_dir(dir);
    for var in ["SMELTR_RECOVER_ADMIN", "SMELTR_CONFIG_FILE", "SMELTR_PUBLIC_ORIGIN"] {
        cmd.env_remove(var);
    }
    for (name, value) in extra {
        cmd.env(name, value);
    }
    cmd.env("SMELTR_LEADER_ENABLED", "1")
        .env("SMELTR_WORKER_ENABLED", "0")
        .env("SMELTR_LEADER_TLS", "off")
        .env("SMELTR_GUI_BIND", ORIGIN_HOST)
        .env("SMELTR_GUI_PORT", port.to_string())
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

    // Owned before it is waited for: a `Leader` that exists is one whose `Drop` kills the process, so the
    // assertion below can fail without leaving a leader running and a port held.
    let mut leader = Leader {
        child,
        console,
        base: format!("http://{ORIGIN_HOST}:{port}"),
        origin: format!("http://{ORIGIN_HOST}:{port}"),
        password: String::new(),
    };

    // The console says it is listening just before it binds, so the log is not the readiness signal -- a
    // connection is. Waiting on the log alone is a race this test lost once.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let said = leader.console();
        if said.contains("one-time password:") && std::net::TcpStream::connect((ORIGIN_HOST, port)).is_ok() {
            leader.password = said
                .lines()
                .find_map(|l| l.split_once("one-time password:"))
                .unwrap()
                .1
                .trim()
                .to_string();
            return Ok(leader);
        }
        // A leader that has exited will never serve, whatever the deadline says. Answered in milliseconds
        // rather than after a minute of asking a socket nobody is listening on.
        if leader.child.try_wait().is_ok_and(|exited| exited.is_some()) {
            // The reader thread may still be draining what it said as it went.
            std::thread::sleep(std::time::Duration::from_millis(50));
            return Err(leader.console());
        }
        if std::time::Instant::now() >= deadline {
            return Err(said);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

impl Leader {
    fn console(&self) -> String {
        self.console.lock().unwrap().clone()
    }

    /// Sign in and keep the cookie and the CSRF value, the way the GUI does.
    fn sign_in(&self, username: &str, password: &str) -> Session {
        let body = serde_json::json!({ "username": username, "password": password }).to_string();
        let response = ureq::post(format!("{}/api/auth/login", self.base))
            .header("origin", &self.origin)
            .header("content-type", "application/json")
            .send(&body)
            .expect("the login call was answered");
        assert_eq!(response.status(), 200, "sign-in refused");
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
        Session {
            cookie,
            csrf: dto["csrf"].as_str().expect("a csrf token").to_string(),
        }
    }
}

struct Session {
    cookie: String,
    csrf: String,
}

/// `GET path` with no credential at all.
fn anonymous_get(base: &str, path: &str) -> u16 {
    match ureq::get(format!("{base}{path}")).call() {
        Ok(r) => r.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("{path}: {e}"),
    }
}

fn status_of(result: Result<ureq::http::Response<ureq::Body>, ureq::Error>, what: &str) -> u16 {
    match result {
        Ok(r) => r.status().as_u16(),
        Err(ureq::Error::StatusCode(code)) => code,
        Err(e) => panic!("{what}: {e}"),
    }
}

/// Every operator route the leader serves, as a caller would name it. Read routes, so a refusal is the gate's
/// doing rather than a missing body's.
const OPERATOR_ROUTES: &[&str] = &[
    "/api/health",
    "/api/warnings",
    "/api/flow",
    "/api/streams",
    "/api/logs",
    "/api/workers",
    "/api/secrets",
    "/api/cluster",
    "/api/map",
    "/api/map/measurements",
    "/api/stats",
    "/api/config",
    "/api/pipelines",
    "/api/sources",
    "/api/samples",
    "/api/tests",
    "/api/branches",
    "/api/changes",
    "/api/history",
    "/api/worker-groups",
    "/api/users",
    "/api/security-events",
    "/api/pipeline-callers",
    "/api/connections?kind=pipeline&id=p1",
    "/api/settings/oidc",
];

/// The property the whole slice exists for.
#[test]
fn every_operator_route_refuses_an_anonymous_caller() {
    let leader = start("anonymous");
    for path in OPERATOR_ROUTES {
        let status = anonymous_get(&leader.base, path);
        assert_eq!(status, 401, "{path} answered {status} to a caller with no session");
    }
}

/// And answers with JSON, so a browser's `fetch` does not parse an HTML login page as its data.
#[test]
fn a_refusal_is_json_rather_than_the_apps_html() {
    let leader = start("json");
    let response = ureq::get(format!("{}/api/health", leader.base)).call();
    let Err(ureq::Error::StatusCode(401)) = response else {
        panic!("expected a 401");
    };
    // Fetched again to read the body, since `StatusCode` does not carry one.
    let response = ureq::get(format!("{}/api/health", leader.base))
        .config()
        .http_status_as_error(false)
        .build()
        .call()
        .unwrap();
    assert_eq!(response.headers()["content-type"], "application/json");
    let body: serde_json::Value = response.into_body().read_json().unwrap();
    assert_eq!(body["ok"], serde_json::Value::Bool(false));
}

/// An API path nothing serves used to return `index.html` with a `200`, because the SPA fallback covered every
/// unmatched path. A caller then parsed a web page as its data.
#[test]
fn an_unknown_api_path_is_a_json_404_rather_than_the_app() {
    let leader = start("unknown");
    let response = ureq::get(format!("{}/api/nothing-serves-this", leader.base))
        .config()
        .http_status_as_error(false)
        .build()
        .call()
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(response.headers()["content-type"], "application/json");
}

/// The app itself is still served, so a deep link into the GUI survives a refresh.
#[test]
fn a_page_route_still_reaches_the_app() {
    let leader = start("spa");
    let response = ureq::get(format!("{}/changes/branches", leader.base))
        .config()
        .http_status_as_error(false)
        .build()
        .call()
        .unwrap();
    assert!(response.status() == 200, "{}", response.status());
}

/// Signing in with the password bootstrap printed, and what that session may then do.
#[test]
fn the_bootstrap_password_signs_in_and_must_be_changed_first() {
    let leader = start("bootstrap-login");
    let session = leader.sign_in("admin", &leader.password);

    // A session that must change its password reads its own status and nothing else.
    let status = status_of(
        ureq::get(format!("{}/api/health", leader.base))
            .header("cookie", &session.cookie)
            .call(),
        "health",
    );
    assert_eq!(status, 403, "a must-change session loaded the fleet");

    let body: serde_json::Value = ureq::get(format!("{}/api/auth/session", leader.base))
        .header("cookie", &session.cookie)
        .call()
        .unwrap()
        .into_body()
        .read_json()
        .unwrap();
    assert_eq!(body["authenticated"], serde_json::Value::Bool(true));
    assert_eq!(body["mustChangePassword"], serde_json::Value::Bool(true));
    assert_eq!(body["username"], "admin");
}

/// The whole flow: sign in with the temporary password, choose one, sign in again, use the fleet.
#[test]
fn choosing_a_password_opens_the_rest_of_the_api() {
    let leader = start("password-flow");
    let first = leader.sign_in("admin", &leader.password);
    // A second session the account holds, which the change must end as well as the one that asked.
    let elsewhere = leader.sign_in("admin", &leader.password);

    let chosen = "a perfectly ordinary passphrase";
    let body = serde_json::json!({
        "username": "admin",
        "newPassword": chosen,
    })
    .to_string();
    let status = status_of(
        ureq::post(format!("{}/api/auth/password", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &first.cookie)
            .header("x-smeltr-csrf", &first.csrf)
            .header("content-type", "application/json")
            .send(&body),
        "password change",
    );
    assert_eq!(status, 200);

    // Changing a password ends every session the account holds, this one included. A session that survived would
    // still be a must-change one and answer 403, so 401 is the session gone.
    for (which, session) in [("the one that asked", &first), ("another", &elsewhere)] {
        assert_eq!(
            leader.get_as(session, "/api/health").0,
            401,
            "{which} survived the change"
        );
    }

    let second = leader.sign_in("admin", chosen);
    let status = status_of(
        ureq::get(format!("{}/api/health", leader.base))
            .header("cookie", &second.cookie)
            .call(),
        "health",
    );
    assert_eq!(status, 200, "a signed-in account could not read the fleet");
}

/// A cookie is attached by the browser whoever caused the request, so an unsafe one has to prove it came from the
/// GUI. Without the header it does not.
#[test]
fn an_unsafe_request_without_the_csrf_header_is_refused() {
    let leader = start("csrf");
    let session = signed_in_and_settled(&leader);
    let status = status_of(
        ureq::post(format!("{}/api/enrollment-codes", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("content-type", "application/json")
            .send("{}"),
        "mint an enrollment code",
    );
    assert_eq!(status, 403);
}

/// And with the header but from somewhere else, it does not either.
#[test]
fn an_unsafe_request_from_another_origin_is_refused() {
    let leader = start("origin");
    let session = signed_in_and_settled(&leader);
    for origin in ["https://evil.example.com", "http://127.0.0.1:1", ""] {
        let status = status_of(
            ureq::post(format!("{}/api/enrollment-codes", leader.base))
                .header("origin", origin)
                .header("cookie", &session.cookie)
                .header("x-smeltr-csrf", &session.csrf)
                .header("content-type", "application/json")
                .send("{}"),
            "mint an enrollment code",
        );
        assert_eq!(status, 403, "origin {origin:?} was accepted");
    }
}

/// With both, it works -- otherwise the two tests above would pass on a route that never works at all.
#[test]
fn an_unsafe_request_from_the_gui_is_allowed() {
    let leader = start("csrf-ok");
    let session = signed_in_and_settled(&leader);
    let status = status_of(
        ureq::post(format!("{}/api/enrollment-codes", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("x-smeltr-csrf", &session.csrf)
            .header("content-type", "application/json")
            .send("{}"),
        "mint an enrollment code",
    );
    assert_eq!(status, 200);
}

/// A login cannot be caused from another page either. There is no session yet to hold a CSRF token, so the exact
/// origin is what stands in its place.
#[test]
fn a_login_from_another_origin_is_refused() {
    let leader = start("login-origin");
    let body = serde_json::json!({ "username": "admin", "password": leader.password }).to_string();
    for origin in [Some("https://evil.example.com"), None] {
        let mut request =
            ureq::post(format!("{}/api/auth/login", leader.base)).header("content-type", "application/json");
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        assert_eq!(status_of(request.send(&body), "login"), 403, "{origin:?}");
    }
}

/// A form can be posted cross-site and carries no JSON content type, so requiring one closes that door too.
#[test]
fn a_login_that_is_not_json_is_refused() {
    let leader = start("login-form");
    let status = status_of(
        ureq::post(format!("{}/api/auth/login", leader.base))
            .header("origin", &leader.origin)
            .header("content-type", "application/x-www-form-urlencoded")
            .send("username=admin&password=whatever"),
        "login",
    );
    assert_eq!(status, 403);
}

/// One answer for a wrong password and an account that does not exist.
#[test]
fn a_bad_sign_in_says_nothing_about_which_part_was_wrong() {
    let leader = start("generic");
    let mut bodies = Vec::new();
    for (username, password) in [
        ("admin", "not the password at all"),
        ("nobody", "not the password at all"),
    ] {
        let body = serde_json::json!({ "username": username, "password": password }).to_string();
        let response = ureq::post(format!("{}/api/auth/login", leader.base))
            .header("origin", &leader.origin)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(&body)
            .unwrap();
        assert_eq!(response.status(), 401);
        bodies.push(response.into_body().read_to_string().unwrap());
    }
    assert_eq!(bodies[0], bodies[1], "the answers differed");
}

/// Guessing is bounded. The sixth attempt against one username inside a minute is refused before a hash runs.
#[test]
fn sign_in_attempts_are_throttled() {
    let leader = start("throttle");
    let body = serde_json::json!({ "username": "admin", "password": "not the password at all" }).to_string();
    let mut statuses = Vec::new();
    for _ in 0..8 {
        let status = status_of(
            ureq::post(format!("{}/api/auth/login", leader.base))
                .header("origin", &leader.origin)
                .header("content-type", "application/json")
                .send(&body),
            "login",
        );
        statuses.push(status);
    }
    assert!(statuses.contains(&429), "nothing was throttled: {statuses:?}");
    assert_eq!(*statuses.last().unwrap(), 429);
}

/// Behind a proxy terminating TLS (TLS off here, an https public origin) every browser arrives from the proxy's
/// address, so the per-address bucket is not counted: a stranger's thirty bad guesses a minute do not lock every
/// operator out of signing in. Each name is still bounded by its own bucket.
#[test]
fn behind_a_proxy_guesses_are_bounded_per_name_and_not_per_address() {
    let origin = "https://leader.example.com";
    let leader = start_with("proxied", &[("SMELTR_PUBLIC_ORIGIN", origin)]);
    let attempt = |username: &str| {
        let body = serde_json::json!({ "username": username, "password": "not the password at all" }).to_string();
        status_of(
            ureq::post(format!("{}/api/auth/login", leader.base))
                .header("origin", origin)
                .header("content-type", "application/json")
                .send(&body),
            "login",
        )
    };
    for i in 0..31 {
        assert_eq!(attempt(&format!("stranger-{i}")), 401, "attempt {i}");
    }
    let statuses: Vec<u16> = (0..6).map(|_| attempt("admin")).collect();
    assert_eq!(*statuses.last().unwrap(), 429, "{statuses:?}");
}

/// Worker routes keep their own credential. A human cookie is worth nothing there, and the enrollment code is
/// still what enrollment takes.
#[test]
fn a_human_cookie_does_not_open_a_worker_route() {
    let leader = start("worker-boundary");
    let session = signed_in_and_settled(&leader);
    let status = status_of(
        ureq::post(format!("{}/api/heartbeat", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("x-smeltr-csrf", &session.csrf)
            .header("content-type", "application/json")
            .send("{}"),
        "heartbeat",
    );
    assert_eq!(status, 401, "a browser session reached the worker protocol");
}

/// A bearer token is not a session either: the header the old gate accepted is no longer read at all.
#[test]
fn a_bearer_token_is_not_a_session() {
    let leader = start("bearer");
    let status = status_of(
        ureq::get(format!("{}/api/health", leader.base))
            .header("authorization", "Bearer whatever-it-used-to-be")
            .call(),
        "health",
    );
    assert_eq!(status, 401);
}

/// The status route answers anonymously rather than refusing: it is what the GUI asks to find out whether there
/// is anyone to be.
#[test]
fn the_session_route_answers_an_anonymous_browser() {
    let leader = start("anonymous-session");
    let response = ureq::get(format!("{}/api/auth/session", leader.base)).call().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: serde_json::Value = response.into_body().read_json().unwrap();
    assert_eq!(body["authenticated"], serde_json::Value::Bool(false));
    assert!(body.get("csrf").is_none(), "a CSRF token was handed to nobody");
}

/// Signing out ends the session it was made with.
#[test]
fn signing_out_ends_the_session() {
    let leader = start("logout");
    let session = signed_in_and_settled(&leader);
    let status = status_of(
        ureq::post(format!("{}/api/auth/logout", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("x-smeltr-csrf", &session.csrf)
            .send_empty(),
        "logout",
    );
    assert_eq!(status, 200);
    let after = status_of(
        ureq::get(format!("{}/api/health", leader.base))
            .header("cookie", &session.cookie)
            .call(),
        "health",
    );
    assert_eq!(after, 401);
}

/// What `signed_in_and_settled` leaves the bootstrap account's password as.
const SETTLED_PASSWORD: &str = "a perfectly ordinary passphrase";

/// Sign in and get past the must-change password, leaving a session that may use the API.
fn signed_in_and_settled(leader: &Leader) -> Session {
    let first = leader.sign_in("admin", &leader.password);
    let chosen = SETTLED_PASSWORD;
    let body = serde_json::json!({
        "username": "admin",
        "newPassword": chosen,
    })
    .to_string();
    ureq::post(format!("{}/api/auth/password", leader.base))
        .header("origin", &leader.origin)
        .header("cookie", &first.cookie)
        .header("x-smeltr-csrf", &first.csrf)
        .header("content-type", "application/json")
        .send(&body)
        .expect("the password change was answered");
    leader.sign_in("admin", chosen)
}

// -- managing accounts ------------------------------------------------------

/// A signed-in administrator and their CSRF value.
struct Operator {
    session: Session,
}

fn operator(leader: &Leader) -> Operator {
    Operator {
        session: signed_in_and_settled(leader),
    }
}

impl Leader {
    fn post_as(&self, op: &Operator, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let response = ureq::post(format!("{}{path}", self.base))
            .header("origin", &self.origin)
            .header("cookie", &op.session.cookie)
            .header("x-smeltr-csrf", &op.session.csrf)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(&body.to_string())
            .expect("the call was answered");
        let status = response.status().as_u16();
        let parsed = response.into_body().read_json().unwrap_or(serde_json::Value::Null);
        (status, parsed)
    }

    fn patch_as(&self, op: &Operator, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let response = ureq::patch(format!("{}{path}", self.base))
            .header("origin", &self.origin)
            .header("cookie", &op.session.cookie)
            .header("x-smeltr-csrf", &op.session.csrf)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(&body.to_string())
            .expect("the call was answered");
        let status = response.status().as_u16();
        let parsed = response.into_body().read_json().unwrap_or(serde_json::Value::Null);
        (status, parsed)
    }

    fn put_as(&self, op: &Operator, path: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
        let response = ureq::put(format!("{}{path}", self.base))
            .header("origin", &self.origin)
            .header("cookie", &op.session.cookie)
            .header("x-smeltr-csrf", &op.session.csrf)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(&body.to_string())
            .expect("the call was answered");
        let status = response.status().as_u16();
        let parsed = response.into_body().read_json().unwrap_or(serde_json::Value::Null);
        (status, parsed)
    }

    fn delete_as(&self, op: &Operator, path: &str) -> u16 {
        let response = ureq::delete(format!("{}{path}", self.base))
            .header("origin", &self.origin)
            .header("cookie", &op.session.cookie)
            .header("x-smeltr-csrf", &op.session.csrf)
            .config()
            .http_status_as_error(false)
            .build()
            .call()
            .expect("the call was answered");
        response.status().as_u16()
    }

    fn get_as(&self, session: &Session, path: &str) -> (u16, serde_json::Value) {
        let response = ureq::get(format!("{}{path}", self.base))
            .header("cookie", &session.cookie)
            .config()
            .http_status_as_error(false)
            .build()
            .call()
            .expect("the call was answered");
        let status = response.status().as_u16();
        let parsed = response.into_body().read_json().unwrap_or(serde_json::Value::Null);
        (status, parsed)
    }
}

/// A self-signed certificate for `name` and its private key, in PEM.
fn certificate_and_key(name: &str) -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
    (params.self_signed(&key).unwrap().pem(), key.serialize_pem())
}

/// A certificate is uploaded to the list, kept out of the secrets, replaced by a new version and deleted. A key that
/// is not the certificate's is refused before anything is stored.
#[test]
fn an_administrator_keeps_certificates_in_the_list() {
    let leader = start("certificate-list");
    let op = operator(&leader);
    let (certificate, private_key) = certificate_and_key("receiver.example.com");
    let (_, other_key) = certificate_and_key("other.example.com");

    let upload = |key: &str| serde_json::json!({"name": "receiver", "certificate": certificate, "privateKey": key});
    let (_, refused) = leader.post_as(&op, "/api/certificates", upload(&other_key));
    assert_eq!(refused["ok"], false, "a key that is not the certificate's: {refused}");
    let (_, created) = leader.post_as(&op, "/api/certificates", upload(&private_key));
    assert_eq!(created["ok"], true, "{created}");

    let (status, listed) = leader.get_as(&op.session, "/api/certificates");
    assert_eq!(status, 200);
    assert_eq!(listed[0]["name"], "receiver");
    assert_eq!(listed[0]["chain"][0]["subject"], "rcgen self signed cert");
    assert_eq!(listed[0]["served"], false);
    let (_, secrets) = leader.get_as(&op.session, "/api/secrets");
    assert!(
        !secrets.to_string().contains("receiver"),
        "a certificate is not listed with the secrets: {secrets}"
    );
    let (_, value) = leader.put_as(&op, "/api/secrets/receiver", serde_json::json!({"value": "x"}));
    assert_eq!(
        value["ok"], false,
        "a certificate's value is not set as a secret's: {value}"
    );
    let verify = serde_json::json!({"version": 1, "candidate": private_key});
    let (status, _) = leader.post_as(&op, "/api/secrets/receiver/verify", verify);
    assert_eq!(status, 409, "a certificate's key is not verified as a secret's");

    let (renewed, renewed_key) = certificate_and_key("receiver.example.com");
    let body = serde_json::json!({"certificate": renewed, "privateKey": renewed_key});
    let (_, replaced) = leader.put_as(&op, "/api/certificates/receiver", body);
    assert_eq!(replaced["ok"], true, "{replaced}");
    let (_, listed) = leader.get_as(&op.session, "/api/certificates");
    assert_eq!(listed[0]["versions"].as_array().map(Vec::len), Some(2));

    assert_eq!(leader.delete_as(&op, "/api/secrets/receiver"), 200);
    let (_, listed) = leader.get_as(&op.session, "/api/certificates");
    assert_eq!(listed, serde_json::json!([]));
}

/// The whole account lifecycle, from an administrator creating one to the new person using it.
#[test]
fn an_administrator_creates_an_account_that_can_then_sign_in() {
    let leader = start("create-account");
    let op = operator(&leader);

    let (status, created) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana Okafor" }),
    );
    assert_eq!(status, 201, "{created}");
    let temporary = created["temporaryPassword"].as_str().expect("a temporary password");

    // The new account signs in, and has to choose a password before it can do anything.
    let theirs = leader.sign_in("dana", temporary);
    let (status, _) = leader.get_as(&theirs, "/api/health");
    assert_eq!(
        status, 403,
        "a new account reached the fleet before choosing a password"
    );

    let (_, session) = leader.get_as(&theirs, "/api/auth/session");
    assert_eq!(session["mustChangePassword"], serde_json::Value::Bool(true));
    assert_eq!(session["username"], "dana");
    assert_eq!(session["displayName"], "Dana Okafor");
}

/// A model endpoint in this process speaking the Anthropic Messages API, taking one key.
struct FakeModel {
    base: String,
    /// Each request the agent's turns sent.
    asked: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    _runtime: tokio::runtime::Runtime,
}

const MODEL_KEY: &str = "sk-model-key";

/// The context the fake endpoint's Models API says each model holds.
const MODEL_CONTEXT: u64 = 1_000_000;

impl FakeModel {
    fn start() -> FakeModel {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind((ORIGIN_HOST, 0)))
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = asked.clone();
        let app = axum::Router::new()
            .route(
                "/v1/models",
                axum::routing::get(|| async {
                    axum::Json(serde_json::json!({
                        "data": [
                            {"id": "claude-opus-5-5", "display_name": "Claude Opus 5.5", "max_input_tokens": MODEL_CONTEXT,
                             "capabilities": {"effort": {"supported": true, "low": {"supported": true},
                                 "medium": {"supported": true}, "high": {"supported": true},
                                 "xhigh": {"supported": true}, "max": {"supported": true}}}},
                            {"id": "claude-haiku-5-5", "display_name": "Claude Haiku 5.5", "max_input_tokens": 200_000},
                        ],
                        "has_more": false,
                    }))
                }),
            )
            .route(
                "/v1/models/:id",
                axum::routing::get(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                    let holds = if id.contains("haiku") { 200_000 } else { MODEL_CONTEXT };
                    axum::Json(serde_json::json!({"type": "model", "id": id, "max_input_tokens": holds}))
                }),
            )
            .route(
                "/v1/messages",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| async move {
                        use axum::response::IntoResponse;
                        if body.get("tools").is_some() {
                            kept.lock().unwrap().push(body.clone());
                        }
                        if headers.get("x-api-key").and_then(|v| v.to_str().ok()) != Some(MODEL_KEY) {
                            return (
                                axum::http::StatusCode::UNAUTHORIZED,
                                axum::Json(
                                    serde_json::json!({"type": "error", "error": {"message": "invalid x-api-key"}}),
                                ),
                            )
                                .into_response();
                        }
                        let answer = FakeModel::answer(&body);
                        match body["stream"] == true {
                            true => (
                                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                                FakeModel::streamed(&answer),
                            )
                                .into_response(),
                            false => axum::Json(answer).into_response(),
                        }
                    },
                ),
            );
        runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
        FakeModel {
            base,
            asked,
            _runtime: runtime,
        }
    }
}

impl FakeModel {
    /// `answer` as Anthropic streams it: each block opened, its text in two pieces or its input whole, and closed.
    fn streamed(answer: &serde_json::Value) -> String {
        let mut events = vec![serde_json::json!({"type": "message_start", "message": {
            "role": "assistant", "content": [], "usage": {"input_tokens": answer["usage"]["input_tokens"], "output_tokens": 0},
        }})];
        for (index, block) in answer["content"].as_array().unwrap().iter().enumerate() {
            let (start, deltas) = match block["text"].as_str() {
                Some(text) => {
                    let (a, b) = text.split_at(text.len() / 2);
                    let delta = |t: &str| serde_json::json!({"type": "text_delta", "text": t});
                    (
                        serde_json::json!({"type": "text", "text": ""}),
                        vec![delta(a), delta(b)],
                    )
                }
                None => {
                    let mut start = block.clone();
                    start["input"] = serde_json::json!({});
                    let json =
                        serde_json::json!({"type": "input_json_delta", "partial_json": block["input"].to_string()});
                    (start, vec![json])
                }
            };
            events.push(serde_json::json!({"type": "content_block_start", "index": index, "content_block": start}));
            for delta in deltas {
                events.push(serde_json::json!({"type": "content_block_delta", "index": index, "delta": delta}));
            }
            events.push(serde_json::json!({"type": "content_block_stop", "index": index}));
        }
        events.push(
            serde_json::json!({"type": "message_delta", "delta": {"stop_reason": answer["stop_reason"]},
            "usage": {"output_tokens": answer["usage"]["output_tokens"]}}),
        );
        events.push(serde_json::json!({"type": "message_stop"}));
        events
            .iter()
            .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
            .collect()
    }

    /// "ok" to the check made when AI agents are turned on. To an agent's turn, one call: a write when the person
    /// said "write", a read otherwise; then, given its result, the status it came back with.
    fn answer(body: &serde_json::Value) -> serde_json::Value {
        let text = |text: String| {
            serde_json::json!({
                "content": [{"type": "text", "text": text}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 5, "output_tokens": 1},
            })
        };
        if body.get("tools").is_none() {
            return text("ok".to_string());
        }
        let last = &body["messages"].as_array().unwrap().last().unwrap()["content"];
        let said: Vec<&serde_json::Value> = last.as_array().unwrap().iter().collect();
        if let Some(result) = said.iter().find(|c| c["type"] == "tool_result") {
            let status = result["content"].as_str().unwrap().lines().next().unwrap().to_string();
            return text(format!("It answered {status}."));
        }
        let asked = said.iter().filter_map(|c| c["text"].as_str()).collect::<String>();
        if asked.contains("Summarize the conversation") {
            return text("They read what runs in shared.".to_string());
        }
        let input = match asked.contains("write") {
            true => serde_json::json!({
                "method": "PUT", "path": "/api/pipelines/p1", "query": {"scope": "shared", "branch": "agent-work"},
                "body": {"id": "p1", "name": "p1", "steps": []},
            }),
            false => serde_json::json!({"method": "GET", "path": "/api/pipelines", "query": {"scope": "shared"}}),
        };
        serde_json::json!({
            "content": [{"type": "tool_use", "id": "tu_1", "name": "request", "input": input}],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 5, "output_tokens": 1},
        })
    }
}

/// Turn AI agents on, against `model`.
fn agents_on(leader: &Leader, op: &Operator, model: &FakeModel) {
    let (status, body) = leader.put_as(
        op,
        "/api/settings/agents",
        serde_json::json!({
            "enabled": true, "provider": "anthropic", "endpoint": model.base, "model": "claude-opus-5-5",
            "apiKey": MODEL_KEY,
        }),
    );
    assert_eq!((status, body["ok"].clone()), (200, serde_json::json!(true)), "{body}");
}

/// While AI agents are off, nothing an agent may do can be changed, and the session says they are off.
#[test]
fn the_agent_apis_wait_until_ai_agents_are_turned_on() {
    let leader = start("agents-off");
    let op = operator(&leader);
    let (_, session) = leader.get_as(&op.session, "/api/auth/session");
    assert_eq!(session["agents"], false);
    let (status, body) = leader.put_as(
        &op,
        "/api/me/agent-permissions",
        serde_json::json!({ "kind": "everything" }),
    );
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "AI agents are not turned on");
    let (status, _) = leader.get_as(&op.session, "/api/me/agent-permissions");
    assert_eq!(status, 200);

    let model = FakeModel::start();
    agents_on(&leader, &op, &model);
    let (_, session) = leader.get_as(&op.session, "/api/auth/session");
    assert_eq!(session["agents"], true);
}

/// Turning AI agents on asks the endpoint first; the key is never read back, and is forgotten with its endpoint.
#[test]
fn ai_agents_turn_on_only_with_an_endpoint_that_answers() {
    let leader = start("agents-settings");
    let op = operator(&leader);
    let model = FakeModel::start();
    let put = |body: serde_json::Value| leader.put_as(&op, "/api/settings/agents", body).1;

    let body = put(serde_json::json!({
        "enabled": true, "provider": "anthropic", "endpoint": model.base, "model": "claude-opus-5-5",
    }));
    assert_eq!(body["ok"], false, "{body}");
    assert_eq!(body["errors"][0]["message"], "Turning AI agents on needs: API key.");

    let body = put(serde_json::json!({
        "enabled": true, "provider": "anthropic", "endpoint": model.base, "model": "claude-opus-5-5",
        "apiKey": "sk-wrong",
    }));
    assert_eq!(body["ok"], false, "{body}");
    assert!(
        body["errors"][0]["message"]
            .as_str()
            .unwrap()
            .ends_with("answered 401: invalid x-api-key"),
        "{body}"
    );
    assert_eq!(body["settings"]["enabled"], false);
    assert_eq!(body["settings"]["apiKeySet"], false);

    agents_on(&leader, &op, &model);
    let (_, read) = leader.get_as(&op.session, "/api/settings/agents");
    assert_eq!(read["settings"]["apiKeySet"], true, "{read}");
    // Anthropic says how much its model holds.
    assert_eq!(read["settings"]["contextTokens"], MODEL_CONTEXT, "{read}");
    assert!(!read.to_string().contains(MODEL_KEY), "the key came back: {read}");

    // Saved again with no key, it keeps the one it holds.
    let body = put(serde_json::json!({
        "enabled": true, "provider": "anthropic", "endpoint": model.base, "model": "claude-opus-5-5",
    }));
    assert_eq!(body["ok"], true, "{body}");

    // Another endpoint forgets it, and that leaves AI agents off.
    let body = put(serde_json::json!({
        "enabled": false, "provider": "openai-compatible", "endpoint": format!("{}/v1", model.base), "model": "m",
        "contextTokens": 32_768,
    }));
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(body["settings"]["apiKeySet"], false, "{body}");
    assert_eq!(body["settings"]["contextTokens"], 32_768, "{body}");
    let body = put(serde_json::json!({
        "enabled": false, "provider": "openai-compatible", "endpoint": format!("{}/v1", model.base), "model": "m",
        "contextTokens": 100,
    }));
    assert_eq!(
        body["errors"][0]["message"], "a context window is from 4096 to 10000000 tokens",
        "{body}"
    );
    let (_, session) = leader.get_as(&op.session, "/api/auth/session");
    assert_eq!(session["agents"], false);

    let (_, events) = leader.get_as(&op.session, "/api/security-events");
    assert!(
        events.to_string().contains("agents-change"),
        "no agents-change row: {events}"
    );
}

/// A person reads their agent permissions, viewer until they say otherwise, and sets it.
#[test]
fn a_person_sets_what_their_agent_may_use() {
    let leader = start("agent-permissions");
    let op = operator(&leader);
    let model = FakeModel::start();
    agents_on(&leader, &op, &model);

    let (status, permissions) = leader.get_as(&op.session, "/api/me/agent-permissions");
    assert_eq!(status, 200);
    assert_eq!(
        permissions,
        serde_json::json!({ "kind": "upto", "default": "viewer", "areas": {} })
    );
    let (status, _) = leader.put_as(
        &op,
        "/api/me/agent-permissions",
        serde_json::json!({ "kind": "everything" }),
    );
    assert_eq!(status, 200);
    let (_, permissions) = leader.get_as(&op.session, "/api/me/agent-permissions");
    assert_eq!(permissions, serde_json::json!({ "kind": "everything" }));
    let (status, _) = leader.put_as(
        &op,
        "/api/me/agent-permissions",
        serde_json::json!({ "kind": "upto", "default": "viewer", "areas": { "../x": "editor" } }),
    );
    assert_eq!(status, 400);
}

/// The conversation `id` once its turn has ended.
fn turn_ended(leader: &Leader, op: &Operator, id: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let (status, body) = leader.get_as(&op.session, &format!("/api/agent/conversations/{id}"));
        assert_eq!(status, 200, "{body}");
        if body["conversation"]["running"] == false {
            return body["conversation"].clone();
        }
        assert!(std::time::Instant::now() < deadline, "the turn did not end: {body}");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// A person's agent calls the API through the gate as them, held to what they let it use, and its conversation
/// is theirs alone.
#[test]
fn a_persons_agent_calls_the_api_as_them_within_what_they_let_it() {
    let leader = start("agent-chat");
    assert_eq!(anonymous_get(&leader.base, "/api/agent/conversations"), 401);
    let op = operator(&leader);
    let model = FakeModel::start();
    agents_on(&leader, &op, &model);

    let (status, started) = leader.post_as(
        &op,
        "/api/agent/conversations",
        serde_json::json!({ "message": "What runs in shared?" }),
    );
    assert_eq!(status, 202, "{started}");
    let id = started["id"].as_str().unwrap().to_string();
    let read = turn_ended(&leader, &op, &id);
    let kinds: Vec<&str> = read["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["message", "call", "result", "answer"], "{read}");
    assert_eq!(read["items"][3]["text"], "It answered 200.", "{read}");
    assert_eq!(read["ended"], "answered");

    // Their agent may only read until they say otherwise, however much they hold.
    let (status, _) = leader.post_as(
        &op,
        &format!("/api/agent/conversations/{id}/messages"),
        serde_json::json!({ "message": "Now write a pipeline." }),
    );
    assert_eq!(status, 202);
    let read = turn_ended(&leader, &op, &id);
    let result = &read["items"][6];
    assert_eq!(result["kind"], "result", "{read}");
    assert_eq!(result["isError"], true, "{read}");
    assert!(result["output"].as_str().unwrap().starts_with("403\n"), "{read}");

    let (_, listed) = leader.get_as(&op.session, "/api/agent/conversations");
    assert_eq!(listed["conversations"][0]["title"], "What runs in shared?");

    // The person picks from every model the key reaches; a conversation goes on with what made its last answer.
    let (status, models) = leader.get_as(&op.session, "/api/agent/models");
    assert_eq!(status, 200, "{models}");
    assert_eq!(models["default"], "claude-opus-5-5");
    assert_eq!(models["models"][1]["id"], "claude-haiku-5-5", "{models}");
    assert_eq!(models["models"][0]["efforts"].as_array().unwrap().len(), 5, "{models}");
    assert_eq!(read["model"], "claude-opus-5-5", "{read}");
    assert_eq!(read["context"]["window"], MODEL_CONTEXT, "{read}");
    let (status, _) = leader.post_as(
        &op,
        &format!("/api/agent/conversations/{id}/messages"),
        serde_json::json!({ "message": "Read it again.", "model": "claude-haiku-5-5", "effort": "low" }),
    );
    assert_eq!(status, 202);
    let read = turn_ended(&leader, &op, &id);
    assert_eq!(
        (&read["model"], &read["effort"]),
        (&serde_json::json!("claude-haiku-5-5"), &serde_json::json!("low"))
    );
    assert_eq!(read["context"]["window"], 200_000, "{read}");
    let (status, _) = leader.post_as(
        &op,
        &format!("/api/agent/conversations/{id}/messages"),
        serde_json::json!({ "message": "And once more." }),
    );
    assert_eq!(status, 202);
    turn_ended(&leader, &op, &id);
    let last = model.asked.lock().unwrap().last().cloned().unwrap();
    assert_eq!(
        (&last["model"], &last["output_config"]["effort"]),
        (&serde_json::json!("claude-haiku-5-5"), &serde_json::json!("low")),
        "{last}"
    );

    // Summarized, the model reads the summary in place of what came before; cleared, nothing.
    let context = |summarize: bool| {
        let (status, body) = leader.post_as(
            &op,
            &format!("/api/agent/conversations/{id}/context"),
            serde_json::json!({ "summarize": summarize }),
        );
        (status, body)
    };
    assert_eq!(context(true).0, 202);
    let read = turn_ended(&leader, &op, &id);
    let fresh = read["items"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(
        fresh,
        serde_json::json!({"kind": "fresh", "summary": "They read what runs in shared."}),
        "{read}"
    );
    assert_eq!(read["context"]["used"], 1, "{read}");
    assert_eq!(context(false).0, 409, "nothing was said since");
    let (status, _) = leader.post_as(
        &op,
        &format!("/api/agent/conversations/{id}/messages"),
        serde_json::json!({ "message": "What next?" }),
    );
    assert_eq!(status, 202);
    turn_ended(&leader, &op, &id);
    let first = model.asked.lock().unwrap().last().cloned().unwrap()["messages"][0].clone();
    assert_eq!(
        first["content"][0]["text"], "Summary of the conversation before this:\nThey read what runs in shared.",
        "{first}"
    );
    assert_eq!(context(false).0, 202);
    let read = turn_ended(&leader, &op, &id);
    assert_eq!(read["context"]["used"], 0, "{read}");
    assert_eq!(
        read["items"].as_array().unwrap().last().unwrap()["summary"],
        serde_json::Value::Null
    );

    let other = second_operator(&leader, &op, "bea", "Bea");
    let (status, _) = leader.get_as(&other.session, &format!("/api/agent/conversations/{id}"));
    assert_eq!(status, 404);
    let (_, listed) = leader.get_as(&other.session, "/api/agent/conversations");
    assert_eq!(listed["conversations"], serde_json::json!([]));
    assert_eq!(leader.delete_as(&other, &format!("/api/agent/conversations/{id}")), 404);

    assert_eq!(leader.delete_as(&op, &format!("/api/agent/conversations/{id}")), 200);
    let (status, _) = leader.get_as(&op.session, &format!("/api/agent/conversations/{id}"));
    assert_eq!(status, 404);
}

/// A username is taken once, whatever case it is typed in.
#[test]
fn a_username_cannot_be_taken_twice() {
    let leader = start("duplicate-username");
    let op = operator(&leader);
    let create = |username: &str| {
        leader.post_as(
            &op,
            "/api/users",
            serde_json::json!({ "username": username, "displayName": "Someone" }),
        )
    };
    assert_eq!(create("dana").0, 201);
    assert_eq!(create("dana").0, 409);
    assert_eq!(create("DANA").0, 409, "a case variant is the same account");
    assert_eq!(create("admin").0, 409, "the bootstrap account's name is taken too");
}

/// A rename answers with the account as it now stands, login name included.
#[test]
fn a_renamed_account_keeps_its_username() {
    let leader = start("rename");
    let op = operator(&leader);
    let (_, created) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana" }),
    );
    let id = created["userId"].as_str().unwrap();
    let (status, patched) = leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "displayName": "Dana Okafor" }),
    );
    assert_eq!(status, 200);
    assert_eq!(patched["displayName"], "Dana Okafor");
    // A client replaces its roster entry with this response, so it must still carry the login name.
    assert_eq!(patched["username"], "dana", "{patched}");
}

/// The last way in cannot be closed. A leader whose only administrator disabled themselves is one nobody can
/// reach, and the remedy would be stopping it and recovering from the host.
#[test]
fn the_last_administrator_cannot_disable_themselves() {
    let leader = start("last-admin");
    let op = operator(&leader);
    let (_, users) = leader.get_as(&op.session, "/api/users");
    let id = users[0]["id"].as_str().unwrap();
    let (status, body) = leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "enabled": false }),
    );
    assert_eq!(status, 409, "{body}");
}

/// Disabling somebody denies their very next request, rather than waiting for their session to expire.
#[test]
fn disabling_an_account_ends_its_session_at_once() {
    let leader = start("disable-session");
    let op = operator(&leader);
    let (_, created) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana" }),
    );
    let id = created["userId"].as_str().unwrap().to_string();
    let temporary = created["temporaryPassword"].as_str().unwrap().to_string();

    // Get them a working session first, so what is being withdrawn is real access rather than a restricted one.
    let theirs = leader.sign_in("dana", &temporary);
    let chosen = "another perfectly ordinary passphrase";
    leader.post_as(
        &Operator {
            session: Session {
                cookie: theirs.cookie.clone(),
                csrf: theirs.csrf.clone(),
            },
        },
        "/api/auth/password",
        serde_json::json!({ "username": "dana", "newPassword": chosen }),
    );
    let theirs = leader.sign_in("dana", chosen);
    assert_eq!(
        leader.get_as(&theirs, "/api/health").0,
        200,
        "they could not use the API"
    );

    leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "enabled": false }),
    );
    assert_eq!(
        leader.get_as(&theirs, "/api/health").0,
        401,
        "a disabled account kept its session"
    );

    // And re-enabling does not revive it: the session is gone, not suspended.
    leader.patch_as(&op, &format!("/api/users/{id}"), serde_json::json!({ "enabled": true }));
    assert_eq!(leader.get_as(&theirs, "/api/health").0, 401, "an old session came back");
}

/// Revoking ends every session an account holds without touching its password, so they sign in again with what
/// they already know.
#[test]
fn revoking_sessions_ends_them_and_leaves_the_password_alone() {
    let leader = start("revoke");
    let op = operator(&leader);
    let (_, users) = leader.get_as(&op.session, "/api/users");
    let id = users[0]["id"].as_str().unwrap().to_string();

    let (status, _) = leader.post_as(&op, &format!("/api/users/{id}/revoke-sessions"), serde_json::json!({}));
    assert_eq!(status, 200);
    assert_eq!(
        leader.get_as(&op.session, "/api/health").0,
        401,
        "the revoked session still worked"
    );
    // The password is untouched, so signing in again works with the one they already have.
    leader.sign_in("admin", SETTLED_PASSWORD);
}

/// A reset gives somebody a password they did not choose, so they must choose one before doing anything.
#[test]
fn resetting_a_password_ends_the_sessions_and_requires_a_new_choice() {
    let leader = start("reset");
    let op = operator(&leader);
    let (_, created) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana" }),
    );
    let id = created["userId"].as_str().unwrap().to_string();
    let before = leader.sign_in("dana", created["temporaryPassword"].as_str().unwrap());

    let (status, reset) = leader.post_as(&op, &format!("/api/users/{id}/reset-password"), serde_json::json!({}));
    assert_eq!(status, 200, "{reset}");
    assert_eq!(reset["username"], "dana");
    // A session that survived would still be a must-change one and answer 403, so 401 is the session gone.
    assert_eq!(
        leader.get_as(&before, "/api/health").0,
        401,
        "a session outlived the reset"
    );
    let fresh = reset["temporaryPassword"].as_str().unwrap();

    let theirs = leader.sign_in("dana", fresh);
    let (_, session) = leader.get_as(&theirs, "/api/auth/session");
    assert_eq!(session["mustChangePassword"], serde_json::Value::Bool(true));
}

/// What happened to accounts is recorded, with the account that did it and never a password.
/// The GUI, the API and a route nobody serves alike: a browser is told not to sniff a body into a script, not to
/// let another site frame the GUI, and to keep where a person came from inside this origin.
#[test]
fn every_answer_carries_the_browser_hardening_headers() {
    let leader = start("hardening");
    let op = operator(&leader);
    for path in ["/", "/api/auth/session", "/api/health", "/api/no-such-route"] {
        let response = ureq::get(format!("{}{path}", leader.base))
            .header("cookie", &op.session.cookie)
            .config()
            .http_status_as_error(false)
            .build()
            .call()
            .expect("the call was answered");
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .unwrap_or_else(|| panic!("{path} carries no {name}"))
        };
        assert_eq!(header("x-content-type-options"), "nosniff", "{path}");
        assert_eq!(header("content-security-policy"), "frame-ancestors 'none'", "{path}");
        assert_eq!(header("referrer-policy"), "same-origin", "{path}");
    }
}

#[test]
fn account_changes_are_audited_without_their_passwords() {
    let leader = start("audit");
    let op = operator(&leader);
    leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana" }),
    );

    let (status, events) = leader.get_as(&op.session, "/api/security-events");
    assert_eq!(status, 200);
    let rendered = events.to_string();
    let actions: Vec<&str> = events
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["action"].as_str())
        .collect();
    for expected in ["bootstrap", "login", "password-change", "account-create"] {
        assert!(actions.contains(&expected), "{expected} is not in {actions:?}");
    }
    assert!(!rendered.contains(SETTLED_PASSWORD), "an audit row carried a password");
    assert!(
        !rendered.contains(&leader.password),
        "an audit row carried the bootstrap password"
    );
    assert!(!rendered.contains("argon2"), "an audit row carried a verifier");
}

/// A display name reaches a git author line, where a newline would end it and start whatever came next.
#[test]
fn a_display_name_with_a_newline_is_refused() {
    let leader = start("display-name");
    let op = operator(&leader);
    let (status, _) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({
            "username": "dana",
            "displayName": "Dana\n<someone@else>",
        }),
    );
    assert_eq!(status, 400);
}

// -- attribution ------------------------------------------------------------

/// Make a change on `work` and deploy it, which is the path a configuration edit really takes. The merge onto
/// `main` is what History shows, so this is also what attribution has to reach for it to be visible anywhere.
fn deploy_something(leader: &Leader, op: &Operator) {
    let (status, created) = leader.post_as(
        op,
        "/api/worker-groups?branch=work",
        serde_json::json!({ "id": "west", "name": "West" }),
    );
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["ok"], serde_json::Value::Bool(true), "{created}");

    let (status, opened) = leader.post_as(
        op,
        "/api/changes",
        serde_json::json!({ "branch": "work", "title": "Add the West group", "description": "" }),
    );
    assert_eq!(status, 200, "{opened}");
    assert_eq!(opened["ok"], serde_json::Value::Bool(true), "{opened}");

    let (status, deployed) = leader.post_as(op, "/api/changes/work/deploy", serde_json::json!({}));
    assert_eq!(status, 200, "{deployed}");
    assert_eq!(deployed["ok"], serde_json::Value::Bool(true), "{deployed}");
}

/// The account id is what "mine" would be answered by, and it survives a rename -- which a display name does not.
#[test]
fn a_renamed_person_keeps_the_commits_they_wrote() {
    let leader = start("attribution-rename");
    let op = operator(&leader);
    let (_, users) = leader.get_as(&op.session, "/api/users");
    let id = users[0]["id"].as_str().unwrap().to_string();

    deploy_something(&leader, &op);
    leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "displayName": "Someone Else" }),
    );

    let (_, history) = leader.get_as(&op.session, "/api/history?limit=5");
    let newest = &history["commits"][0];
    assert_eq!(
        newest["author"], "Administrator",
        "renaming rewrote what a commit already said: {history}"
    );
    assert_eq!(newest["authorId"].as_str().unwrap(), id, "{history}");
}

/// Commits the leader made for itself say so, rather than borrowing whoever happened to be signed in. The seeded
/// first commit on `main` is one.
#[test]
fn what_the_leader_did_for_itself_names_no_person() {
    let leader = start("attribution-system");
    let op = operator(&leader);
    let (status, history) = leader.get_as(&op.session, "/api/history?limit=20");
    assert_eq!(status, 200, "{history}");
    let seeded = history["commits"]
        .as_array()
        .unwrap()
        .last()
        .expect("main has a first commit");
    assert_eq!(seeded["author"], "smeltr-leader", "{history}");
    assert!(
        seeded.get("authorId").is_none(),
        "a commit no person made named an account: {history}"
    );
}

/// Who wrote a commit is the server's to say. A body naming somebody else changes nothing.
#[test]
fn a_client_cannot_name_the_author() {
    let leader = start("attribution-forged");
    let op = operator(&leader);
    let (status, created) = leader.post_as(
        &op,
        "/api/worker-groups?branch=work",
        serde_json::json!({
            "id": "west",
            "name": "West",
            "author": "Somebody Else",
            "authorId": "u_someone_else",
        }),
    );
    assert_eq!(status, 200, "{created}");
    leader.post_as(
        &op,
        "/api/changes",
        serde_json::json!({ "branch": "work", "title": "Add the West group", "description": "" }),
    );
    leader.post_as(&op, "/api/changes/work/deploy", serde_json::json!({}));

    let (_, history) = leader.get_as(&op.session, "/api/history?limit=5");
    let newest = &history["commits"][0];
    assert_eq!(newest["author"], "Administrator", "{history}");
    assert_ne!(newest["authorId"], "u_someone_else", "{history}");
}

// -- what the first review found --------------------------------------------

/// A session cookie this leader never issued -- one that outlived a restart, say -- is cleared on the first request
/// that presents it, so the browser stops sending it.
#[test]
fn an_unknown_session_cookie_is_cleared_when_activity_is_reported() {
    let leader = start("stale-cookie");
    let response = ureq::post(format!("{}/api/auth/activity", leader.base))
        .header("origin", &leader.origin)
        .header("cookie", "smeltr_session=not-a-token-this-leader-issued")
        .config()
        .http_status_as_error(false)
        .build()
        .send_empty()
        .expect("the report was answered");
    assert_eq!(response.status(), 401);
    let cleared = response
        .headers()
        .get("set-cookie")
        .map(|v| v.to_str().unwrap().to_string())
        .expect("a Set-Cookie that clears the cookie");
    assert!(cleared.contains("Max-Age=0"), "{cleared}");
    let body: serde_json::Value = response.into_body().read_json().expect("a JSON refusal");
    assert_eq!(body["ok"], serde_json::Value::Bool(false), "{body}");
    assert!(body["error"].is_string(), "{body}");
}

/// A required change is not asked for the password it replaces, and a settled session is: the first was issued
/// by presenting it, and the second is not a license to lock its owner out.
#[test]
fn only_a_required_change_skips_the_current_password() {
    let leader = start("password-without-current");
    let session = signed_in_and_settled(&leader);
    let body = serde_json::json!({ "username": "admin", "newPassword": "a perfectly fine replacement" }).to_string();
    let status = status_of(
        ureq::post(format!("{}/api/auth/password", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("x-smeltr-csrf", &session.csrf)
            .header("content-type", "application/json")
            .send(&body),
        "password",
    );
    assert_eq!(status, 400);
    assert_eq!(
        leader.get_as(&session, "/api/health").0,
        200,
        "the session was not kept"
    );
}

/// A username is whatever was sent, valid or not, and it reaches a throttle bucket. One whose bound falls inside a
/// multibyte character used to panic the handler.
#[test]
fn a_sign_in_with_an_awkward_username_is_answered_rather_than_dropped() {
    let leader = start("awkward-username");
    for filler in [127, 128, 129] {
        let username = format!("{}\u{e9}", "x".repeat(filler));
        let body = serde_json::json!({ "username": username, "password": "not the password" }).to_string();
        let status = status_of(
            ureq::post(format!("{}/api/auth/login", leader.base))
                .header("origin", &leader.origin)
                .header("content-type", "application/json")
                .send(&body),
            "login",
        );
        assert!(status == 401 || status == 429, "{filler}: answered {status}");
    }
}

/// An unauthenticated caller must not be able to write a body-sized audit row per attempt.
#[test]
fn a_refused_sign_in_records_a_bounded_subject() {
    let leader = start("bounded-subject");
    let op = operator(&leader);
    // Two names ten times apart in size. A subject that grew with what was sent would differ between them.
    let (long, longer) = ("a".repeat(4_000), "b".repeat(40_000));
    for username in [&long, &longer] {
        let body = serde_json::json!({ "username": username, "password": "not the password" }).to_string();
        let _ = ureq::post(format!("{}/api/auth/login", leader.base))
            .header("origin", &leader.origin)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(&body);
    }

    let (_, events) = leader.get_as(&op.session, "/api/security-events");
    let recorded = |sent: &str| {
        let head = &sent[..4];
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["outcome"] == "refused")
            .filter_map(|e| e["target"].as_str())
            .find(|target| target.starts_with(head))
            .unwrap_or_else(|| panic!("the refusal of {head}... was not recorded: {events}"))
            .len()
    };
    let (subject, subject_of_longer) = (recorded(&long), recorded(&longer));
    assert_eq!(
        subject, subject_of_longer,
        "the recorded subject grew with the name that was sent"
    );
    assert!(subject < long.len() / 10, "a {subject}-byte subject was recorded");
}

/// A rename and an enable are separate writes, so a request carrying both could land one and be refused on the
/// other. Refusing before anything is written is the answer that cannot half-apply.
#[test]
fn a_patch_changing_two_things_at_once_is_refused_before_anything_changes() {
    let leader = start("patch-atomic");
    let op = operator(&leader);
    let (_, users) = leader.get_as(&op.session, "/api/users");
    let id = users[0]["id"].as_str().unwrap().to_string();
    let was = users[0]["displayName"].as_str().unwrap().to_string();

    let (status, _) = leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "displayName": "Renamed", "enabled": false }),
    );
    assert_eq!(status, 400);

    let (_, after) = leader.get_as(&op.session, "/api/users");
    assert_eq!(after[0]["displayName"], was, "the rename landed anyway");
}

/// An origin carrying a path or a query can never equal what a browser sends, so accepting it would leave every
/// login refused with nothing saying why.
#[test]
fn a_configured_origin_that_is_not_one_refuses_to_start() {
    for bad in ["https://leader.example.com/path", "https://leader.example.com?x=1"] {
        let dir = scratch("bad-origin");
        let out = Command::new(env!("CARGO_BIN_EXE_smeltr"))
            .current_dir(&dir)
            .env_remove("SMELTR_CONFIG_FILE")
            .env("SMELTR_LEADER_ENABLED", "1")
            .env("SMELTR_WORKER_ENABLED", "0")
            .env("SMELTR_LEADER_TLS", "off")
            .env("SMELTR_PUBLIC_ORIGIN", bad)
            .env("SMELTR_LEADER_DATA_DIR", dir.join("leader").to_str().unwrap())
            .env(
                "SMELTR_LEADER_SECRET_KEY",
                dir.join("keys/sealing.key").to_str().unwrap(),
            )
            .output()
            .expect("spawn smeltr");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(1), "{bad}: {stderr}");
        assert!(stderr.contains("SMELTR_PUBLIC_ORIGIN"), "{bad}: {stderr}");
    }
}

// -- what the second review found -------------------------------------------

/// Create a second account and leave it able to use the API, which takes choosing a password first.
fn second_operator(leader: &Leader, op: &Operator, username: &str, display_name: &str) -> Operator {
    let (status, created) = leader.post_as(
        op,
        "/api/users",
        serde_json::json!({ "username": username, "displayName": display_name }),
    );
    assert_eq!(status, 201, "{created}");
    let temporary = created["temporaryPassword"].as_str().expect("a temporary password");

    let first = leader.sign_in(username, temporary);
    let chosen = SETTLED_PASSWORD;
    let body = serde_json::json!({
        "username": username,
        "newPassword": chosen,
    })
    .to_string();
    ureq::post(format!("{}/api/auth/password", leader.base))
        .header("origin", &leader.origin)
        .header("cookie", &first.cookie)
        .header("x-smeltr-csrf", &first.csrf)
        .header("content-type", "application/json")
        .send(&body)
        .expect("the password change was answered");
    Operator {
        session: leader.sign_in(username, chosen),
    }
}

/// A deploy squashes the branch away, so the commit that survives is the only place the author can be. Recording
/// only the deployer loses whoever wrote the content -- which is most of what "who changed this?" is asking.
#[test]
fn a_deploy_records_the_author_and_the_deployer_separately() {
    let leader = start("attribution-two-people");
    let admin = operator(&leader);
    let dana = second_operator(&leader, &admin, "dana", "Dana Okafor");

    // Dana writes the change; the administrator deploys it.
    let (status, created) = leader.post_as(
        &dana,
        "/api/worker-groups?branch=work",
        serde_json::json!({ "id": "west", "name": "West" }),
    );
    assert_eq!(status, 200, "{created}");
    let (status, opened) = leader.post_as(
        &dana,
        "/api/changes",
        serde_json::json!({ "branch": "work", "title": "Add the West group", "description": "" }),
    );
    assert_eq!(status, 200, "{opened}");
    let (status, deployed) = leader.post_as(&admin, "/api/changes/work/deploy", serde_json::json!({}));
    assert_eq!(status, 200, "{deployed}");

    let (_, history) = leader.get_as(&admin.session, "/api/history?limit=5");
    let newest = &history["commits"][0];
    assert_eq!(
        newest["author"], "Dana Okafor",
        "the author was lost in the squash: {history}"
    );
    assert_eq!(newest["deployer"], "Administrator", "{history}");
    assert_ne!(
        newest["authorId"], newest["deployerId"],
        "two people deployed this and history names one: {history}"
    );
    assert!(
        !newest["body"].as_str().unwrap_or_default().contains("Deployed-by"),
        "the trailer is how the deployer is carried, not something to read twice: {history}"
    );
}

/// Deploying your own change is the common case, and it reads as one person rather than as a coincidence.
#[test]
fn deploying_your_own_change_names_you_as_both() {
    let leader = start("attribution-one-person");
    let op = operator(&leader);
    deploy_something(&leader, &op);

    let (_, history) = leader.get_as(&op.session, "/api/history?limit=5");
    let newest = &history["commits"][0];
    assert_eq!(newest["author"], "Administrator", "{history}");
    assert_eq!(newest["deployer"], "Administrator", "{history}");
    assert_eq!(newest["authorId"], newest["deployerId"], "{history}");
}

/// `<` and `>` end a git author line. A name carrying one was accepted and then quietly rewritten, so history
/// reported a name the account was never called, and two accepted names could collapse into one.
#[test]
fn a_display_name_git_cannot_hold_is_refused() {
    let leader = start("display-name-brackets");
    let op = operator(&leader);
    let (status, body) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana <Ops>" }),
    );
    assert_eq!(status, 400, "{body}");

    // And the same name cannot arrive by renaming afterward.
    let (status, created) = leader.post_as(
        &op,
        "/api/users",
        serde_json::json!({ "username": "dana", "displayName": "Dana Okafor" }),
    );
    assert_eq!(status, 201, "{created}");
    let id = created["userId"].as_str().expect("an id");
    let (status, renamed) = leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "displayName": "Dana <Ops>" }),
    );
    assert_eq!(status, 400, "{renamed}");
}

/// Changing a password is throttled, and the caller is told how long to wait. A session is not a license to guess
/// the password it was issued with: without the throttle, anyone holding one could try passwords at the full rate
/// the hash permits. Without the header a client can only guess, and a guess that is short spends the next attempt
/// on nothing -- both refusals carry it.
#[test]
fn a_throttled_caller_is_told_how_long_to_wait() {
    let leader = start("retry-after");
    let session = signed_in_and_settled(&leader);

    let mut seen = None;
    for _ in 0..14 {
        let response = ureq::post(format!("{}/api/auth/password", leader.base))
            .header("origin", &leader.origin)
            .header("cookie", &session.cookie)
            .header("x-smeltr-csrf", &session.csrf)
            .header("content-type", "application/json")
            .config()
            .http_status_as_error(false)
            .build()
            .send(
                &serde_json::json!({
                    "username": "admin",
                    "currentPassword": "not the password",
                    "newPassword": "something else entirely",
                })
                .to_string(),
            )
            .expect("the password change was answered");
        if response.status() == 429 {
            seen = response
                .headers()
                .get("retry-after")
                .map(|v| v.to_str().unwrap().to_string());
            break;
        }
    }

    let seconds = seen.expect("the throttle refused without saying how long to wait");
    assert!(
        seconds.parse::<u64>().is_ok_and(|s| s >= 1),
        "Retry-After is not a number of seconds: {seconds:?}"
    );
}

// -- system settings --------------------------------------------------------

/// Session deadlines belong to the leader, not to the binary: an operator sets them and a restart keeps them.
#[test]
fn session_timeouts_are_configured_rather_than_compiled_in() {
    let leader = start("session-settings");
    let op = operator(&leader);

    let (status, settings) = leader.get_as(&op.session, "/api/settings/sessions");
    assert_eq!(status, 200, "{settings}");
    assert_eq!(settings["idleSeconds"], 30 * 60, "the shipped default: {settings}");
    assert_eq!(settings["absoluteSeconds"], 12 * 60 * 60, "{settings}");
    // The bounds come from the leader so the form does not restate them and drift.
    assert!(settings["minSeconds"].as_u64().is_some_and(|s| s >= 1), "{settings}");
    assert!(settings["maxSeconds"].as_u64().is_some(), "{settings}");

    let (status, saved) = leader.put_as(
        &op,
        "/api/settings/sessions",
        serde_json::json!({ "idleSeconds": 900, "absoluteSeconds": 28800 }),
    );
    assert_eq!(status, 200, "{saved}");
    assert_eq!(saved["idleSeconds"], 900, "{saved}");

    let (_, reread) = leader.get_as(&op.session, "/api/settings/sessions");
    assert_eq!(reread["idleSeconds"], 900, "{reread}");
    assert_eq!(reread["absoluteSeconds"], 28800, "{reread}");
}

/// A pair that constrains itself is refused where it is written, and nothing is stored.
#[test]
fn an_idle_timeout_past_the_absolute_one_is_refused() {
    let leader = start("session-settings-nonsense");
    let op = operator(&leader);

    let (status, body) = leader.put_as(
        &op,
        "/api/settings/sessions",
        serde_json::json!({ "idleSeconds": 86400, "absoluteSeconds": 3600 }),
    );
    assert_eq!(status, 400, "{body}");

    let (_, settings) = leader.get_as(&op.session, "/api/settings/sessions");
    assert_eq!(
        settings["idleSeconds"],
        30 * 60,
        "a refused pair changed something: {settings}"
    );
}

// -- OIDC sign-in --------------------------------------------------------------

/// URL-safe base64 without padding, as JOSE writes it.
fn b64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

/// Who the provider signs in, without asking, the next time it is visited.
#[derive(Clone)]
struct Person {
    subject: String,
    name: String,
    email: String,
    groups: Vec<String>,
}

/// What the provider remembers between handing out a code and taking it back.
struct Issued {
    nonce: String,
    challenge: String,
}

struct ProviderState {
    issuer: String,
    key: aws_lc_rs::signature::EcdsaKeyPair,
    person: Mutex<Person>,
    codes: Mutex<std::collections::HashMap<String, Issued>>,
    /// The refresh tokens it will still take. Each is spent by one use and replaced by a new one.
    refresh_tokens: Mutex<std::collections::HashSet<String>>,
}

/// An OpenID provider in this process: discovery, its key, an authorize endpoint that signs in [`Person`] at once,
/// and a token endpoint that holds to PKCE and the client's secret.
struct Provider {
    base: String,
    state: Arc<ProviderState>,
    _runtime: tokio::runtime::Runtime,
}

const CLIENT_ID: &str = "smeltr";
const CLIENT_SECRET: &str = "s3cret-value";

impl Provider {
    fn start(person: Person) -> Provider {
        use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};
        use axum::routing::{get, post};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind((ORIGIN_HOST, 0)))
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(ProviderState {
            issuer: base.clone(),
            key: EcdsaKeyPair::generate(&ECDSA_P256_SHA256_FIXED_SIGNING).unwrap(),
            person: Mutex::new(person),
            codes: Mutex::new(std::collections::HashMap::new()),
            refresh_tokens: Mutex::new(std::collections::HashSet::new()),
        });
        let app = axum::Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/authorize", get(authorize))
            .route("/token", post(token))
            .with_state(state.clone());
        runtime.spawn(async move { axum::serve(listener, app).await.unwrap() });
        Provider {
            base,
            state,
            _runtime: runtime,
        }
    }

    fn becomes(&self, person: Person) {
        *self.state.person.lock().unwrap() = person;
    }

    /// Withdraw every refresh token handed out, as a provider does when the person's session there ends.
    fn withdraws_grants(&self) {
        self.state.refresh_tokens.lock().unwrap().clear();
    }
}

async fn discovery(axum::extract::State(p): axum::extract::State<Arc<ProviderState>>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "issuer": p.issuer,
        "authorization_endpoint": format!("{}/authorize", p.issuer),
        "token_endpoint": format!("{}/token", p.issuer),
        "jwks_uri": format!("{}/jwks", p.issuer),
        "response_types_supported": ["code"],
        "code_challenge_methods_supported": ["S256"],
        "id_token_signing_alg_values_supported": ["ES256"],
    }))
}

async fn jwks(axum::extract::State(p): axum::extract::State<Arc<ProviderState>>) -> axum::Json<serde_json::Value> {
    use aws_lc_rs::signature::KeyPair;
    // An uncompressed point: 0x04, then x, then y.
    let point = p.key.public_key().as_ref().to_vec();
    axum::Json(serde_json::json!({ "keys": [{
        "kty": "EC", "crv": "P-256", "kid": "k1", "use": "sig", "alg": "ES256",
        "x": b64url(&point[1..33]), "y": b64url(&point[33..65]),
    }]}))
}

async fn authorize(
    axum::extract::State(p): axum::extract::State<Arc<ProviderState>>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    assert_eq!(q["client_id"], CLIENT_ID);
    assert_eq!(q["response_type"], "code");
    assert_eq!(q["code_challenge_method"], "S256");
    let code = format!("code-{}", p.codes.lock().unwrap().len());
    p.codes.lock().unwrap().insert(
        code.clone(),
        Issued {
            nonce: q["nonce"].clone(),
            challenge: q["code_challenge"].clone(),
        },
    );
    let back = format!("{}?code={code}&state={}", q["redirect_uri"], q["state"]);
    (axum::http::StatusCode::FOUND, [(axum::http::header::LOCATION, back)]).into_response()
}

async fn token(
    axum::extract::State(p): axum::extract::State<Arc<ProviderState>>,
    headers: axum::http::HeaderMap,
    axum::Form(form): axum::Form<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let refused = |why: &str| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "error": "invalid_grant", "error_description": why })),
        )
            .into_response()
    };
    // The client's secret, in a Basic header or in the form: whichever the leader chose.
    let basic = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .map(str::to_string);
    let in_form = form.get("client_secret").map(|s| s == CLIENT_SECRET).unwrap_or(false);
    let in_header = basic.is_some_and(|b| {
        let expected = format!("{CLIENT_ID}:{CLIENT_SECRET}");
        let standard = b64url(expected.as_bytes()).replace('-', "+").replace('_', "/");
        b.trim_end_matches('=') == standard
    });
    if !in_form && !in_header {
        return refused("the client did not prove itself");
    }
    if form.get("grant_type").map(String::as_str) == Some("refresh_token") {
        let spent = form
            .get("refresh_token")
            .is_some_and(|t| p.refresh_tokens.lock().unwrap().remove(t));
        if !spent {
            return refused("unknown refresh token");
        }
        // A refreshed ID token carries no nonce (OpenID Connect Core 12.2).
        return axum::Json(tokens_for(&p, None)).into_response();
    }
    let Some(issued) = form.get("code").and_then(|c| p.codes.lock().unwrap().remove(c)) else {
        return refused("unknown code");
    };
    let verifier = form.get("code_verifier").cloned().unwrap_or_default();
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, verifier.as_bytes());
    if b64url(digest.as_ref()) != issued.challenge {
        return refused("PKCE");
    }
    axum::Json(tokens_for(&p, Some(&issued.nonce))).into_response()
}

/// The token endpoint's answer for whoever [`Person`] is now: an ID token carrying `nonce` if given, and a fresh
/// refresh token.
fn tokens_for(p: &ProviderState, nonce: Option<&str>) -> serde_json::Value {
    let person = p.person.lock().unwrap().clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let header = b64url(br#"{"alg":"ES256","kid":"k1","typ":"JWT"}"#);
    let mut claims = serde_json::json!({
        "iss": p.issuer, "sub": person.subject, "aud": CLIENT_ID, "iat": now, "exp": now + 300,
        "name": person.name, "email": person.email, "email_verified": true, "groups": person.groups,
    });
    if let Some(nonce) = nonce {
        claims["nonce"] = nonce.into();
    }
    let signed = format!("{header}.{}", b64url(claims.to_string().as_bytes()));
    let signature = p
        .key
        .sign(&aws_lc_rs::rand::SystemRandom::new(), signed.as_bytes())
        .unwrap();
    let id_token = format!("{signed}.{}", b64url(signature.as_ref()));
    let mut random = [0u8; 16];
    aws_lc_rs::rand::fill(&mut random).unwrap();
    let refresh_token = b64url(&random);
    p.refresh_tokens.lock().unwrap().insert(refresh_token.clone());
    serde_json::json!({
        "access_token": "a", "token_type": "Bearer", "id_token": id_token, "refresh_token": refresh_token,
    })
}

/// A browser that does not follow redirects, so each hop can be looked at.
fn browser() -> ureq::Agent {
    ureq::Agent::config_builder()
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent()
}

/// The `name=value` of the cookie called `name` that `response` sets.
fn set_cookie(response: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").to_string())
        .find(|c| c.starts_with(&format!("{name}=")) && c.len() > name.len() + 1)
}

/// The `name=value` of the cookie binding a browser to the flow `response` started. Named per flow.
fn binding_cookie(response: &ureq::http::Response<ureq::Body>) -> Option<String> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").to_string())
        .find(|c| c.starts_with("smeltr_oidc_") && !c.ends_with('='))
}

fn location(response: &ureq::http::Response<ureq::Body>) -> String {
    response
        .headers()
        .get("location")
        .expect("a redirect")
        .to_str()
        .unwrap()
        .to_string()
}

/// One sign-in through the provider, as a browser makes it. Where it ended, and the session cookie if it set one.
fn oidc_sign_in(leader: &Leader) -> (String, Option<String>) {
    let browser = browser();
    let started = browser
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .call()
        .unwrap();
    assert_eq!(started.status(), 303);
    let binding = binding_cookie(&started).expect("a cookie binding the flow to this browser");
    let at_provider = browser.get(location(&started)).call().unwrap();
    let back = location(&at_provider);
    let returned = browser.get(back).header("cookie", &binding).call().unwrap();
    assert_eq!(returned.status(), 303);
    (location(&returned), set_cookie(&returned, "smeltr_session"))
}

fn configure_oidc(leader: &Leader, op: &Operator, provider: &Provider, admin_group: &str) -> serde_json::Value {
    let (status, body) = leader.put_as(
        op,
        "/api/settings/oidc",
        serde_json::json!({
            "enabled": true,
            "issuer": provider.base,
            "clientId": CLIENT_ID,
            "clientSecret": CLIENT_SECRET,
        }),
    );
    assert_eq!(status, 200);
    // The administrators' role, which is also what lets a person holding its group in.
    let (status, role) = leader.put_as(
        op,
        &format!("/api/roles/{admin_group}"),
        serde_json::json!({ "administrator": true }),
    );
    assert_eq!(status, 200, "{role}");
    body
}

fn ada() -> Person {
    Person {
        subject: "00u-ada".to_string(),
        name: "Ada Lovelace".to_string(),
        email: "ada@example.com".to_string(),
        groups: vec!["smeltr-admins".to_string()],
    }
}

/// The whole path: an administrator configures the provider, a person signs in through it, and the session they
/// get is the same as a password's -- the fleet opens to it, and disabling the account ends it.
#[test]
fn a_provider_sign_in_ends_in_a_session_like_a_password_one() {
    let leader = start("oidc");
    let op = operator(&leader);
    assert_eq!(
        leader.get_as(&op.session, "/api/auth/oidc").1["offered"],
        false,
        "offered before it was configured"
    );
    let provider = Provider::start(ada());
    let body = configure_oidc(&leader, &op, &provider, "smeltr-admins");
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(body["settings"]["clientSecretSet"], true, "{body}");
    assert!(
        body["settings"].get("clientSecret").is_none(),
        "the secret came back: {body}"
    );
    assert_eq!(
        body["settings"]["redirectUri"],
        format!("{}/api/auth/oidc/callback", leader.origin)
    );
    assert_eq!(leader.get_as(&op.session, "/api/auth/oidc").1["offered"], true);

    let (ended, cookie) = oidc_sign_in(&leader);
    assert_eq!(ended, "/");
    let session = Session {
        cookie: cookie.expect("a session cookie"),
        csrf: String::new(),
    };
    assert_eq!(leader.get_as(&session, "/api/health").0, 200);
    let (_, me) = leader.get_as(&session, "/api/auth/session");
    assert_eq!(me["authSource"], "oidc", "{me}");
    assert_eq!(me["displayName"], "Ada Lovelace", "{me}");
    assert_eq!(me["providerLogin"], "ada@example.com", "{me}");
    assert_eq!(me["mustChangePassword"], false, "{me}");

    // An OIDC administrator manages accounts like a local one.
    let oidc_op = Operator {
        session: Session {
            cookie: session.cookie.clone(),
            csrf: me["csrf"].as_str().unwrap().to_string(),
        },
    };
    let (status, body) = leader.post_as(
        &oidc_op,
        "/api/users",
        serde_json::json!({ "username": "grace", "displayName": "Grace" }),
    );
    assert_eq!(status, 201, "{body}");

    // Signing in again is the same account, not a second one.
    let (ended, _) = oidc_sign_in(&leader);
    assert_eq!(ended, "/");
    let (_, users) = leader.get_as(&op.session, "/api/users");
    let oidc: Vec<_> = users
        .as_array()
        .expect("a roster")
        .iter()
        .filter(|u| u["authSource"] == "oidc")
        .collect();
    assert_eq!(oidc.len(), 1, "{users}");
    assert_eq!(oidc[0]["providerLogin"], "ada@example.com", "{users}");
    let id = oidc[0]["id"].as_str().unwrap().to_string();

    // In no group a role names, the provider's word is not enough.
    provider.becomes(Person {
        groups: vec![],
        ..ada()
    });
    assert_eq!(oidc_sign_in(&leader), ("/?oidc=refused".to_string(), None));

    // Disabled, the account's session ends and it cannot sign in again.
    provider.becomes(ada());
    let (status, body) = leader.patch_as(
        &op,
        &format!("/api/users/{id}"),
        serde_json::json!({ "enabled": false }),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(leader.get_as(&session, "/api/health").0, 401);
    assert_eq!(oidc_sign_in(&leader), ("/?oidc=refused".to_string(), None));

    // Enabled again, it signs in afresh, and the session disabling ended stays ended.
    let (status, body) = leader.patch_as(&op, &format!("/api/users/{id}"), serde_json::json!({ "enabled": true }));
    assert_eq!(status, 200, "{body}");
    let (ended, again) = oidc_sign_in(&leader);
    assert_eq!(ended, "/");
    assert!(again.is_some());
    assert_eq!(leader.get_as(&session, "/api/health").0, 401);

    // Every refusal is on the record, and so is what the settings change touched.
    let (_, events) = leader.get_as(&op.session, "/api/security-events");
    let rows = events.as_array().unwrap();
    let refused = rows
        .iter()
        .filter(|e| e["action"] == "login" && e["outcome"] == "refused")
        .count();
    assert_eq!(refused, 2, "{events}");
    let change = rows
        .iter()
        .find(|e| e["action"] == "oidc-change")
        .expect("an oidc-change row");
    assert_eq!(
        change["target"], "turned on, issuer, client id, client secret",
        "{events}"
    );

    // And a password still signs in, whatever the provider does.
    leader.sign_in("admin", SETTLED_PASSWORD);
}

/// A flow is finished only by the browser that started it, and only once: a link carrying somebody else's code and
/// state signs nobody in.
#[test]
fn a_callback_from_another_browser_or_a_second_time_is_refused() {
    let leader = start("oidc-binding");
    let op = operator(&leader);
    let provider = Provider::start(ada());
    configure_oidc(&leader, &op, &provider, "smeltr-admins");

    let browser = browser();
    let started = browser
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .call()
        .unwrap();
    let binding = binding_cookie(&started).unwrap();
    let back = location(&browser.get(location(&started)).call().unwrap());

    let elsewhere = browser.get(&back).call().unwrap();
    assert_eq!(location(&elsewhere), "/?oidc=expired");
    assert_eq!(set_cookie(&elsewhere, "smeltr_session"), None);

    // The flow was spent by that attempt, so even its own browser cannot finish it now.
    let replayed = browser.get(&back).header("cookie", &binding).call().unwrap();
    assert_eq!(location(&replayed), "/?oidc=expired");
    assert_eq!(set_cookie(&replayed, "smeltr_session"), None);
}

/// Each flow has a cookie of its own: a second sign-in in another tab leaves the first one able to finish, and one
/// flow's cookie does not finish another, so a link carrying an attacker's code and state signs a visitor in as
/// nobody even when the visitor holds a live flow of their own.
#[test]
fn each_flow_is_finished_only_with_its_own_cookie() {
    let leader = start("oidc-tabs");
    let op = operator(&leader);
    let provider = Provider::start(ada());
    configure_oidc(&leader, &op, &provider, "smeltr-admins");
    let browser = browser();
    let begin = || {
        let started = browser
            .get(format!("{}/api/auth/oidc/start", leader.base))
            .call()
            .unwrap();
        let cookie = binding_cookie(&started).unwrap();
        let back = location(&browser.get(location(&started)).call().unwrap());
        (cookie, back)
    };
    let (first_cookie, first_back) = begin();
    let (second_cookie, second_back) = begin();
    assert_ne!(first_cookie.split('=').next(), second_cookie.split('=').next());

    // The attacker's flow, finished with the visitor's own cookie for another flow.
    let crossed = browser
        .get(&first_back)
        .header("cookie", &second_cookie)
        .call()
        .unwrap();
    assert_eq!(location(&crossed), "/?oidc=expired");

    // Both cookies sent, as a browser holding two flows sends them: the second still finishes.
    let both = format!("{first_cookie}; {second_cookie}");
    let finished = browser.get(&second_back).header("cookie", &both).call().unwrap();
    assert_eq!(location(&finished), "/");
    assert!(set_cookie(&finished, "smeltr_session").is_some());
}

/// The settings are read again when the provider sends a person back, so turning OIDC off while somebody is at
/// the provider ends their sign-in.
#[test]
fn turning_oidc_off_ends_a_sign_in_under_way() {
    let leader = start("oidc-off");
    let op = operator(&leader);
    let provider = Provider::start(ada());
    configure_oidc(&leader, &op, &provider, "smeltr-admins");
    let browser = browser();
    let started = browser
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .call()
        .unwrap();
    let cookie = binding_cookie(&started).unwrap();
    let back = location(&browser.get(location(&started)).call().unwrap());

    let (_, body) = leader.put_as(
        &op,
        "/api/settings/oidc",
        serde_json::json!({
            "enabled": false, "issuer": provider.base, "clientId": CLIENT_ID,
        }),
    );
    assert_eq!(body["ok"], true, "{body}");
    assert_eq!(
        body["settings"]["clientSecretSet"], true,
        "turning it off kept the secret: {body}"
    );
    let returned = browser.get(&back).header("cookie", &cookie).call().unwrap();
    assert_eq!(location(&returned), "/?oidc=unavailable");
    assert_eq!(set_cookie(&returned, "smeltr_session"), None);
}

/// The provider sends a person back to the configured origin, where a cookie set under an alias would not be
/// sent. A sign-in started at an alias moves to the configured origin before it begins.
#[test]
fn a_sign_in_started_at_an_alias_moves_to_the_configured_origin() {
    let leader = start("oidc-alias");
    let op = operator(&leader);
    let provider = Provider::start(ada());
    configure_oidc(&leader, &op, &provider, "smeltr-admins");
    let port = leader.base.rsplit(':').next().unwrap();
    let started = browser()
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .header("host", format!("localhost:{port}"))
        .call()
        .unwrap();
    assert_eq!(started.status(), 303);
    assert_eq!(location(&started), format!("{}/api/auth/oidc/start", leader.origin));
    assert_eq!(binding_cookie(&started), None);
}

/// Turning sign-in on reads the provider first, so a setting that cannot work is refused when it is saved.
#[test]
fn a_provider_that_cannot_be_read_is_refused_when_saved() {
    let leader = start("oidc-refused");
    let op = operator(&leader);
    for issuer in ["http://127.0.0.1:1", "http://id.example.com"] {
        let (_, body) = leader.put_as(
            &op,
            "/api/settings/oidc",
            serde_json::json!({
                "enabled": true, "issuer": issuer, "clientId": CLIENT_ID, "clientSecret": CLIENT_SECRET,
            }),
        );
        assert_eq!(body["ok"], false, "{issuer}: {body}");
        assert_eq!(body["settings"]["enabled"], false, "{issuer}: {body}");
    }
    let started = browser()
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .call()
        .unwrap();
    assert_eq!(location(&started), "/?oidc=unavailable");
}

// -- what an area-restricted account may do -----------------------------------

/// The group every provider account here holds. Its role gives nothing, so it only lets a person in: what they may
/// do comes from the other groups they hold.
const SIGN_IN_GROUP: &str = "smeltr-users";

/// A leader whose provider signs people in through [`SIGN_IN_GROUP`], with a `west` group deployed and two roles
/// over it: `smeltr-west-view` (viewer) and `smeltr-west-edit` (editor).
fn with_west_roles(label: &str) -> (Leader, Operator, Provider) {
    with_west_roles_and(label, &[])
}

/// [`with_west_roles`], with extra environment for the leader.
fn with_west_roles_and(label: &str, extra: &[(&str, &str)]) -> (Leader, Operator, Provider) {
    let leader = start_with(label, extra);
    let op = operator(&leader);
    deploy_something(&leader, &op);
    let provider = Provider::start(ada());
    configure_oidc(&leader, &op, &provider, SIGN_IN_GROUP);
    // `configure_oidc` makes its group the administrators' role; here it gives nothing.
    let roles = [
        (SIGN_IN_GROUP, serde_json::json!({})),
        ("smeltr-west-view", serde_json::json!({ "areas": { "west": "viewer" } })),
        ("smeltr-west-edit", serde_json::json!({ "areas": { "west": "editor" } })),
    ];
    for (name, role) in roles {
        let (status, body) = leader.put_as(&op, &format!("/api/roles/{name}"), role);
        assert_eq!(status, 200, "{name}: {body}");
    }
    (leader, op, provider)
}

/// Sign in through the provider as someone holding `groups` besides [`SIGN_IN_GROUP`], with the CSRF value an
/// unsafe request needs.
fn provider_account(leader: &Leader, provider: &Provider, groups: &[&str]) -> Operator {
    provider.becomes(dana(groups));
    let (ended, cookie) = oidc_sign_in(leader);
    assert_eq!(ended, "/");
    let mut session = Session {
        cookie: cookie.expect("a session cookie"),
        csrf: String::new(),
    };
    let (status, me) = leader.get_as(&session, "/api/auth/session");
    assert_eq!(status, 200, "{me}");
    assert_eq!(me["access"]["administrator"], false, "{me}");
    session.csrf = me["csrf"].as_str().expect("a csrf token").to_string();
    Operator { session }
}

/// Dana, holding `groups` besides [`SIGN_IN_GROUP`].
fn dana(groups: &[&str]) -> Person {
    Person {
        subject: "00u-dana".to_string(),
        name: "Dana Okafor".to_string(),
        email: "dana@example.com".to_string(),
        groups: std::iter::once(SIGN_IN_GROUP)
            .chain(groups.iter().copied())
            .map(str::to_string)
            .collect(),
    }
}

/// A source to save, which every scope accepts.
fn a_source() -> serde_json::Value {
    serde_json::json!([{ "id": "storefront", "type": "logs" }])
}

/// A viewer reads their group and nothing else, and changes nothing in it.
#[test]
fn a_viewer_reads_their_group_and_saves_nothing() {
    let (leader, _op, provider) = with_west_roles("oidc-viewer");
    let viewer = provider_account(&leader, &provider, &["smeltr-west-view"]);

    let (status, body) = leader.get_as(&viewer.session, "/api/config?scope=west");
    assert_eq!(status, 200, "{body}");
    let (status, body) = leader.get_as(&viewer.session, "/api/config?scope=east");
    assert_eq!(status, 403, "read a group no role gives: {body}");

    let (status, body) = leader.put_as(&viewer, "/api/sources?scope=west&branch=dana", a_source());
    assert_eq!(status, 403, "a viewer saved: {body}");
    let (status, body) = leader.get_as(&viewer.session, "/api/users");
    assert_eq!(status, 403, "a viewer read the roster: {body}");
}

/// An editor saves in their group and opens a change, but deploying it takes a deployer.
#[test]
fn an_editor_saves_in_their_group_and_does_not_deploy() {
    let (leader, _op, provider) = with_west_roles("oidc-editor");
    let editor = provider_account(&leader, &provider, &["smeltr-west-edit"]);

    let (status, body) = leader.put_as(&editor, "/api/sources?scope=west&branch=dana", a_source());
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ok"], true, "{body}");
    let (status, body) = leader.put_as(&editor, "/api/sources?scope=east&branch=dana", a_source());
    assert_eq!(status, 403, "saved in a group no role gives: {body}");

    let (status, body) = leader.post_as(
        &editor,
        "/api/changes",
        serde_json::json!({ "branch": "dana", "title": "A storefront source", "description": "" }),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["ok"], true, "{body}");

    let (status, body) = leader.post_as(&editor, "/api/changes/dana/deploy", serde_json::json!({}));
    assert_eq!(status, 403, "an editor deployed a change: {body}");
    let (status, body) = leader.post_as(&editor, "/api/deploy", serde_json::json!({}));
    assert_eq!(status, 403, "an editor deployed a commit: {body}");
    let (status, body) = leader.get_as(&editor.session, "/api/users");
    assert_eq!(status, 403, "an editor read the roster: {body}");
}

/// Refreshes each OIDC session at the provider this often, so the tests below wait seconds rather than minutes.
const QUICK_REFRESH: (&str, &str) = ("SMELTR_OIDC_REFRESH_INTERVAL_MS", "200");

/// Waits for `holds` to come true, a refresh or two at [`QUICK_REFRESH`].
fn eventually(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !holds() {
        assert!(std::time::Instant::now() < deadline, "{what} never happened");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Whether `session` still stands for somebody.
fn signed_in(leader: &Leader, session: &Session) -> bool {
    leader.get_as(session, "/api/auth/session").1["authenticated"] == true
}

/// A group given or taken away at the provider reaches a person already signed in, without their signing in again.
#[test]
fn a_group_change_at_the_provider_reaches_a_signed_in_person() {
    let (leader, _op, provider) = with_west_roles_and("oidc-refresh-groups", &[QUICK_REFRESH]);
    let dana_now = provider_account(&leader, &provider, &["smeltr-west-view"]);
    assert_eq!(leader.get_as(&dana_now.session, "/api/config?scope=west").0, 200);

    provider.becomes(dana(&[]));
    eventually("the viewer role going", || {
        leader.get_as(&dana_now.session, "/api/config?scope=west").0 == 403
    });
    provider.becomes(dana(&["smeltr-west-edit"]));
    eventually("the editor role arriving", || {
        leader
            .put_as(&dana_now, "/api/sources?scope=west&branch=dana", a_source())
            .0
            == 200
    });
    assert!(signed_in(&leader, &dana_now.session), "the session itself lives on");
}

/// A provider that refuses the refresh, because the person's session there ended or they were disabled, ends their
/// session here.
#[test]
fn a_refused_refresh_ends_the_session() {
    let (leader, _op, provider) = with_west_roles_and("oidc-refresh-refused", &[QUICK_REFRESH]);
    let dana_now = provider_account(&leader, &provider, &["smeltr-west-view"]);
    // Refreshed at least once first, so the token spent is one the leader was handed on a refresh.
    std::thread::sleep(std::time::Duration::from_millis(800));
    assert!(signed_in(&leader, &dana_now.session));

    provider.withdraws_grants();
    eventually("the session ending", || !signed_in(&leader, &dana_now.session));
}

/// Leaving every group a role names ends a session as surely as never holding one, and leaving only some does not.
#[test]
fn losing_every_group_a_role_names_ends_the_session() {
    let (leader, _op, provider) = with_west_roles_and("oidc-refresh-roles", &[QUICK_REFRESH]);
    let dana_now = provider_account(&leader, &provider, &["smeltr-west-view"]);

    let mut fewer = dana(&["smeltr-west-view"]);
    fewer.groups.retain(|g| g != SIGN_IN_GROUP);
    provider.becomes(fewer);
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(
        signed_in(&leader, &dana_now.session),
        "a role's group was still held and the session ended"
    );

    let mut outside = dana(&[]);
    outside.groups = vec!["everyone".to_string()];
    provider.becomes(outside);
    eventually("the session ending", || !signed_in(&leader, &dana_now.session));
}

/// A role that gives nothing lets a person in and nothing more: they are refused every operator route.
#[test]
fn an_account_holding_no_role_reaches_no_operator_route() {
    let (leader, _op, provider) = with_west_roles("oidc-no-role");
    let nobody = provider_account(&leader, &provider, &["everyone"]);
    let (_, me) = leader.get_as(&nobody.session, "/api/auth/session");
    assert_eq!(me["access"]["areas"], serde_json::json!({}), "{me}");

    for route in OPERATOR_ROUTES {
        let (status, body) = leader.get_as(&nobody.session, route);
        assert_eq!(status, 403, "{route} answered someone holding nothing: {body}");
    }
    let (status, body) = leader.put_as(&nobody, "/api/sources?scope=west&branch=dana", a_source());
    assert_eq!(status, 403, "{body}");
}

// -- OIDC sign-in against a real Keycloak --------------------------------------

/// The `smeltr` realm's client, as `tests/live/keycloak/smeltr-realm.json` sets it.
const KEYCLOAK_SECRET: &str = "smeltr-live-1";

/// The `Set-Cookie` values `response` carries, as one `Cookie` header adds them.
fn cookies_of(response: &ureq::http::Response<ureq::Body>) -> Vec<String> {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").to_string())
        .filter(|c| !c.ends_with('='))
        .collect()
}

/// The login form's `action`, unescaped, from the page Keycloak answers its authorize endpoint with.
fn login_action(page: &str) -> String {
    let form = &page[page.find("id=\"kc-form-login\"").expect("Keycloak's login form")..];
    let action = &form[form.find("action=\"").expect("the form's action") + 8..];
    action[..action.find('"').unwrap()].replace("&amp;", "&")
}

/// One sign-in through Keycloak as `username`, typing the password into its own login page. Where it ended, and the
/// session cookie if it set one.
fn keycloak_sign_in(leader: &Leader, username: &str) -> (String, Option<String>) {
    let browser = browser();
    let started = browser
        .get(format!("{}/api/auth/oidc/start", leader.base))
        .call()
        .unwrap();
    assert_eq!(started.status(), 303);
    let binding = binding_cookie(&started).unwrap();
    let mut page = browser.get(location(&started)).call().unwrap();
    assert_eq!(page.status(), 200, "Keycloak's login page");
    let held = cookies_of(&page).join("; ");
    let action = login_action(&page.body_mut().read_to_string().unwrap());
    let posted = browser
        .post(&action)
        .header("cookie", &held)
        .send_form([("username", username), ("password", KEYCLOAK_SECRET)])
        .unwrap();
    assert_eq!(posted.status(), 302, "Keycloak took the password");
    let returned = browser
        .get(location(&posted))
        .header("cookie", &binding)
        .call()
        .unwrap();
    assert_eq!(returned.status(), 303);
    (location(&returned), set_cookie(&returned, "smeltr_session"))
}

/// The contract with a real provider: Keycloak's discovery, keys, login and token endpoint, and the groups claim its
/// group membership mapper writes. A member of a role's group signs in; somebody outside it does not.
#[test]
fn live_keycloak_signs_in_only_a_role_s_group() {
    let Some(issuer) = crate::live::keycloak("live_keycloak_signs_in_only_a_role_s_group") else {
        return;
    };
    let leader = start("oidc-keycloak");
    let op = operator(&leader);
    let (status, body) = leader.put_as(
        &op,
        "/api/settings/oidc",
        serde_json::json!({
            "enabled": true, "issuer": issuer, "clientId": "smeltr", "clientSecret": KEYCLOAK_SECRET,
        }),
    );
    assert_eq!((status, body["ok"].clone()), (200, serde_json::json!(true)), "{body}");
    let (status, role) = leader.put_as(
        &op,
        "/api/roles/smeltr-admins",
        serde_json::json!({ "administrator": true }),
    );
    assert_eq!(status, 200, "{role}");

    let (ended, cookie) = keycloak_sign_in(&leader, "ada");
    assert_eq!(ended, "/");
    let session = Session {
        cookie: cookie.expect("a session cookie"),
        csrf: String::new(),
    };
    assert!(signed_in(&leader, &session));
    let (_, me) = leader.get_as(&session, "/api/auth/session");
    assert_eq!(me["authSource"], "oidc", "{me}");
    assert_eq!(me["displayName"], "Ada Lovelace", "{me}");
    assert_eq!(me["providerLogin"], "ada@example.com", "{me}");

    assert_eq!(keycloak_sign_in(&leader, "bob"), ("/?oidc=refused".to_string(), None));
}

/// Keycloak's admin API, as its bootstrap administrator: a bearer token, and the realm's admin base.
fn keycloak_admin(issuer: &str) -> (String, String) {
    let base = &issuer[..issuer.find("/realms/").expect("a realm issuer")];
    let mut answer = ureq::post(format!("{base}/realms/master/protocol/openid-connect/token"))
        .send_form([
            ("grant_type", "password"),
            ("client_id", "admin-cli"),
            ("username", "admin"),
            ("password", KEYCLOAK_SECRET),
        ])
        .expect("Keycloak's admin token");
    let token: serde_json::Value = answer.body_mut().read_json().unwrap();
    (
        format!("Bearer {}", token["access_token"].as_str().unwrap()),
        format!("{base}/admin/realms/smeltr"),
    )
}

/// The id of the first thing Keycloak's admin API lists at `path`.
fn keycloak_id(bearer: &str, path: &str) -> String {
    let mut answer = ureq::get(path).header("authorization", bearer).call().unwrap();
    let found: serde_json::Value = answer.body_mut().read_json().unwrap();
    found[0]["id"].as_str().expect("found").to_string()
}

/// A Keycloak user made for one test, in `smeltr-admins`, deleted however the test ends: its admin URL and the
/// bearer to delete it with. Its own, so taking it out of a group or ending its sessions disturbs no other test.
struct KeycloakUser(String, String);

impl KeycloakUser {
    fn make(bearer: &str, admin: &str, username: &str) -> KeycloakUser {
        let path = format!("{admin}/users?username={username}&exact=true");
        let mut listed = ureq::get(&path).header("authorization", bearer).call().unwrap();
        let listed: serde_json::Value = listed.body_mut().read_json().unwrap();
        if let Some(id) = listed[0]["id"].as_str() {
            let _ = ureq::delete(format!("{admin}/users/{id}"))
                .header("authorization", bearer)
                .call();
        }
        let made = ureq::post(format!("{admin}/users"))
            .header("authorization", bearer)
            .send_json(serde_json::json!({
                "username": username, "enabled": true, "email": format!("{username}@example.com"),
                "emailVerified": true, "firstName": "Cleo", "lastName": "Refresh", "groups": ["/smeltr-admins"],
                "credentials": [{ "type": "password", "value": KEYCLOAK_SECRET, "temporary": false }],
            }))
            .unwrap();
        assert_eq!(made.status(), 201, "Keycloak made {username}");
        KeycloakUser(
            format!("{admin}/users/{}", keycloak_id(bearer, &path)),
            bearer.to_string(),
        )
    }
}

impl Drop for KeycloakUser {
    fn drop(&mut self) {
        let _ = ureq::delete(&self.0).header("authorization", &self.1).call();
    }
}

/// The contract for refreshing: Keycloak refreshes a session with the grant it handed out, its refreshed ID token is
/// believed, a group taken away there ends the session here, and so does ending the person's sessions there.
#[test]
fn live_keycloak_refreshes_and_ends_sessions() {
    let Some(issuer) = crate::live::keycloak("live_keycloak_refreshes_and_ends_sessions") else {
        return;
    };
    let leader = start_with("oidc-keycloak-refresh", &[QUICK_REFRESH]);
    let op = operator(&leader);
    let (status, body) = leader.put_as(
        &op,
        "/api/settings/oidc",
        serde_json::json!({
            "enabled": true, "issuer": issuer, "clientId": "smeltr", "clientSecret": KEYCLOAK_SECRET,
        }),
    );
    assert_eq!((status, body["ok"].clone()), (200, serde_json::json!(true)), "{body}");
    let (status, role) = leader.put_as(
        &op,
        "/api/roles/smeltr-admins",
        serde_json::json!({ "administrator": true }),
    );
    assert_eq!(status, 200, "{role}");
    let (bearer, admin) = keycloak_admin(&issuer);
    let cleo = KeycloakUser::make(&bearer, &admin, "cleo");
    let group = keycloak_id(&bearer, &format!("{admin}/groups?search=smeltr-admins&exact=true"));
    let membership = format!("{}/groups/{group}", cleo.0);

    let session = |ended: (String, Option<String>)| {
        assert_eq!(ended.0, "/");
        Session {
            cookie: ended.1.expect("a session cookie"),
            csrf: String::new(),
        }
    };
    let first = session(keycloak_sign_in(&leader, "cleo"));
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert!(
        signed_in(&leader, &first),
        "a refresh through Keycloak ended the session"
    );

    let removed = ureq::delete(&membership)
        .header("authorization", &bearer)
        .call()
        .unwrap();
    assert_eq!(removed.status(), 204);
    eventually("leaving the role's group ending the session", || {
        !signed_in(&leader, &first)
    });
    let added = ureq::put(&membership)
        .header("authorization", &bearer)
        .send_empty()
        .unwrap();
    assert_eq!(added.status(), 204);

    let second = session(keycloak_sign_in(&leader, "cleo"));
    let logged_out = ureq::post(format!("{}/logout", cleo.0))
        .header("authorization", &bearer)
        .send_empty()
        .unwrap();
    assert_eq!(logged_out.status(), 204);
    eventually("ending the session at Keycloak ending it here", || {
        !signed_in(&leader, &second)
    });
}
