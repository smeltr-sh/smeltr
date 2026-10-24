//! The leader: worker groups, the heartbeat registry, git-backed bundle distribution (`git`), and worker enrollment
//! (`auth`).
//!
//! Authentication runs in two directions. A worker's Bearer token (`require_worker_auth`) keeps a spoofed worker from
//! talking to the leader; the certificate the leader serves, signed by the authority every worker trusts (`tls`),
//! keeps a worker from talking to a spoofed leader. Neither needs external PKI. A revoked credential blocks future
//! heartbeats and fetches; it never reaches into a running process.
//!
//! Operator access to the admin routes is the binary's concern (`src/bin/smeltr/admin_auth.rs`), which is why
//! [`admin_router`] is handed out separately: [`compose`] puts whatever gate the caller chose over that half only.
//!
//! Durable state (worker credentials, enrollment codes, a group's color) is behind `Store`. A worker group exists
//! exactly when `main` holds a file under `groups/<id>/`; its identity, name and configuration are all in the one git
//! repository (`git`) -- only its color is the store's. Heartbeat state is `Registry`: in-memory always, and
//! durable when opened against its own database (`registry::Registry::open`, `series`).
//!
//! Deploying moves the pointer workers fetch (`git::DEPLOYED_REF`) to a commit of `main`'s history. Every group
//! fetches it and decides for itself whether its own subtree moved (`worker::content_id`).

pub mod access;
pub mod accounts;
pub mod acme;
pub mod acme_dns;
pub mod agent;
pub mod auth;
pub mod capture;
pub mod connections;
pub mod deploy_checks;
pub mod directives;
pub mod docs;
pub mod enrollment_codes;
pub mod envelope;
pub mod forget;
pub mod git;
pub mod holds;
pub mod listen;
pub mod lock;
pub mod lookups;
pub mod map;
pub mod measurements;
pub mod model;
pub mod oidc;
pub mod onboard;
pub mod outbound;
pub mod preview;
pub mod private_file;
pub mod registry;
pub mod remote;
pub mod resolver;
pub mod secrets;
pub mod series;
pub mod sessions;
pub mod sqlite_store;
pub mod stats;
pub mod steps;
pub mod store;
pub mod throttle;
pub mod throughput;
pub mod tls;
pub mod tree;
pub mod updates;
pub mod warnings;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Extension, Json, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};

use crate::heartbeat::{HeartbeatRequest, HeartbeatResponse, ThreadKind};
use envelope::Envelope;
use registry::{Registry, ThreadHealth};
use store::{Spend, Store, StoreError};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn Store>,
    pub registry: Arc<Registry>,
    /// Capture sessions in flight. Empty whenever nobody is debugging.
    pub captures: Arc<capture::Captures>,
    /// Holds `repos/config.git`, the one repository every group's bundle is a subtree of.
    pub data_dir: PathBuf,
    /// Which secrets each group's bundle names, by the commit it was read at.
    pub entitlements: Arc<Entitlements>,
    /// What bounds the rate a secret may be asked about.
    pub verify_limiter: Arc<VerifyLimiter>,
    /// When each deployed identity's certificates run out, by the commit they were read at.
    pub expiries: Arc<Expiries>,
    /// Each source's last window against its normal, recomputed once per five-minute bucket (`throughput`).
    pub throughput: Arc<throughput::Readings>,
    /// What is wrong with the deployed commit: groups that do not load there, and tests that fail there.
    pub deploy_checks: Arc<deploy_checks::DeployChecks>,
    /// Every host a browser reaches this leader at, resolved once at startup. Fed into every certificate this
    /// leader signs for itself alongside `startup_config::leader_certificate_names`'s other names, so a rotation
    /// reissues under the same list a restart would -- never a certificate judged against a different one than
    /// the one it was issued for.
    pub origin_hosts: Vec<String>,
    /// Syncs with the git remote this leader follows, when one is set.
    pub remote: Arc<remote::Syncing>,
    /// The look for a newer release (`updates`), and what the last one found.
    pub updates: Arc<updates::Updates>,
    /// What operators have asked of workers and the workers have not yet taken.
    pub directives: Arc<directives::Directives>,
}

/// What each group is entitled to, remembered per commit. The answer changes only when the bundle does, so it is
/// computed when a deploy moves the fleet rather than on every heartbeat.
#[derive(Default)]
pub struct Entitlements(std::sync::Mutex<std::collections::HashMap<String, (String, Vec<SecretAt>)>>);

/// One credential a bundle quotes: its name, and the version that bundle runs.
pub type SecretAt = (String, i64);

/// When the certificates each deployed group's identities carry run out, remembered per `(group, commit)`.
/// Answering means loading a bundle and parsing PEM, and the answer changes only when the bundle does -- the same
/// reckoning [`Entitlements`] makes, about the same commit. Keyed by commit rather than overwritten in place: a
/// group mid-rollout has workers on two commits at once, and each needs its own answer.
#[derive(Default)]
pub struct Expiries(std::sync::Mutex<std::collections::HashMap<(String, String), Vec<warnings::DeployedCertificate>>>);

/// Every `(group, version)` a deployed bundle quotes, by secret name: the deployed commit's, and the one each of `running`'s
/// `(group, commit)` runs.
///
/// The catalog reads it to show an admin which value each group is actually on rather than only what was stored
/// last. A worker still on an older commit, mid-rollout or refusing the deployed one, runs what that commit quotes.
/// Retention keeps more than this ([`kept_versions`]).
///
/// An error when a running bundle cannot be read: what it quotes is unknown, and reading it as quoting nothing
/// would let retention forget a value that worker cannot get back.
///
/// Blocking; the caller is inside `spawn_blocking`.
fn deployed_versions(
    data_dir: &std::path::Path,
    cache: &Entitlements,
    running: &std::collections::HashSet<(String, String)>,
) -> Result<std::collections::HashMap<String, Vec<SecretAt>>, String> {
    let mut groups: Vec<String> = deployed_groups(data_dir).into_iter().collect();
    groups.sort();
    let mut out: std::collections::HashMap<String, Vec<SecretAt>> = std::collections::HashMap::new();
    for group in groups {
        for (name, version) in entitled_secrets(data_dir, cache, &group) {
            out.entry(name).or_default().push((group.clone(), version));
        }
    }
    let repo = git::repo_path(data_dir);
    for (group, commit) in running {
        let quoted = secrets_at(&repo, commit, group)
            .map_err(|e| format!("the bundle {group} is running at {commit} cannot be read: {e}"))?;
        for (name, version) in quoted {
            out.entry(name).or_default().push((group.clone(), version));
        }
    }
    for users in out.values_mut() {
        users.sort();
        users.dedup();
    }
    Ok(out)
}

/// The leader-stored secrets `group_id`'s bundle names in the commit the fleet runs.
///
/// Entitlement is what the bundle asks for: a worker is sent the credentials its own deployment needs and no
/// others, and there is nothing for an admin to keep in step with the configuration. A group whose bundle does not
/// load is entitled to nothing, which its worker meets as a refused deploy naming what it lacks.
///
/// Blocking; the caller is inside `spawn_blocking`.
fn entitled_secrets(data_dir: &std::path::Path, cache: &Entitlements, group_id: &str) -> Vec<SecretAt> {
    let repo = git::repo_path(data_dir);
    if !git::repo_ready(&repo) {
        return Vec::new();
    }
    let Some(head) = git::deployed(&repo) else {
        return Vec::new();
    };
    if let Some((at, names)) = cache.0.lock().unwrap().get(group_id)
        && at == &head
    {
        return names.clone();
    }
    let names = secrets_at(&repo, &head, group_id).unwrap_or_default();
    cache
        .0
        .lock()
        .unwrap()
        .insert(group_id.to_string(), (head, names.clone()));
    names
}

/// The leader-stored secrets `group_id`'s bundle names at `rev`, once each.
fn secrets_at(
    repo: &std::path::Path,
    rev: &str,
    group_id: &str,
) -> Result<Vec<SecretAt>, crate::config_file::BundleError> {
    use crate::plan::topology::SecretRef;

    let mut names = tree::load_group_at(repo, rev, group_id).map(|system| {
        system
            .topology
            .named_secrets
            .into_iter()
            .filter_map(|s| match s.reference {
                SecretRef::LeaderStored { name, version } => Some((name, version)),
                SecretRef::WorkerLocal(_) => None,
            })
            .collect::<Vec<_>>()
    })?;
    // `named_secrets` names one per connector, on purpose -- `missing_secrets` reads every one of those to say
    // which connectors are stuck, not just the first. A credential several connectors share, an identity's key
    // among them, would otherwise be sealed and sent once per connector naming it.
    names.sort();
    names.dedup();
    Ok(names)
}

/// Every version [`deployed_versions`] names, and those `main`'s head names: what retention and deletion keep. A
/// rollback holds the fleet on the deployed commit while releasing deploys `main`, so either may be what runs next.
/// A group that does not load at `main`'s head names nothing there: its workers refuse that bundle, and `running`
/// keeps what they run instead.
///
/// Blocking; the caller is inside `spawn_blocking`.
fn kept_versions(
    data_dir: &std::path::Path,
    cache: &Entitlements,
    running: &std::collections::HashSet<(String, String)>,
) -> Result<std::collections::HashMap<String, Vec<SecretAt>>, String> {
    let mut out = deployed_versions(data_dir, cache, running)?;
    let repo = git::repo_path(data_dir);
    if let Some(head) = git::repo_ready(&repo).then(|| git::main_head(&repo)).flatten() {
        for group in tree::groups_at(&repo, &head).unwrap_or_default() {
            for (name, version) in secrets_at(&repo, &head, &group).unwrap_or_default() {
                out.entry(name).or_default().push((group.clone(), version));
            }
        }
    }
    for users in out.values_mut() {
        users.sort();
        users.dedup();
    }
    Ok(out)
}

// -- heartbeat ----------------------------------------------------------

/// Records what each thread reported, and the worker process's own `stop_after_unreachable_secs` and running
/// commit. What to run is `git::DEPLOYED_REF`, which the worker fetches for itself; what a worker *is* running is
/// the worker's own answer, since one that refused a commit keeps running the one before it and says so here.
///
/// The body's `worker_id` has to be the one the token was issued to. A token identifies exactly one worker, so a
/// heartbeat claiming another is refused rather than recorded: the registry is keyed by `(worker_id, thread_id)` and
/// never evicts, so accepting a claim would let one credential invent unbounded workers, and report freshness for a
/// worker that is revoked or gone.
async fn heartbeat(
    State(state): State<AppState>,
    Extension(AuthenticatedWorker(worker_id)): Extension<AuthenticatedWorker>,
    Extension(kind): Extension<store::WorkerKind>,
    Json(mut req): Json<HeartbeatRequest>,
) -> Result<Json<HeartbeatResponse>, (axum::http::StatusCode, String)> {
    if req.worker_id != worker_id {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            format!(
                "this token belongs to worker {worker_id:?}, but the heartbeat reports {:?}; a worker keeps the id it \
                 enrolled with",
                req.worker_id
            ),
        ));
    }
    // One "now" for the whole request: what buckets every subject-scoped sample lands in (`Registry::record_heartbeat`,
    // when durable), and, below, the warning catalog's other periodic tick (design doc, Control plane: "recomputed
    // ... on
    // every heartbeat") -- reconciling the fact clock while the timing is exact, rather than waiting for someone to
    // ask for `/api/warnings`, since nothing else has a source of "now" if a fact starts and clears between two reads.
    let now_ms = warnings::now_millis();
    let threads = std::mem::take(&mut req.threads);
    let reported_group = threads.first().map(|t| t.group_id.clone());
    let stopping = req.stopping;
    let directive = state.directives.heard(
        &worker_id,
        std::mem::take(&mut req.accepts),
        req.took.as_deref(),
        stopping,
    );
    // One blocking hop for both, and off the runtime's threads: each is a write transaction behind the registry's
    // one connection lock, and a fleet all beating at once would otherwise park every async thread the leader has.
    // Logs first and separately committed -- what a worker had to say about a deployment it is about to describe is
    // worth having even if describing it fails.
    {
        let registry = state.registry.clone();
        let worker = worker_id.clone();
        let events = std::mem::take(&mut req.logs);
        let streams = req.streams.take();
        let streams_window = req.streams_window;
        let boot_id = req.boot_id;
        // Empty from a worker too old to state one, which is stored as no group rather than as a group named "".
        let group = Some(req.group_id.clone()).filter(|g| !g.is_empty());
        let (stop_after, commit) = (req.stop_after_unreachable_secs, req.commit.clone());
        let smeltr_version = req.smeltr_version.clone();
        let release = req.release.clone();
        let supervising = req.supervising.clone();
        let resources = req.resources;
        let data_dir = state.data_dir.clone();
        let authority_expires = req.authority_expires_millis;
        // Kept by worker rather than by thread, so a worker running nothing still counts toward a rotation. Set
        // before the blocking hop, because the answer below is built from it.
        state
            .registry
            .record_authority(&worker_id, req.authority_fingerprints.clone(), req.takes_authority);
        let recorded = tokio::task::spawn_blocking(move || {
            registry.record_logs(&worker, boot_id, group.as_deref(), &events);
            if let Some(streams) = &streams {
                let window = streams_window.map(|seq| series::StreamWindow {
                    worker_id: &worker,
                    boot_id,
                    seq,
                });
                // A worker that names no commit, or a leader with nothing deployed, cannot tell a rollout apart, so
                // the report counts as the deployed one.
                let deployed = match (&commit, git::deployed(&git::repo_path(&data_dir))) {
                    (Some(running), Some(deployed)) => *running == deployed,
                    _ => true,
                };
                registry.record_streams(group.as_deref(), streams, window, deployed, now_ms);
            }
            registry.record_heartbeat(
                &worker,
                threads,
                stop_after,
                commit,
                smeltr_version,
                release,
                supervising,
                resources,
                authority_expires,
                stopping,
                now_ms,
            );
        })
        .await;
        // Said rather than swallowed: this is what updates the live view as well as the durable one, so a worker
        // whose report never landed reads as silent on `/api/health` while still being answered here.
        if let Err(e) = recorded {
            tracing::warn!(
                target: "leader.heartbeat_not_recorded",
                subject = %worker_id,
                error = %e,
                "a heartbeat could not be recorded, so this worker's threads stay as they were until the next one"
            );
        }
    }
    // Before the forgetting below, so a stopping ephemeral worker's last progress lands and the worker is not
    // heard again after it. A beat whose progress may not have landed is refused, so the worker sends it again
    // rather than count it as recorded.
    let holds = {
        let registry = state.registry.clone();
        let (worker, group) = (worker_id.clone(), req.group_id.clone());
        let runs = std::mem::take(&mut req.single_worker);
        let progress = std::mem::take(&mut req.progress);
        let interval = req.heartbeat_interval_ms.map(std::time::Duration::from_millis);
        let clock = holds::Clock::at(state.registry.up_for());
        let settled = run_store(state.store.clone(), move |store| {
            let beat = holds::Beat {
                worker: &worker,
                group: &group,
                runs,
                progress,
                stopping,
                interval,
            };
            registry.holds.beat(store, beat, clock)
        })
        .await;
        settled.map_err(|e| {
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("could not record which single-worker sources this worker holds: {e}"),
            )
        })?
    };
    let heartbeat_interval = crate::startup_config::worker_heartbeat_interval();
    let group_id = reported_group.unwrap_or_default();
    let data_dir = state.data_dir.clone();
    let entitlements = state.entitlements.clone();
    let for_group = group_id.clone();
    // One blocking hop for both: each reads the repository, and neither belongs on the async runtime's threads.
    let checks = state.deploy_checks.clone();
    let (deployed, entitled, findings) = tokio::task::spawn_blocking(move || {
        (
            deployed_groups(&data_dir),
            entitled_secrets(&data_dir, &entitlements, &for_group),
            checks.findings(&data_dir),
        )
    })
    .await
    .unwrap_or_default();
    // Decided before the cutover below, not after. The bundle this beat can still be handed holds both roots;
    // the one the cutover leaves behind holds only the incoming root, and a worker that has not taken it yet
    // cannot verify the leader that would hand it over. So the worker whose beat carries the deadline past gets
    // the overlap it was promised, and the shortened bundle on the beat after.
    //
    // The one moment a replaced authority can reach a worker: it asks, over a connection its own token
    // authenticates and its current authority verified.
    let authority = authority_to_hand(&state.data_dir, &req.authority_fingerprints, req.takes_authority);

    // An ephemeral worker that says it is stopping is gone, and does not come back as itself. Before anything
    // below reads the roster, so a rotation does not wait on it.
    if stopping && kind == store::WorkerKind::Ephemeral {
        forget::forget(&state, &worker_id).await;
    }
    // The leader's only periodic tick, so the look for ephemeral workers gone without a word rides it too.
    forget::sweep(&state, now_ms).await;
    if state.registry.prune_due(now_ms, PRUNE_PROGRESS_EVERY) {
        let state = state.clone();
        tokio::spawn(async move { prune_source_progress(&state).await });
    }

    // A rotation ends on a fact workers report, and this is where they report it. The leader has no other
    // periodic tick (design doc, Control plane), so the check rides the beat that carries the evidence.
    cut_over_if_due(&state, now_ms).await;

    let snapshot = state.registry.snapshot();
    let mut active_facts = warnings::currently_active_facts(&snapshot, now_ms, heartbeat_interval, &deployed);
    active_facts.extend(warnings::deploy_facts(&findings, &state.registry));
    let readings = throughput::readings(&state, now_ms, heartbeat_interval, false).await;
    let recovering = warnings::recovering_sources(state.registry.series(), now_ms);
    active_facts.extend(warnings::outside_normal_facts(
        &readings,
        &snapshot,
        &recovering,
        heartbeat_interval,
    ));
    state.registry.observe_facts(&active_facts, now_ms);

    // A finished session that nobody collected would otherwise hold its records until the process restarted. The
    // heartbeat is the leader's only periodic tick, so the sweep rides it.
    state
        .captures
        .sweep(std::time::Duration::from_secs(capture::MAX_DEADLINE_SECS));
    // The whole of what this worker should run: it lets go of a session still filling that this leaves out
    // (`Captures::invitations_for`). On the blocking pool, because `holder` waits on any beat in progress, which
    // holds the holds lock across its store writes; an async thread parked on that would stall every handler.
    let (captures_for, secrets) = tokio::task::spawn_blocking(move || {
        let captures_for = state.captures.invitations_for(&worker_id, &group_id, |source| {
            state.registry.holds.holder(&group_id, source)
        });
        let secrets = state
            .store
            .secrets_for_delivery(&entitled, &req.secret_versions)
            .unwrap_or_default();
        (captures_for, secrets)
    })
    .await
    .unwrap_or_default();

    Ok(Json(HeartbeatResponse {
        holds,
        authority,
        directive,
        captures: captures_for
            .into_iter()
            .map(|invitation| crate::heartbeat::Invited::Readable(Box::new(invitation)))
            .collect(),
        secrets: secrets
            .into_iter()
            .map(|(name, version, value)| crate::heartbeat::SecretDelivery { name, version, value })
            .collect(),
    }))
}

// -- capture --------------------------------------------------------------

/// A worker handing back what it captured. Gated by the same worker token the heartbeat is, and the body's
/// `worker_id` has to be the one that token was issued to -- otherwise one credential could answer for every worker
/// in a session and fill a sample with records nobody can attribute.
async fn capture_report(
    State(state): State<AppState>,
    Extension(AuthenticatedWorker(worker_id)): Extension<AuthenticatedWorker>,
    Json(report): Json<crate::heartbeat::CaptureReport>,
) -> Result<axum::http::StatusCode, (axum::http::StatusCode, String)> {
    if report.worker_id != worker_id {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            format!(
                "this credential is worker {worker_id:?}; it cannot report a capture as {:?}",
                report.worker_id
            ),
        ));
    }
    state.captures.record(report);
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// `f` on the blocking pool: where a handler spawns git, reads the store or compiles, none of which belongs on the
/// async executor. A panic in `f` is the error it answers with, never a request left hanging.
pub async fn blocking<T, E>(f: impl FnOnce() -> Result<T, E> + Send + 'static) -> Result<T, E>
where
    T: Send + 'static,
    E: From<String> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| Err(E::from(format!("internal error: {e}"))))
}

/// A `Store` call on the pool.
async fn run_store<T: Send + 'static>(
    store: Arc<dyn Store>,
    f: impl FnOnce(&dyn Store) -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    blocking(move || f(store.as_ref())).await
}

// -- lookup files ------------------------------------------------------------

/// One file of the lookup store, streamed. A worker checks what arrives against the pointer it asked by, so this
/// serves a name and vouches for nothing.
async fn lookup_file(State(state): State<AppState>, Path(oid): Path<String>) -> Response {
    if !crate::lookup::store::is_oid(&oid) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = crate::lookup::store::path_of(&git::repo_path(&state.data_dir), &oid);
    match tokio::fs::File::open(path).await {
        Ok(file) => axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

// -- single-worker sources ---------------------------------------------------

/// How many ledger entries one store read takes: small enough that a heartbeat waiting on the store is not held up
/// for long by a fetch of millions.
const LEDGER_PAGE: usize = 1000;

/// The ledger of a single-worker source this worker holds, as NDJSON: one [`crate::heartbeat::LedgerEntry`] per
/// line, in item order. What a worker taking a hold reads before it opens the source. 404 unless the worker holds
/// it now, in its own group.
///
/// Streamed a page at a time. A store error partway ends the body broken rather than short, so the worker sees a
/// failed fetch and never a smaller ledger.
async fn ledger(
    State(state): State<AppState>,
    Extension(AuthenticatedWorker(worker_id)): Extension<AuthenticatedWorker>,
    Query(key): Query<crate::heartbeat::SourceKey>,
) -> Response {
    let registry = state.registry.clone();
    let lookup = key.clone();
    let group = run_store(state.store.clone(), move |store| {
        let Some(group) = registry.holds.held_in(&worker_id, &lookup) else {
            return Ok(None);
        };
        let holder = store.source_hold(&group, &lookup)?.map(|hold| hold.holder);
        Ok((holder.as_deref() == Some(worker_id.as_str())).then_some(group))
    })
    .await;
    let group = match group {
        Ok(Some(group)) => group,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response(),
    };

    // Two pages in flight at most: a slow reader holds back the store reads rather than the leader's memory.
    let (pages, rx) = tokio::sync::mpsc::channel(2);
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || {
        let mut after: Option<String> = None;
        loop {
            let page = match store.ledger_page(&group, &key, after.as_deref(), LEDGER_PAGE) {
                Ok(page) => page,
                Err(e) => {
                    let _ = pages.blocking_send(Err(std::io::Error::other(e.to_string())));
                    return;
                }
            };
            let mut body = Vec::new();
            for entry in &page {
                // A struct of two strings always serializes.
                serde_json::to_writer(&mut body, entry).expect("a ledger entry serializes");
                body.push(b'\n');
            }
            // A send that fails is a reader that went away.
            if (!body.is_empty() && pages.blocking_send(Ok(body)).is_err()) || page.len() < LEDGER_PAGE {
                return;
            }
            after = page.last().map(|entry| entry.item.clone());
        }
    });
    let body = axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(Pages {
        rx,
        chunk: Vec::new(),
        at: 0,
    }));
    ([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response()
}

/// A ledger's pages as one byte stream, for a response body. An error from the pages is an error from the read, which
/// breaks the body off rather than ending it.
struct Pages {
    rx: tokio::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    at: usize,
}

impl tokio::io::AsyncRead for Pages {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            if self.at < self.chunk.len() {
                let n = buf.remaining().min(self.chunk.len() - self.at);
                buf.put_slice(&self.chunk[self.at..self.at + n]);
                self.at += n;
                return std::task::Poll::Ready(Ok(()));
            }
            match std::task::ready!(self.rx.poll_recv(cx)) {
                Some(Ok(chunk)) => {
                    self.chunk = chunk;
                    self.at = 0;
                }
                Some(Err(e)) => return std::task::Poll::Ready(Err(e)),
                None => return std::task::Poll::Ready(Ok(())),
            }
        }
    }
}

/// Every `(group, commit)` a live worker reports running. Without the roster no worker counts as revoked, so a
/// failed read keeps more rather than less.
async fn running_deployments(state: &AppState) -> std::collections::HashSet<(String, String)> {
    let credentials = run_store(state.store.clone(), |s| s.list_worker_credentials())
        .await
        .unwrap_or_default();
    let interval = crate::startup_config::worker_heartbeat_interval();
    warnings::running_deployments(&state.registry.snapshot(), &credentials, interval)
}

/// Forget the stored lookup files retention no longer keeps (`lookups::prune`), keeping what the fleet runs now.
pub async fn prune_lookups(state: &AppState) {
    let running = running_deployments(state).await;
    let repo = git::repo_path(&state.data_dir);
    let keep = crate::startup_config::leader_lookup_versions();
    let _ = tokio::task::spawn_blocking(move || lookups::prune(&repo, keep, &running)).await;
}

/// How often the leader looks for the progress of sources no bundle reads.
const PRUNE_PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_secs(300);

/// Forgets the stream shapes of every source, and the hold and progress of every single-worker source, that no
/// bundle the fleet runs, or may run next, reads at that place ([`read_sources`]) and no live worker lists
/// ([`holds::Holds::forget_unread`]), so `leader.db` keeps what is deployed rather than all that ever was. A source
/// that comes back afterwards starts afresh. Nothing is forgotten when a running bundle cannot be read.
pub async fn prune_source_progress(state: &AppState) {
    let running = running_deployments(state).await;
    let data_dir = state.data_dir.clone();
    let read = match blocking(move || read_sources(&data_dir, &running)).await {
        Ok(read) => read,
        Err(e) => {
            tracing::warn!(
                target: "leader.progress_not_pruned",
                error = %e,
                "could not read which sources the fleet reads, so no source's progress is forgotten this time"
            );
            return;
        }
    };
    // What a removed source showed of its streams goes with it, as its progress does.
    if let Some(all) = read.all {
        let registry = state.registry.clone();
        let forgotten = blocking(move || match registry.series() {
            Some(series) => series.forget_streams_except(&all).map_err(|e| e.to_string()),
            None => Ok(()),
        });
        if let Err(e) = forgotten.await {
            tracing::warn!(
                target: "leader.streams_not_pruned",
                error = %e,
                "the streams of sources no bundle reads could not be forgotten, so they are looked for again"
            );
        }
    }
    let read = read.single;
    let (registry, store) = (state.registry.clone(), state.store.clone());
    let clock = holds::Clock::at(registry.up_for());
    let forgotten = blocking(move || registry.holds.forget_unread(store.as_ref(), &read, clock))
        .await
        .map(|held| {
            held.into_iter()
                .map(|h| format!("{} {} at {}", h.group, h.key.source, h.key.place))
                .collect::<Vec<_>>()
        });
    match forgotten {
        Ok(forgotten) if forgotten.is_empty() => {}
        Ok(forgotten) => tracing::info!(
            target: "leader.progress_forgotten",
            sources = %forgotten.join(", "),
            "the progress of sources no deployed bundle reads was forgotten"
        ),
        Err(e) => tracing::warn!(
            target: "leader.progress_not_pruned",
            error = %e,
            "the progress of sources no bundle reads could not be forgotten, so it is looked for again"
        ),
    }
}

/// What the bundles retention keeps ([`kept_versions`]) read: each deployed group's at the deployed commit, each of
/// `running`'s `(group, commit)`, and each group's at `main`'s head.
#[derive(Default)]
struct ReadSources {
    /// Every `(group, source)` a single-worker source is read as.
    single: std::collections::HashSet<(String, crate::heartbeat::SourceKey)>,
    /// Every `(group, source id)` of any kind. `None` where the leader has no repository yet, which says nothing of
    /// what the fleet reads.
    all: Option<std::collections::HashSet<(String, String)>>,
}

/// What the bundles retention keeps read ([`ReadSources`]). An error when a running bundle cannot be read, since
/// what it reads is then unknown.
///
/// Blocking; the caller is inside `spawn_blocking`.
fn read_sources(
    data_dir: &std::path::Path,
    running: &std::collections::HashSet<(String, String)>,
) -> Result<ReadSources, String> {
    let repo = git::repo_path(data_dir);
    let mut out = std::collections::HashSet::new();
    let mut all = std::collections::HashSet::new();
    if !git::repo_ready(&repo) {
        return Ok(ReadSources::default());
    }
    let mut add = |rev: &str, group: &str| {
        tree::load_group_at(&repo, rev, group).map(|system| {
            for source in system.topology.sources {
                all.insert((group.to_string(), source.name.clone()));
                if let Some(place) = source.kind.single_worker() {
                    let key = crate::heartbeat::SourceKey {
                        source: source.name,
                        place,
                    };
                    out.insert((group.to_string(), key));
                }
            }
        })
    };
    for (group, commit) in running {
        let unreadable =
            |e: &dyn std::fmt::Display| format!("the bundle {group} is running at {commit} cannot be read: {e}");
        if !git::has_commit(&repo, commit).map_err(|e| unreadable(&e))? {
            return Err(unreadable(&"this leader does not have that commit"));
        }
        // A worker of a group its commit has no bundle for runs an empty deployment, which reads nothing.
        if tree::groups_at(&repo, commit)
            .map_err(|e| unreadable(&e))?
            .contains(group)
        {
            add(commit, group).map_err(|e| unreadable(&e))?;
        }
    }
    // A group whose bundle does not load at a commit reads nothing there: its workers refuse it, and `running` keeps
    // what they read instead.
    for rev in [git::deployed(&repo), git::main_head(&repo)].into_iter().flatten() {
        for group in tree::groups_at(&repo, &rev).unwrap_or_default() {
            let _ = add(&rev, &group);
        }
    }
    Ok(ReadSources {
        single: out,
        all: Some(all),
    })
}

// -- git smart-HTTP, read-only ----------------------------------------------

/// The header a worker names its build's commit in on every request for the repository (`version::commit`).
pub const BUILD_HEADER: &str = "smeltr-commit";

/// The header a worker names its release in beside [`BUILD_HEADER`] (`version::RELEASE`), on a build that is one.
pub const RELEASE_HEADER: &str = "smeltr-release";

/// Every git HTTP request, handed to `git http-backend`. One handler rather than a route per service: the paths a
/// client asks for are git's to define, and enumerating them here would be a third place that has to agree.
async fn git_http(
    State(state): State<AppState>,
    method: axum::http::Method,
    Path(path): Path<String>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    // A worker takes a deployment only from a leader of a compatible build, so no other build is handed one: what
    // one writes, the other may read differently. The worker keeps running what it has, and the leader reports it
    // from the worker's heartbeat (`warnings`).
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let asked = header(BUILD_HEADER).unwrap_or("");
    let asked_release = header(RELEASE_HEADER);
    if !crate::version::compatible(crate::version::commit(), crate::version::RELEASE, asked, asked_release) {
        return (
            axum::http::StatusCode::CONFLICT,
            format!(
                "this leader is {}, and deploys to no worker on {}",
                build_name(crate::version::commit(), crate::version::RELEASE),
                build_name(asked, asked_release),
            ),
        )
            .into_response();
    }
    let repo_path = git::repo_path(&state.data_dir);
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let query = query.unwrap_or_default();
    let method = method.to_string();

    let served = blocking(move || {
        git::http_backend(&repo_path, &path, &method, &query, content_type.as_deref(), &body).map_err(|e| e.to_string())
    })
    .await;

    match served {
        Ok(cgi) => {
            let mut response = axum::response::Response::builder()
                .status(axum::http::StatusCode::from_u16(cgi.status).unwrap_or(axum::http::StatusCode::OK));
            for (name, value) in &cgi.headers {
                response = response.header(name, value);
            }
            response
                .body(axum::body::Body::from(cgi.body))
                .unwrap_or_else(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(error) => {
            // The worker sees only a 500, so this is the one place the cause is said.
            tracing::error!(target: "leader.git_serve_failed", error = %error, "a worker's fetch could not be served");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// A build as people tell it apart: its release when it is one, else its commit.
pub fn build_name(commit: &str, release: Option<&str>) -> String {
    match (release.filter(|r| !r.is_empty()), commit) {
        (Some(release), _) => format!("release {release}"),
        (None, "") => "an unnamed build".to_string(),
        (None, commit) => format!("commit {commit}"),
    }
}

// -- worker health status --------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupHealth {
    pub group_id: String,
    /// The pool: what `thread_limit` sizes, and the only one an operator sets.
    pub transform_threads: usize,
    /// One per source, set by how many sources the group's bundle declares.
    pub readers: usize,
    /// One per destination, set by how many the group delivers to.
    pub egress_threads: usize,
    pub partitions_owned: usize,
}
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub threads: Vec<ThreadHealth>,
    pub groups: Vec<GroupHealth>,
    /// How long a worker may go without a heartbeat before it is silent: the window `workerSilent` is raised on, so
    /// a page counting silent workers agrees with it.
    pub silent_after_ms: u64,
}

/// What `GET /api/health` serves, for the binary's in-process Cluster page.
pub fn health_snapshot(state: &AppState) -> HealthResponse {
    let snapshot = state.registry.snapshot();

    // Counted per kind, because each stage's size comes from something the others cannot influence (design doc,
    // Runtime stages). `Worker` is
    // the process itself, not a thread; a worker older than the pool reports no kind at all, and counts against nothing
    // rather than being guessed at.
    let mut groups: std::collections::BTreeMap<String, GroupHealth> = std::collections::BTreeMap::new();
    for t in &snapshot {
        let g = groups.entry(t.report.group_id.clone()).or_insert_with(|| GroupHealth {
            group_id: t.report.group_id.clone(),
            transform_threads: 0,
            readers: 0,
            egress_threads: 0,
            partitions_owned: 0,
        });
        match t.report.kind {
            Some(ThreadKind::Transform) => g.transform_threads += 1,
            Some(ThreadKind::Reader) => g.readers += 1,
            Some(ThreadKind::Egress) => g.egress_threads += 1,
            Some(ThreadKind::Worker) | None => {}
        }
        g.partitions_owned += t.report.partitions_owned.len();
    }

    HealthResponse {
        threads: snapshot,
        groups: groups.into_values().collect(),
        silent_after_ms: heartbeat_window().as_millis() as u64,
    }
}

/// What `GET /api/system/info` serves: this leader process's own build, for Admin -> System -> General, and what
/// that build can do that another build of the same version might not.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub smeltr_version: &'static str,
    /// The release this build is (`version::RELEASE`); absent on any other build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release: Option<&'static str>,
    /// The newest published release, when it is newer than this one (`updates`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newer_release: Option<updates::Newer>,
    /// The last look for one: when, and why it failed if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_check: Option<updates::Looked>,
    /// The SASL mechanisms a Kafka client here can select. GSSAPI is in it only where a provider was linked, and a
    /// form that offered one this build does not carry would be offering a save that is refused.
    pub sasl_mechanisms: Vec<&'static str>,
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    forget::sweep(&state, warnings::now_millis()).await;
    Json(health_snapshot(&state))
}

async fn system_info(State(state): State<AppState>) -> Json<SystemInfo> {
    Json(SystemInfo {
        smeltr_version: crate::version::VERSION,
        release: crate::version::RELEASE,
        newer_release: state.updates.newer(),
        release_check: state.updates.looked(),
        sasl_mechanisms: crate::io::kafka::available_sasl_mechanisms(),
    })
}

// -- warnings ---------------------------------------------------------------

/// The fixed catalog (design doc, Control plane) without its info entries: what every view that judges health reads,
/// so none of them can count a fact that asks nothing as trouble. [`catalog`] is the whole of it.
pub async fn warnings_snapshot(state: &AppState) -> Result<Vec<warnings::Warning>, StoreError> {
    let mut out = catalog(state).await?;
    out.retain(|w| w.severity != warnings::Severity::Info);
    Ok(out)
}

/// What `GET /api/warnings?info=true` serves: the fixed catalog (design doc, Control plane), info entries and all,
/// derived fresh from the registry and the worker roster on every call -- there is no warning table, so this is the
/// only place the answer lives.
///
/// A route of its own rather than a field on [`HealthResponse`]: the "enrolled, never heartbeating" entry needs the
/// worker roster, which lives in `Store` and so must be read on the blocking pool ([`run_store`]) -- and
/// [`health_snapshot`] is also called synchronously from the in-process Cluster page, which a store read would
/// block. The GUI pages that show warnings already poll `/api/health` on a timer; this rides the same timer as a
/// second, independently-cheap fetch rather than reshaping the first.
pub async fn catalog(state: &AppState) -> Result<Vec<warnings::Warning>, StoreError> {
    let now_ms = warnings::now_millis();
    // What the page polls, and so one of the reads a look for gone ephemeral workers rides: a fleet that has
    // scaled to nothing sends no heartbeat to carry it.
    forget::sweep(state, now_ms).await;
    let heartbeat_interval = crate::startup_config::worker_heartbeat_interval();
    let snapshot = state.registry.snapshot();
    let data_dir = state.data_dir.clone();
    let checks = state.deploy_checks.clone();
    let (deployed, held, findings, shared_pipelines) = tokio::task::spawn_blocking(move || {
        let hold = git::hold(&git::repo_path(&data_dir)).ok().flatten();
        (
            deployed_groups(&data_dir),
            hold,
            checks.findings(&data_dir),
            deployed_shared_pipelines(&data_dir),
        )
    })
    .await
    .unwrap_or_default();
    let mut active_facts = warnings::currently_active_facts(&snapshot, now_ms, heartbeat_interval, &deployed);
    active_facts.extend(warnings::deploy_facts(&findings, &state.registry));
    let readings = throughput::readings(state, now_ms, heartbeat_interval, true).await;
    let recovering = warnings::recovering_sources(state.registry.series(), now_ms);
    active_facts.extend(warnings::outside_normal_facts(
        &readings,
        &snapshot,
        &recovering,
        heartbeat_interval,
    ));
    let key = run_store(state.store.clone(), |s| s.key_standing()).await?;
    active_facts.extend(warnings::encryption_key_facts(&key));
    let starts = state.registry.observe_facts(&active_facts, now_ms);
    let key_warnings = warnings::encryption_key_warnings(&key, &starts);
    let normal_warnings =
        warnings::outside_normal_warnings(&readings, &snapshot, &recovering, &starts, heartbeat_interval);
    let credentials = run_store(state.store.clone(), |s| s.list_worker_credentials()).await?;
    // The other place a rotation's deadline is looked at. The heartbeat carries the evidence a cutover waits on,
    // but a leader whose workers have all gone quiet receives none -- and this is what the page polls while
    // somebody is watching the rotation it promised would end.
    cut_over_if_due(state, now_ms).await;
    let (signing, rotating) = serving_authority(&state.data_dir);
    let presenting = enrollment_codes::Presenting::read(&tls::dir_of(&state.data_dir));
    // Read only while a rotation is open, which is the only time an enrollment code can be about to be stranded by one.
    // The steady state is every poll, and it does not pay for a table nothing would say anything about. Asked of the
    // same value the standing is read from, so the two cannot disagree about what an empty list means.
    let codes = if presenting.rotating() {
        run_store(state.store.clone(), move |s| s.list_usable_enrollment_codes(now_ms)).await?
    } else {
        Vec::new()
    };
    // Off the async runtime: reading the tree and parsing PEM is blocking work, and it is what the cache is for
    // that it is done once per commit rather than once per poll.
    let data_dir = state.data_dir.clone();
    let expiries = state.expiries.clone();
    let running = warnings::running_deployments(&snapshot, &credentials, heartbeat_interval);
    let certificates = tokio::task::spawn_blocking(move || deployed_certificates(&data_dir, &expiries, running))
        .await
        .unwrap_or_default();
    // Off the async runtime: `worker_logged_warnings` reads the logs table the same way `GET /api/logs` does, and
    // that handler already makes this same trip through `spawn_blocking` rather than block the runtime on it.
    let registry = state.registry.clone();
    let remote_failure = state.remote.state().failure;
    let newer_release = state.updates.newer();
    let deploy_warnings = warnings::deploy_checked(&findings, &starts);
    let mut out = tokio::task::spawn_blocking(move || {
        warnings::derive(
            &snapshot,
            &credentials,
            &starts,
            now_ms,
            heartbeat_interval,
            registry.series(),
            &deployed,
            signing.as_ref(),
            &rotating,
            &codes,
            &presenting,
            &certificates,
            held.as_ref(),
            remote_failure.as_ref(),
            newer_release.as_ref(),
        )
    })
    .await
    .unwrap_or_default();
    out.extend(deploy_warnings);
    out.extend(normal_warnings);
    out.extend(key_warnings);
    for w in out
        .iter_mut()
        .filter(|w| w.subject_kind == warnings::SubjectKind::Pipeline)
    {
        w.scope = match shared_pipelines.contains(&w.subject_id) {
            true => Some(crate::config_file::SHARED_DIR.to_string()),
            false => w.group.clone(),
        };
    }
    Ok(out)
}

/// The root this leader signs the certificate it serves under, and whether it is already rotating away from it.
///
/// Of however many `ca.pem` holds: the others are there so a worker keeps verifying across a change, and their
/// dates are nobody's to act on. Both `None` and false where this leader serves plain HTTP, which has no authority
/// to run out.
fn serving_authority(data_dir: &std::path::Path) -> (Option<tls::Described>, warnings::Rotating) {
    // A leader behind a proxy has no authority of its own, whatever files an earlier configuration left behind:
    // warning about one nothing presents is a warning nobody can act on.
    if !crate::startup_config::leader_tls_enabled() {
        return (None, warnings::Rotating::None);
    }
    let dir = tls::dir_of(data_dir);
    // Which of the three, not merely whether a file is there. A rotation that cannot reach a cutover is when the
    // operator most needs telling that the authority is running out -- and telling them to start another would
    // send them to a page that refuses while this one is open.
    let rotating = match tls::rotation(&dir) {
        Ok(None) => warnings::Rotating::None,
        Ok(Some(_)) => match tls::cut_over_is_possible(&dir) {
            Ok(()) => warnings::Rotating::UnderWay,
            Err(why) => warnings::Rotating::Stuck(why.message().to_string()),
        },
        Err(why) => warnings::Rotating::Unreadable(why),
    };
    let signing = tls::in_force(&dir)
        .ok()
        // An operator's certificate is issued by an authority this leader does not hold, and rotating it is
        // theirs to do wherever they issue it. Telling them to use a page that refuses is worse than silence.
        .filter(|(materials, _)| !materials.supplied)
        .and_then(|(materials, served)| tls::signing_root(&materials.ca_pem, &served).ok());
    (signing, rotating)
}

/// Every certificate the fleet is actually carrying, group by group.
///
/// Only identities a connector names, and only on the commit each worker itself reports running -- never `main`,
/// which a worker still on an older commit, or one refusing the current one, is not. A group mid-rollout has
/// workers split across two commits, and both are read: replacing an expired certificate on `main` must not clear
/// the warning while a worker still runs the one it replaced, and a commit nothing has deployed yet must not raise
/// one. A group whose bundle does not load contributes none -- something else already says so, and a warning about
/// a certificate inside a bundle nothing can read would be the second answer to a question already asked.
fn deployed_certificates(
    data_dir: &std::path::Path,
    cache: &Expiries,
    running: std::collections::HashSet<(String, String)>,
) -> Vec<warnings::DeployedCertificate> {
    let repo = git::repo_path(data_dir);
    if !git::repo_ready(&repo) {
        return Vec::new();
    }

    // Rebuilt rather than pruned in place: a commit no worker is running any more must not linger and grow this
    // cache forever, and the fleet moves on from one every time it redeploys.
    let mut cached = cache.0.lock().unwrap();
    let mut fresh = std::collections::HashMap::with_capacity(running.len());
    let mut out = Vec::new();
    for (group, commit) in running {
        let key = (group.clone(), commit.clone());
        let found = cached.remove(&key).unwrap_or_else(|| {
            tree::load_group_at(&repo, &commit, &group)
                .map(|system| certificates_of(&system.topology, &group, &commit))
                .unwrap_or_default()
        });
        out.extend(found.iter().cloned());
        fresh.insert(key, found);
    }
    *cached = fresh;
    out
}

/// What a group's identities say about how long they are good for. PEM the connector would refuse is passed over:
/// a bundle holding one never validated, and the save that refused it is where that is said.
fn certificates_of(
    topology: &crate::plan::topology::TopologyConfig,
    group: &str,
    commit: &str,
) -> Vec<warnings::DeployedCertificate> {
    let mut out = Vec::new();
    for identity in &topology.identities {
        for (half, pem) in [
            (warnings::Half::Presented, identity.certificate.as_deref()),
            (warnings::Half::Trusted, identity.authorities.as_deref()),
        ] {
            let Some(pem) = pem else { continue };
            let Ok(described) = tls::describe_each(pem.as_bytes(), "it") else {
                continue;
            };
            out.extend(described.into_iter().map(|c| warnings::DeployedCertificate {
                group: group.to_string(),
                commit: commit.to_string(),
                identity: identity.id.clone(),
                half,
                not_before_millis: c.not_before_millis,
                not_after_millis: c.not_after_millis,
            }));
        }
    }
    out
}

fn deployed_groups(data_dir: &std::path::Path) -> std::collections::HashSet<String> {
    let repo = git::repo_path(data_dir);
    if !git::repo_ready(&repo) {
        return std::collections::HashSet::new();
    }
    let Some(deployed) = git::deployed(&repo) else {
        return std::collections::HashSet::new();
    };
    tree::groups_at(&repo, &deployed)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

/// The pipelines the deployed commit declares in `shared/`, which a warning about one links to there rather than to
/// its group.
fn deployed_shared_pipelines(data_dir: &std::path::Path) -> std::collections::HashSet<String> {
    let repo = git::repo_path(data_dir);
    if !git::repo_ready(&repo) {
        return std::collections::HashSet::new();
    }
    let Some(deployed) = git::deployed(&repo) else {
        return std::collections::HashSet::new();
    };
    let mut files = tree::TreeFiles::new(&repo, &deployed, crate::config_file::SHARED_DIR);
    crate::config_file::read_raw_from(&mut files)
        .file
        .pipelines
        .into_iter()
        .map(|p| p.id)
        .collect()
}

/// `GET /api/warnings`'s query: whether the catalog's info entries come too.
#[derive(serde::Deserialize)]
struct WarningsQuery {
    #[serde(default)]
    info: bool,
}

/// The catalog narrowed to what the caller may see ([`warnings::Warning::visible_to`]): the gate let in anyone
/// viewing somewhere, and the catalog is the whole fleet's.
async fn list_warnings(
    State(state): State<AppState>,
    axum::Extension(access): axum::Extension<access::Access>,
    Query(query): Query<WarningsQuery>,
) -> Result<Json<Vec<warnings::Warning>>, (axum::http::StatusCode, String)> {
    let all = match query.info {
        true => catalog(&state).await,
        false => warnings_snapshot(&state).await,
    };
    all.map(|all| Json(all.into_iter().filter(|w| w.visible_to(&access)).collect()))
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// -- flow (E2) ----------------------------------------------------------------

/// `GET /api/flow`'s query: how far back to look, in minutes -- the window the caller chooses (design doc E2),
/// sixty by default and clamped to thirty days, the longest tier keeps.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FlowQuery {
    #[serde(default = "default_window_minutes")]
    window_minutes: i64,
}

fn default_window_minutes() -> i64 {
    60
}

/// The longest window any read serves: the longest tier's retention, past which there is nothing left to serve
/// from (`series::Tier::for_window`).
pub const MAX_WINDOW_MINUTES: i64 = 30 * 24 * 60;

/// One bucket of a [`FlowSeries`]. Absent from `points` entirely when the bucket has no row -- never a zero
/// (`series` module docs, "Retention"): a gap reads as no data, not as nothing moving.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowPoint {
    pub bucket_millis: i64,
    pub value: f64,
}

/// One worker's source or destination series within [`FlowResponse`]. Worker identity permits exact
/// dashboard scoping, while group identity resolves the same collision `Warning::subject_id` and `MapNode::author_id` already carry a
/// group to resolve: without it two groups each running a source called `orders` are one line on the chart
/// and one row in the ranking.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowSeries {
    pub group: String,
    /// The worker whose heartbeat supplied this series. Older retained points predate
    /// worker-scoped keys and therefore have no worker id.
    pub worker_id: Option<String>,
    pub subject_id: String,
    pub points: Vec<FlowPoint>,
}

/// What `GET /api/flow` serves (design doc E2 "Flow over time"): rows and payload bytes, in per source and out per
/// destination, over the caller's window, from whichever tier fits it (`series::Tier::for_window`) -- one tier
/// queried, not several stitched together, so a window that outgrows the finest tier is coarser than the window
/// itself, and its most recent stretch can be absent until that data has aged into the tier being served from.
///
/// `bytes_out` is not `rows_out` weighed. A row counts as out when a destination's queue takes it; its bytes count
/// when the destination acknowledges them, a queue later. The two agree while a destination keeps up and part
/// company while it does not -- which is when somebody is watching. Dividing one by the other is not a row size,
/// and the panel that reads them says which is which.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowResponse {
    pub bucket_millis: i64,
    pub rows_in: Vec<FlowSeries>,
    pub rows_out: Vec<FlowSeries>,
    pub bytes_in: Vec<FlowSeries>,
    pub bytes_out: Vec<FlowSeries>,
}

impl FlowResponse {
    /// Only the series of groups `access` holds viewer in.
    pub fn visible_to(mut self, access: &access::Access) -> Self {
        for series in [
            &mut self.rows_in,
            &mut self.rows_out,
            &mut self.bytes_in,
            &mut self.bytes_out,
        ] {
            series.retain(|s| access.allows(&s.group, access::Level::Viewer));
        }
        self
    }
}

/// The group label `registry::subject_key` stamps on every series subject.
const GROUP_LABEL: &str = "group";
/// `Reader::step`'s count of records read from the source (`runtime.rs`), the "in" of E2.
pub(crate) const ROWS_IN_METRIC: &str = "smeltr_rows_polled_total";
/// `Transform::send`'s count of records the destination accepted (`runtime.rs`), the "out" of E2 -- what actually
/// left, not what was attempted.
const ROWS_OUT_METRIC: &str = "smeltr_rows_sent_total";
/// `egress::run`'s count of rows a destination has acknowledged (`egress.rs`) -- a real delivery, past
/// `Dispatched::InFlight`.
const ROWS_DELIVERED_METRIC: &str = "smeltr_rows_delivered_total";
/// `Transform::poll`'s payload bytes read, beside its row count (`runtime.rs`).
const BYTES_POLLED_METRIC: &str = "smeltr_bytes_polled_total";
/// `egress::note_delivered`'s bytes acknowledged, beside its row count. The weight of what a destination took, so
/// it answers for `ROWS_DELIVERED_METRIC`'s rows rather than `ROWS_OUT_METRIC`'s: nothing weighs a row at the
/// moment it is sent.
const BYTES_DELIVERED_METRIC: &str = "smeltr_bytes_delivered_total";
/// `Transform::place_unmatched`'s count of rows no route on their source matched (`runtime.rs`), counted whether the
/// deployment delivers them or drops them.
const ROWS_UNMATCHED_METRIC: &str = "smeltr_rows_unmatched_total";
/// `Transform::divert_unprocessable`'s count of batches sent to each of the deployment's unprocessable destinations
/// (`runtime.rs`) -- batches, not rows: nothing processed one, so it has no rows to count.
const BATCHES_DLQ_METRIC: &str = "smeltr_batches_dlq_total";

/// [`FlowResponse`] for the last `window`. No durable registry behind this leader (`Registry::series`, e.g. a
/// freshly-`Registry::default`ed one in a test) answers with empty series rather than an error: there is simply no
/// history to serve yet.
pub fn flow_snapshot(state: &AppState, window: Duration) -> Result<FlowResponse, StoreError> {
    let tier = series::Tier::for_window(window);
    let Some(store) = state.registry.series() else {
        return Ok(FlowResponse {
            bucket_millis: tier.bucket_ms(),
            rows_in: Vec::new(),
            rows_out: Vec::new(),
            bytes_in: Vec::new(),
            bytes_out: Vec::new(),
        });
    };
    let since_ms = warnings::now_millis() - window.as_millis() as i64;
    Ok(FlowResponse {
        bucket_millis: tier.bucket_ms(),
        rows_in: flow_series(store, ROWS_IN_METRIC, "source", tier, since_ms)?,
        rows_out: flow_series(store, ROWS_OUT_METRIC, "destination", tier, since_ms)?,
        bytes_in: flow_series(store, BYTES_POLLED_METRIC, "source", tier, since_ms)?,
        bytes_out: flow_series(store, BYTES_DELIVERED_METRIC, "destination", tier, since_ms)?,
    })
}

fn flow_series(
    store: &series::RegistryStore,
    metric: &str,
    label_name: &str,
    tier: series::Tier,
    since_ms: i64,
) -> Result<Vec<FlowSeries>, StoreError> {
    let mut out = Vec::new();
    // Counted over the pass rather than reported per subject: a key this reader cannot parse is a shape it does
    // not know, and every other key of that shape will be unreadable too.
    let (mut unreadable, mut exemplar) = (0usize, None);
    for (subject_key, points) in store.series_since(metric, tier, since_ms)? {
        let (Some(group), Some(subject_id)) = (
            subject_label(&subject_key, GROUP_LABEL),
            subject_label(&subject_key, label_name),
        ) else {
            unreadable += 1;
            exemplar.get_or_insert(subject_key);
            continue;
        };
        out.push(FlowSeries {
            worker_id: subject_label(&subject_key, "worker"),
            group,
            subject_id,
            points: points
                .into_iter()
                .map(|(bucket_ms, value)| FlowPoint {
                    bucket_millis: bucket_ms,
                    value,
                })
                .collect(),
        });
    }
    if unreadable > 0 {
        tracing::warn!(
            target: "leader.series_subjects_unreadable",
            subject = %metric,
            subjects = unreadable,
            exemplar = exemplar.as_deref(),
            "series subjects were dropped from a read because their keys could not be parsed"
        );
    }
    out.sort_by(|a, b| (&a.group, &a.subject_id).cmp(&(&b.group, &b.subject_id)));
    Ok(out)
}

/// One label's value out of a series subject key (`registry::subject_key`'s output, opaque to `series` itself).
fn subject_label(subject_key: &str, name: &str) -> Option<String> {
    let labels: Vec<(String, String)> = serde_json::from_str(subject_key).ok()?;
    labels.into_iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

/// The gate let in anyone viewing somewhere, so the fleet's flow is narrowed to the caller's groups here.
async fn get_flow(
    State(state): State<AppState>,
    axum::Extension(access): axum::Extension<access::Access>,
    Query(query): Query<FlowQuery>,
) -> Result<Json<FlowResponse>, (axum::http::StatusCode, String)> {
    let minutes = query.window_minutes.clamp(1, MAX_WINDOW_MINUTES);
    flow_snapshot(&state, Duration::from_secs((minutes * 60) as u64))
        .map(|flow| Json(flow.visible_to(&access)))
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// -- streams ------------------------------------------------------------------

/// What `GET /api/streams` serves (design doc, Observability: Streams): what the fleet has seen of each source's
/// streams, summed from every heartbeat of every worker -- the fields each Parser produced on each route, and the
/// keys of the rows each source received that no route took, each with when it was first and last seen.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamsResponse {
    pub parsers: Vec<series::StoredParserShape>,
    pub unmatched: Vec<series::StoredUnmatched>,
}

impl StreamsResponse {
    /// Only the groups `access` holds viewer in (design doc, user stories P6).
    pub fn visible_to(mut self, access: &access::Access) -> Self {
        self.parsers
            .retain(|p| access.allows(&p.group_id, access::Level::Viewer));
        self.unmatched
            .retain(|u| access.allows(&u.group_id, access::Level::Viewer));
        self
    }
}

async fn get_streams(
    State(state): State<AppState>,
    axum::Extension(access): axum::Extension<access::Access>,
) -> Result<Json<StreamsResponse>, (axum::http::StatusCode, String)> {
    let registry = state.registry.clone();
    let data_dir = state.data_dir.clone();
    // Off the runtime's threads: a blocking read behind the lock every heartbeat's write takes, and the bundles read
    // from the tree.
    let stored = tokio::task::spawn_blocking(move || {
        let mut stored = match registry.series() {
            Some(store) => store.streams()?,
            None => series::StoredStreams::default(),
        };
        stream_conditions(&data_dir, &mut stored.unmatched);
        let since = warnings::now_millis() - series::DRIFT_WINDOW.as_millis() as i64;
        for key in stored.unmatched.iter_mut().flat_map(|u| &mut u.keys) {
            key.recent = key.last_seen_ms >= since;
        }
        Ok::<_, StoreError>(stored)
    })
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(
        StreamsResponse {
            parsers: stored.parsers,
            unmatched: stored.unmatched,
        }
        .visible_to(&access),
    ))
}

/// Each key's route condition, from its source's kind in the bundle its group is deployed at. A group whose bundle
/// does not load, and a source it no longer declares, get none: what selects their streams is not known here.
fn stream_conditions(data_dir: &std::path::Path, unmatched: &mut [series::StoredUnmatched]) {
    let repo = git::repo_path(data_dir);
    let Some(commit) = git::deployed(&repo) else { return };
    let mut bundles = std::collections::HashMap::new();
    for streams in unmatched {
        let bundle = bundles
            .entry(streams.group_id.clone())
            .or_insert_with(|| tree::load_group_at(&repo, &commit, &streams.group_id).ok());
        let Some(source) = bundle
            .as_ref()
            .and_then(|b| b.topology.sources.iter().find(|s| s.name == streams.source))
        else {
            continue;
        };
        let meta = source.kind.meta();
        for key in &mut streams.keys {
            key.condition = meta.stream_condition(&key.key);
        }
    }
}

// -- logs ---------------------------------------------------------------------

/// The worker id a leader's own logs are kept under, where no worker of its own carries them.
pub const LEADER_LOGS: &str = "leader";

/// How often a leader writes what it has logged into its store.
const LEADER_LOGS_EVERY: std::time::Duration = std::time::Duration::from_secs(1);

/// Write what this leader logs into its own store, where `GET /api/logs` reads the fleet's. Runs for the life of the
/// process; [`crate::logs::mark_leader_thread`] is what queues them.
pub async fn keep_own_logs(state: AppState) {
    loop {
        tokio::time::sleep(LEADER_LOGS_EVERY).await;
        let registry = state.registry.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let events = crate::logs::leader_pending_map(crate::heartbeat::LOGS_PER_HEARTBEAT, |event| {
                crate::heartbeat::LogDto::from(event)
            });
            // Best-effort like a heartbeat's: a store that refuses them says so, and they are not held for a retry
            // that would refuse them again.
            registry.record_logs(LEADER_LOGS, crate::logs::boot_id(), None, &events);
            crate::logs::leader_delivered(events.len());
        })
        .await;
    }
}

/// `GET /api/logs`'s query. Every filter is optional and absent means "any", so the bare path asks for the newest
/// of everything.
#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct LogsQuery {
    /// Comma-separated: `?level=warn,error`. Severities do not nest here, so an operator wanting both names both.
    ///
    /// One key rather than a repeated one because `Query` decodes through `serde_urlencoded`, which has no
    /// notion of a repeated key and refuses a sequence outright -- a refused request rather than a wrong answer.
    level: Option<String>,
    worker: Option<String>,
    group: Option<String>,
    /// Matched anywhere in the dotted call site.
    site: Option<String>,
    /// Matched anywhere in the message.
    q: Option<String>,
    since_millis: Option<i64>,
    until_millis: Option<i64>,
    limit: Option<usize>,
    /// Page toward the oldest: rows below this id. Set from the previous page's `nextBefore`, never
    /// typed by an operator.
    before: Option<i64>,
    /// Page toward the newest: rows above this id. Set from `nextAfter`.
    after: Option<i64>,
}

/// How many rows one read answers with unless the caller asks for fewer.
const LOGS_PAGE: usize = 200;
/// The most one read will answer with. A page is rendered in a browser, and past this the cost lands on whatever
/// has to draw it rather than on the query, which stays flat to the row cap.
const LOGS_PAGE_MAX: usize = 1000;

/// One row of [`LogsResponse`]. `fields` is the delivered list of name/value pairs, passed through as JSON rather
/// than reshaped: what a call site said is what a reader indexes.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRow {
    pub id: i64,
    pub at_millis: i64,
    pub worker_id: String,
    pub group: Option<String>,
    pub thread: Option<String>,
    pub level: String,
    pub site: String,
    pub message: String,
    pub fields: serde_json::Value,
}

/// What `GET /api/logs` serves: what the fleet's workers and this leader have said, newest first.
///
/// A leader writes its own under [`LEADER_LOGS`], whether or not a worker runs in the same process (design doc,
/// Observability).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogsResponse {
    pub rows: Vec<LogRow>,
    /// What the caller asked for, after clamping, so a page that came back short is told apart from one cut off.
    pub limit: usize,
    /// How long a delivered message may be before the heartbeat cuts it (`heartbeat::MAX_TEXT_LEN`). A
    /// search cannot match past this, and a reader should say so rather than quietly find nothing.
    pub message_limit: usize,
    /// The cursor for "older", present only while a row exists below the page. Absent means these are the
    /// last rows the filter has.
    pub next_before: Option<i64>,
    /// The cursor for "newer", present only while a row exists above the page. Absent on the head, which a
    /// page reached with no cursor already is.
    pub next_after: Option<i64>,
}

#[cfg(test)]
mod logs_query_tests {
    use super::*;

    fn decode(query: &str) -> LogsQuery {
        let uri: axum::http::Uri = format!("http://leader/api/logs?{query}").parse().unwrap();
        Query::<LogsQuery>::try_from_uri(&uri)
            .unwrap_or_else(|e| panic!("{query:?} is not a query this endpoint accepts: {e}"))
            .0
    }

    /// Through the extractor a request actually uses, rather than by building the struct: `Query` decodes with
    /// `serde_urlencoded`, which has no notion of a repeated key, and a handler test that skips it would not
    /// notice a shape the wire cannot carry.
    #[test]
    fn several_severities_travel_as_one_comma_separated_key() {
        assert_eq!(decode("level=warn,error").level.as_deref(), Some("warn,error"));
        assert_eq!(decode("level=info&site=worker.").site.as_deref(), Some("worker."));
        assert!(decode("").level.is_none(), "the bare path asks for everything");
    }
}

async fn get_logs(
    State(state): State<AppState>,
    Query(query): Query<LogsQuery>,
) -> Result<Json<LogsResponse>, (axum::http::StatusCode, String)> {
    let limit = query.limit.unwrap_or(LOGS_PAGE).clamp(1, LOGS_PAGE_MAX);
    let levels: Vec<String> = query
        .level
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    let before_id = query.before;
    let after_id = query.after;
    let registry = state.registry.clone();
    // Off the runtime's threads: this is a blocking read behind the same lock every heartbeat's write takes, and
    // at the row cap an unindexed search reads the whole table.
    let mut rows = tokio::task::spawn_blocking(move || {
        let Some(store) = registry.series() else {
            return Ok(Vec::new());
        };
        store.logs(
            &series::LogFilter {
                levels: &levels,
                worker_id: query.worker.as_deref(),
                group_id: query.group.as_deref(),
                site_contains: query.site.as_deref(),
                message_contains: query.q.as_deref(),
                since_ms: query.since_millis,
                until_ms: query.until_millis,
                before_id,
                after_id,
            },
            limit + 1,
        )
    })
    .await
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e: StoreError| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // One row past the page: whether it came back is what tells a page cut short by `limit` from one that
    // ended on the last row, since both are the same length. A cursor offered on length alone steps onto an
    // empty page at either end.
    let more = rows.len() > limit;
    if more {
        // `more` and the extra row both belong to the edge the scan was limited on: the newest for an
        // `after_id` scan (fetched ascending, then reversed), the oldest for every other -- the bare,
        // cursor-less case included.
        if after_id.is_some() {
            rows.remove(0);
        } else {
            rows.pop();
        }
    }

    // `rows` is newest-first however it was fetched (`series::logs`'s doc). The edge a request arrived from
    // needs no test: a cursor came from an adjacent page, so that page still exists.
    let newest = rows.first().map(|r| r.id);
    let oldest = rows.last().map(|r| r.id);
    let (next_before, next_after) = if after_id.is_some() {
        (oldest, more.then_some(newest).flatten())
    } else {
        (
            more.then_some(oldest).flatten(),
            before_id.is_some().then_some(newest).flatten(),
        )
    };

    Ok(Json(LogsResponse {
        rows: rows
            .into_iter()
            .map(|r| LogRow {
                id: r.id,
                at_millis: r.at_ms,
                worker_id: r.worker_id,
                group: r.group_id,
                thread: r.thread_name,
                level: r.level,
                site: r.site,
                message: r.message,
                // Stored as the delivered list; a row whose JSON will not parse carries an empty list rather
                // than failing the page it is one row of.
                fields: serde_json::from_str(&r.fields_json).unwrap_or(serde_json::Value::Array(Vec::new())),
            })
            .collect(),
        limit,
        message_limit: crate::heartbeat::MAX_TEXT_LEN,
        next_before,
        next_after,
    }))
}

// -- worker authentication --------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EnrollRequest {
    pub(crate) code: String,
    pub(crate) worker_id: String,
}

#[derive(serde::Serialize)]
pub(crate) struct EnrollResult {
    pub(crate) ok: bool,
    pub(crate) error: Option<String>,
    /// Whether sending this same enrollment code again could ever succeed. False for an enrollment code this leader
    /// will refuse for as long as it is asked, which the worker cannot tell from the message: only this leader knows
    /// which of its refusals is about the enrollment code and which is about itself.
    pub(crate) retry: bool,
    /// The issued Bearer token, visible only here. The worker persists it.
    pub(crate) token: Option<String>,
}

/// Spends an enrollment code for a credential. The enrollment code's secret is the gate; nothing else authenticates a
/// first-time worker.
///
/// Every refusal says which it was, because a worker holding an enrollment code it cannot spend has nothing else to go
/// on and an operator reading its log needs to know whether to mint another or to look at the leader. That tells an
/// unauthenticated caller whether a secret it already holds is known here, which is worth far less than an operator
/// sent to the right place.
async fn enroll(State(state): State<AppState>, Json(req): Json<EnrollRequest>) -> Json<EnrollResult> {
    // The enrollment code will be refused the same way however long the worker waits. Every one of these names
    // something an operator has to change: an enrollment code to mint, or an id to free.
    let settled = |error: String| {
        Json(EnrollResult {
            ok: false,
            error: Some(error),
            retry: false,
            token: None,
        })
    };
    // This leader could not answer. The enrollment code may be perfectly good, and a worker at a first boot is racing a
    // leader that is still coming up.
    let refuse = |error: String| {
        Json(EnrollResult {
            ok: false,
            error: Some(error),
            retry: true,
            token: None,
        })
    };

    let code = match req.code.parse::<crate::enrollment_code::EnrollmentCode>() {
        Ok(code) => code,
        // A worker parses its own enrollment code before sending it, so this is a caller that is not one.
        Err(e) => return settled(e),
    };
    // The root this enrollment code had to be minted against, for the spend to hold it to. Not compared here: the
    // authority rides the pasted value in the clear beside the secret, and only the secret is hashed, so whoever holds
    // the enrollment code can edit one and not the other. What the leader wrote down is the copy that cannot be
    // altered.
    let presenting = enrollment_codes::Presenting::read(&tls::dir_of(&state.data_dir));
    let may_have_named: Option<Vec<String>> = presenting
        .may_have_named()
        .map(|may| may.into_iter().map(str::to_string).collect());
    let Some(may_have_named) = may_have_named else {
        // Nothing read, so nothing to hold this enrollment code to. Answered as "ask again" rather than "come in":
        // accepting here spends the enrollment code against no check at all, which is what every refusal above exists
        // to prevent.
        return refuse(
            "this leader cannot read which certificate authority it signs under, so it cannot tell whether that \
             enrollment code still names one a worker could verify it by. Try again in a moment."
                .to_string(),
        );
    };
    let code_hash = auth::hash_secret(&code.secret);
    let token = auth::generate_secret();
    let token_hash = auth::hash_secret(&token);
    // Kept for the rollback below, which may only take back the row this call created.
    let issued = token_hash.clone();
    let worker_id = req.worker_id;
    let now_ms = warnings::now_millis();

    // The root the pasted enrollment code names, written the way this leader writes one down. Compared with the row
    // rather than trusted: it rides the value in the clear beside the secret, and only the secret is hashed.
    let presented = match &code.authority {
        crate::enrollment_code::Authority::Pinned(fingerprint) => fingerprint.clone(),
        crate::enrollment_code::Authority::System => String::new(),
    };
    let enrolling = worker_id.clone();
    let spent = run_store(state.store.clone(), move |s| {
        let may: Vec<&str> = may_have_named.iter().map(String::as_str).collect();
        s.spend_enrollment_code(&code_hash, &enrolling, &token_hash, now_ms, &presented, &may)
    })
    .await;

    match spent {
        // Read again, because the answer above was decided before it. The root was taken under the lock a certificate
        // replacement holds, and that lock is gone by the time the store commits on another thread -- so a cutover in
        // between spends this enrollment code against a root it has just retired, and no withdrawal can be in time for
        // a worker already holding the credential. Asked again, because the answer above was decided before the spend.
        // The roots were read under the lock a certificate replacement holds, and that lock is gone by the time the
        // store commits on another thread -- so a cutover in between spends this enrollment code against a root it has
        // just retired, and no withdrawal can be in time for a worker already holding the credential. The enrollment
        // code's own fingerprint, as the leader wrote it down, measured against what it presents now.
        Ok(Spend::Spent { fingerprint, code_id }) => {
            // Whether the root that fingerprint names is one this leader still presents, and whether it could
            // tell. Not the same question: a rotation landing here retires a root outright, while the files
            // going unreadable mid-spend says nothing either way -- and a credential this leader cannot vouch
            // for is taken back either way, since a worker holding one it cannot verify has nothing else to go
            // on.
            let presenting = enrollment_codes::Presenting::read(&tls::dir_of(&state.data_dir));
            let (usable, could_tell) = match presenting.may_have_named() {
                Some(may) => (may.contains(&fingerprint.as_str()), true),
                None => (false, false),
            };
            if usable {
                return Json(EnrollResult {
                    ok: true,
                    error: None,
                    retry: false,
                    token: Some(token),
                });
            }
            if could_tell {
                // Taken back rather than left: a credential nobody can use still counts as a worker enrolled, which
                // holds the next rotation open to its deadline and stands as a warning nobody can clear. The use stays
                // spent -- the enrollment code named a root that is gone, and minting another is the remedy, not asking
                // for this one again. Removed rather than revoked. The token never reached the worker, so there is no
                // identity to keep a record of -- and a revoked row would hold that worker id for good, since an
                // enrollment code cannot be spent by one that has ever enrolled.
                let taken_back = run_store(state.store, move |s| s.remove_worker_credential(&worker_id, &issued)).await;
                if let Err(e) = taken_back {
                    tracing::error!(
                        target: "leader.enrollment_not_taken_back",
                        error = %e,
                        "this leader replaced its authority while a worker was enrolling and could not take back \
                         the credential it had just issued, so a worker that can never reach it reads as enrolled"
                    );
                }
                settled(
                    "this leader replaced the certificate authority it signs under while that enrollment code was \
                     being spent, so a worker taking it could not verify this leader. Mint another on the leader's \
                     Workers page."
                        .to_string(),
                )
            } else {
                // The same fault the pre-spend check above refuses on -- except a credential now exists that nothing
                // has checked, spent from an enrollment code that answer told the worker to send again. Given back in
                // full, not just the credential: a use left spent would make that retry fail on an enrollment code with
                // none left, or collide with the row this spend already wrote on one that has.
                let given_back = run_store(state.store, move |s| {
                    s.unspend_enrollment_code(&code_id, &worker_id, &issued)
                })
                .await;
                if let Err(e) = given_back {
                    tracing::error!(
                        target: "leader.enrollment_not_taken_back",
                        error = %e,
                        "this leader could not read which certificate authority it signs under while a worker was \
                         enrolling and could not give back the enrollment code it had just spent, so a retry the \
                         answer promised will not find one"
                    );
                }
                refuse(
                    "this leader cannot read which certificate authority it signs under, so it cannot tell \
                     whether that enrollment code still names one a worker could verify it by. Try again in a moment."
                        .to_string(),
                )
            }
        }
        Ok(Spend::Unknown) => settled(
            "this leader does not know that enrollment code. It was minted by a different leader, or never minted at \
             all -- mint one on this leader's Workers page."
                .to_string(),
        ),
        Ok(Spend::Expired { .. }) => {
            settled("that enrollment code has expired. Mint another on the leader's Workers page.".to_string())
        }
        Ok(Spend::Withdrawn) => {
            settled("that enrollment code was withdrawn. Mint another on the leader's Workers page.".to_string())
        }
        Ok(Spend::Altered) => settled(
            "that enrollment code names a different certificate authority than the one it was minted against, so it \
             was altered after it was issued. Mint another on the leader's Workers page."
                .to_string(),
        ),
        Ok(Spend::WrongAuthority) => settled(
            "that enrollment code was minted against a certificate authority this leader no longer signs under, so a \
             worker taking it could not verify this leader. Mint another on the leader's Workers page."
                .to_string(),
        ),
        // The count and not the names: this answer goes to whoever holds a spent secret, and the names are the
        // fleet's. An operator wanting to know which workers reads them off the Workers page, behind the gate.
        Ok(Spend::Exhausted { used_by }) => settled(format!(
            "that enrollment code has already brought in every worker it was minted for ({}). Mint another, or mint \
             one for more than one worker.",
            if used_by.len() == 1 {
                "1 worker".to_string()
            } else {
                format!("{} workers", used_by.len())
            }
        )),
        Ok(Spend::AlreadyEnrolled) => settled(
            "a worker with this id has enrolled here before. A worker that lost its credential needs a new worker \
             id: remove its state directory, or move it aside."
                .to_string(),
        ),
        Err(e) => refuse(e.to_string()),
    }
}

/// Asks a heartbeating worker to end its deployment and start it again, on its next heartbeat.
async fn restart_worker(State(state): State<AppState>, Path(worker_id): Path<String>) -> Json<Envelope> {
    ask_worker(&state, &worker_id, crate::heartbeat::DirectiveAction::Restart).await
}

/// Asks a heartbeating worker to end its deployment and exit, as a SIGTERM would, on its next heartbeat.
async fn shutdown_worker(State(state): State<AppState>, Path(worker_id): Path<String>) -> Json<Envelope> {
    ask_worker(&state, &worker_id, crate::heartbeat::DirectiveAction::Shutdown).await
}

async fn ask_worker(state: &AppState, worker_id: &str, action: crate::heartbeat::DirectiveAction) -> Json<Envelope> {
    let id = worker_id.to_string();
    let revoked = run_store(state.store.clone(), move |s| {
        Ok(s.list_worker_credentials()?
            .into_iter()
            .find(|c| c.worker_id == id)
            .map(|c| c.revoked))
    })
    .await;
    // A worker gone without a word would take the request whenever it came back.
    if !state.registry.heartbeating(worker_id, heartbeat_window()) {
        return Json(Envelope::failed_one(
            "this worker is not heartbeating, so it cannot be asked anything",
        ));
    }
    match revoked {
        Ok(Some(false)) => match state.directives.ask(worker_id, action) {
            Ok(()) => Json(Envelope::done()),
            Err(e) => Json(Envelope::failed_one(e)),
        },
        Ok(Some(true)) => Json(Envelope::failed_one(
            "this worker is revoked, so it hears nothing from this leader",
        )),
        Ok(None) => Json(Envelope::failed_one("no such worker has ever enrolled")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Removes a worker that is not heartbeating, or is revoked, for good: revoked, and gone from every list. Its id
/// stays refused, so the enrollment code that brought it in cannot bring it back. A heartbeating worker is refused,
/// since removing it would leave a process nobody can see still moving data. A revoked one is not: this leader
/// already refuses its heartbeats, so the beats still inside the window are from before the revocation, and it can
/// no longer be asked to shut down.
async fn remove_worker(State(state): State<AppState>, Path(worker_id): Path<String>) -> Json<Envelope> {
    let id = worker_id.clone();
    let revoked = run_store(state.store.clone(), move |s| {
        Ok(s.list_worker_credentials()?
            .into_iter()
            .any(|c| c.worker_id == id && c.revoked))
    })
    .await;
    match revoked {
        Ok(true) => {}
        Ok(false) if state.registry.heartbeating(&worker_id, heartbeat_window()) => {
            return Json(Envelope::failed_one("this worker is heartbeating; shut it down first"));
        }
        Ok(false) => {}
        Err(e) => return Json(Envelope::failed_one(e)),
    }
    let id = worker_id.clone();
    let now_ms = warnings::now_millis();
    match run_store(state.store.clone(), move |s| s.remove_worker(&id, now_ms)).await {
        Ok(true) => {
            state.registry.forget_worker(&worker_id);
            state.directives.forget(&worker_id);
            forget::free_holds(&state, &worker_id).await;
            Json(Envelope::done())
        }
        Ok(false) => Json(Envelope::failed_one("no such worker has ever enrolled")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// How recently a worker must have beaten to count as heartbeating: the window past which what it said stops
/// counting as current.
fn heartbeat_window() -> Duration {
    Duration::from_millis(warnings::stale_after_ms(crate::startup_config::worker_heartbeat_interval()).max(0) as u64)
}

/// Soft revocation: blocks the worker's future heartbeats and fetches. The single-worker sources it held go to
/// another worker on that one's next beat, since this one will never report their progress again.
async fn revoke_worker(State(state): State<AppState>, Path(worker_id): Path<String>) -> Json<Envelope> {
    let id = worker_id.clone();
    let result = run_store(state.store.clone(), move |s| s.revoke_worker_credential(&id)).await;
    match result {
        Ok(true) => {
            forget::free_holds(&state, &worker_id).await;
            Json(Envelope::done())
        }
        Ok(false) => Json(Envelope::failed_one("no such worker has ever enrolled")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

// -- the secret catalog -------------------------------------------------

/// A catalog row. Never carries a value, and there is no route that does.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretOut {
    pub name: String,
    pub custody: String,
    pub description: String,
    /// Every version still held, oldest first. Empty for worker-local custody, whose value the leader never had.
    pub versions: Vec<SecretVersionOut>,
    /// The version each deployed group's bundle quotes, as `(group, version)`. Empty means nothing running names
    /// it -- which is what tells an admin a value they just stored has not reached the fleet.
    pub deployed: Vec<(String, i64)>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretVersionOut {
    pub version: i64,
    pub set_at: i64,
    /// Whether the leader can hand this value out. `false` is a credential the fleet cannot obtain.
    pub usable: bool,
}

impl From<store::SecretRecord> for SecretOut {
    fn from(r: store::SecretRecord) -> SecretOut {
        SecretOut {
            custody: r.custody().as_str().to_string(),
            name: r.name,
            description: r.description,
            versions: r
                .versions
                .into_iter()
                .map(|v| SecretVersionOut {
                    version: v.version,
                    set_at: v.set_at,
                    usable: v.usable,
                })
                .collect(),
            deployed: Vec::new(),
        }
    }
}

/// Which versions to stop keeping: everything but the newest `keep` and anything `pinned`, which for a secret is what
/// a deployed bundle names and for a lookup file what a branch names (`lookups::prune`).
///
/// The second clause is what keeps retention from cutting the fleet off. A group still running version 3 must keep
/// version 3 whatever the count says, or the next redeploy of a bundle nobody touched would find a credential the
/// leader cannot produce.
fn versions_to_forget<T: Ord + Clone>(held: &[T], keep: usize, pinned: &[T]) -> Vec<T> {
    let mut newest = held.to_vec();
    newest.sort_unstable();
    let drop_below = newest.len().saturating_sub(keep);
    newest
        .into_iter()
        .take(drop_below)
        .filter(|v| !pinned.contains(v))
        .collect()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSecretRequest {
    pub name: String,
    /// `"worker-local"` or `"leader-stored"`.
    pub custody: String,
    #[serde(default)]
    pub description: String,
    /// Required for leader-stored custody, refused for worker-local.
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretValueRequest {
    pub value: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifySecretRequest {
    /// Which version to ask about. An admin checking whether a credential needs rotating usually means the one the
    /// fleet is running, which is not always the newest.
    pub version: i64,
    pub candidate: String,
}

#[derive(serde::Serialize)]
pub struct VerifySecretResponse {
    pub matches: bool,
}

/// How often one secret may be asked "is this the value", and over how long.
const VERIFY_ATTEMPTS: usize = 5;
const VERIFY_WINDOW: Duration = Duration::from_secs(60);

/// Recent verify attempts per secret.
///
/// Verify is an oracle: it answers a question about a value. Bounding the rate is what keeps it from being a way to
/// guess a value one try at a time. Per secret rather than per caller, so more accounts do not buy more guesses.
#[derive(Default)]
pub struct VerifyLimiter(std::sync::Mutex<std::collections::HashMap<String, Vec<std::time::Instant>>>);

impl VerifyLimiter {
    /// Whether `name` may be asked again, counting the attempt when it may. An attempt refused still counts as
    /// asking, so a caller cannot spend the window and then wait out only the refusals.
    fn allow(&self, name: &str, now: std::time::Instant) -> bool {
        let fresh = |t: &std::time::Instant| now.duration_since(*t) < VERIFY_WINDOW;
        let mut attempts = self.0.lock().unwrap();
        // A name is whatever the URL carried, including secrets that do not exist, so an entry that has aged out
        // goes with its attempts rather than keeping a row for a name nobody will ask about again.
        attempts.retain(|_, recent| recent.iter().any(fresh));
        let recent = attempts.entry(name.to_string()).or_default();
        recent.retain(fresh);
        let allowed = recent.len() < VERIFY_ATTEMPTS;
        recent.push(now);
        allowed
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

async fn list_secrets(State(state): State<AppState>) -> Result<Json<Vec<SecretOut>>, (axum::http::StatusCode, String)> {
    let data_dir = state.data_dir.clone();
    let entitlements = state.entitlements.clone();
    let running = running_deployments(&state).await;
    // What the catalog shows; a bundle it cannot read leaves the Running column empty rather than the page.
    let mut deployed = tokio::task::spawn_blocking(move || deployed_versions(&data_dir, &entitlements, &running))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default();
    let rows = run_store(state.store, |s| s.list_secrets())
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(
        rows.into_iter()
            // Listed on their own page, and changed only through it.
            .filter(|r| !r.certificate)
            .map(|r| {
                let mut out = SecretOut::from(r);
                out.deployed = deployed.remove(&out.name).unwrap_or_default();
                out
            })
            .collect(),
    ))
}

async fn create_secret(State(state): State<AppState>, Json(req): Json<CreateSecretRequest>) -> Json<Envelope> {
    let Some(custody) = store::Custody::parse(&req.custody) else {
        return Json(Envelope::failed_one(format!(
            "custody {:?} is neither \"worker-local\" nor \"leader-stored\"",
            req.custody
        )));
    };
    let result = run_store(state.store, move |s| {
        s.create_secret(&req.name, custody, &req.description, req.value.as_deref())
    })
    .await;
    match result {
        Ok(()) => Json(Envelope::done()),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Where the secrets encryption key stands, as the Secrets page shows it.
#[derive(serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionKeyOut {
    /// `absent`, `held`, `missing` or `mismatched` (`store::KeyState`).
    pub state: String,
    /// The key's id, where there is a key file.
    pub key_id: Option<String>,
    pub path: String,
    /// Each value whose newest copy the key cannot open.
    pub unopenable: Vec<UnopenableOut>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnopenableOut {
    /// `secret` or `setting`.
    pub kind: &'static str,
    /// A secret's name, or a setting's key.
    pub name: String,
    /// What a person calls it: a secret's name, or what the setting is.
    pub label: String,
}

impl From<store::KeyStanding> for EncryptionKeyOut {
    fn from(standing: store::KeyStanding) -> EncryptionKeyOut {
        let (state, key_id) = match standing.key {
            store::KeyState::Absent => ("absent", None),
            store::KeyState::Held { key_id } => ("held", Some(key_id)),
            store::KeyState::Missing => ("missing", None),
            store::KeyState::Mismatched { key_id } => ("mismatched", Some(key_id)),
        };
        EncryptionKeyOut {
            state: state.to_string(),
            key_id,
            path: standing.path.display().to_string(),
            unopenable: standing
                .unopenable
                .into_iter()
                .map(|u| match u {
                    store::Unopenable::Secret(name) => UnopenableOut {
                        kind: "secret",
                        label: name.clone(),
                        name,
                    },
                    store::Unopenable::Setting(name) => UnopenableOut {
                        kind: "setting",
                        label: warnings::sealed_setting_place(&name).0.to_string(),
                        name,
                    },
                })
                .collect(),
        }
    }
}

async fn encryption_key(
    State(state): State<AppState>,
) -> Result<Json<EncryptionKeyOut>, (axum::http::StatusCode, String)> {
    run_store(state.store, |s| s.key_standing())
        .await
        .map(|standing| Json(standing.into()))
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn regenerate_encryption_key(
    State(state): State<AppState>,
    // Inserted by the operator gate: who regenerated it is what the audit row says.
    axum::Extension(actor): axum::Extension<store::Actor>,
) -> Json<Envelope<EncryptionKeyOut>> {
    let event = accounts::event_for(&actor, "key-regenerate", "", "ok");
    Json(
        run_store(state.store, move |s| s.regenerate_key(&event))
            .await
            .map(EncryptionKeyOut::from)
            .into(),
    )
}

/// Stores a new value and forgets what retention no longer keeps.
///
/// Adding and forgetting are two store calls rather than one, because what may be forgotten depends on what the
/// fleet is running -- which the store cannot see and this handler can.
async fn store_secret_value(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<SecretValueRequest>,
) -> Json<Envelope> {
    match is_certificate(&state, &name).await {
        Ok(false) => store_value(state, name, req.value, None).await,
        Ok(true) => Json(Envelope::failed_one(format!(
            "{name:?} is a certificate; replace it on the Certificates page"
        ))),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Whether `name` is a certificate rather than a secret of any other kind.
async fn is_certificate(state: &AppState, name: &str) -> Result<bool, StoreError> {
    let name = name.to_string();
    run_store(state.store.clone(), move |s| {
        Ok(s.list_secrets()?.iter().any(|r| r.name == name && r.certificate))
    })
    .await
}

/// [`store_secret_value`] for a value already checked, with its certificate chain where it is a certificate's key.
async fn store_value(state: AppState, name: String, value: String, chain: Option<String>) -> Json<Envelope> {
    let store = state.store.clone();
    let added = {
        let name = name.clone();
        run_store(store, move |s| match chain {
            Some(chain) => s.add_certificate_version(&name, &chain, &value),
            None => s.add_secret_version(&name, &value),
        })
        .await
    };
    if let Err(e) = added {
        return Json(Envelope::failed_one(e));
    }

    let data_dir = state.data_dir.clone();
    let entitlements = state.entitlements.clone();
    let wanted = name.clone();
    let running = running_deployments(&state).await;
    let pinned = tokio::task::spawn_blocking(move || {
        kept_versions(&data_dir, &entitlements, &running).map(|mut deployed| {
            deployed
                .remove(&wanted)
                .unwrap_or_default()
                .into_iter()
                .map(|(_, version)| version)
                .collect::<Vec<i64>>()
        })
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(|pinned| pinned);
    // Unknown pins forget nothing: the value is stored, and the next store prunes once the bundle reads again.
    let Ok(pinned) = pinned else {
        return Json(Envelope::done());
    };

    let keep = crate::startup_config::leader_secret_versions();
    let forgotten = run_store(state.store, move |s| {
        let held: Vec<i64> = s
            .list_secrets()?
            .into_iter()
            .find(|r| r.name == name)
            .map(|r| r.versions.iter().map(|v| v.version).collect())
            .unwrap_or_default();
        for version in versions_to_forget(&held, keep, &pinned) {
            s.delete_secret_version(&name, version)?;
        }
        Ok(())
    })
    .await;
    match forgotten {
        // The value is stored either way; failing to forget an old one is not a failed rotation.
        Ok(()) => Json(Envelope::done()),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

// -- certificates ------------------------------------------------------------
//
// A certificate is the secret holding its private key, sealed, versioned, retained and delivered as every secret is,
// with its public chain kept beside each version. These routes are where one is added, replaced and read about.

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateOut {
    pub name: String,
    pub description: String,
    /// Every version still held, oldest first.
    pub versions: Vec<SecretVersionOut>,
    /// The newest version's chain, leaf first. Empty where this leader cannot open it.
    pub chain: Vec<tls::Described>,
    /// Whether the leader serves a copy of it.
    pub served: bool,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CertificateRequest {
    /// The new certificate's name. Ignored when replacing one, which the path names.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// PEM: the certificate, then any intermediates, then optionally the root.
    pub certificate: String,
    /// PEM, unencrypted.
    pub private_key: String,
}

impl CertificateRequest {
    /// Whether the certificate reads as one and the key is its key.
    fn check(&self) -> Result<(), String> {
        tls::describe_each(self.certificate.as_bytes(), "the certificate")?;
        tls::is_pair(self.certificate.as_bytes(), self.private_key.as_bytes())
    }
}

/// The newest version of the certificate `name` and its chain, `None` where it is no certificate.
fn newest_chain(store: &dyn Store, name: &str) -> Result<Option<(i64, String)>, StoreError> {
    let newest = (store.list_secrets()?.into_iter())
        .find(|r| r.name == name && r.certificate)
        .and_then(|r| r.current().map(|v| v.version));
    let Some(version) = newest else {
        return Ok(None);
    };
    Ok(store.certificate_chain(name, version)?.map(|chain| (version, chain)))
}

/// The newest chain and private key of the certificate `name`, `None` where it has none this leader can open.
pub fn newest_certificate(store: &dyn Store, name: &str) -> Result<Option<(String, String)>, StoreError> {
    let Some((version, chain)) = newest_chain(store, name)? else {
        return Ok(None);
    };
    let opened = store.secrets_for_delivery(&[(name.to_string(), version)], &[])?;
    Ok(opened.into_iter().next().map(|(_, _, key)| (chain, key)))
}

/// The fingerprint of the newest leaf of the certificate `name`, `None` where it has none this leader can open.
fn newest_fingerprint(store: &dyn Store, name: &str) -> Result<Option<String>, StoreError> {
    Ok(newest_chain(store, name)?
        .and_then(|(_, chain)| tls::describe(chain.as_bytes(), "the certificate").ok())
        .map(|leaf| leaf.fingerprint))
}

/// The name a certificate uploaded before the list existed is listed under.
pub const UPLOADED_BEFORE_THE_LIST: &str = "leader";

/// Lists the certificate an operator uploaded for the leader before certificates were kept in a list, so the
/// Certificates page shows what the leader serves and it can be chosen again. Run at start; does nothing once the
/// list holds it, or where the leader serves its own or an ordered certificate.
pub fn list_uploaded_certificate(store: &dyn Store, tls_dir: &std::path::Path) -> Result<(), String> {
    if acme::source(tls_dir)? != acme::Source::Upload {
        return Ok(());
    }
    let (materials, served) = tls::in_force(tls_dir)?;
    let fingerprint = tls::describe(&served, "the certificate")?.fingerprint;
    for r in store
        .list_secrets()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|r| r.certificate)
    {
        if newest_fingerprint(store, &r.name).map_err(|e| e.to_string())? == Some(fingerprint.clone()) {
            return Ok(());
        }
    }
    let key_path = &materials.server_key_path;
    let key = std::fs::read_to_string(key_path).map_err(|e| format!("{}: {e}", key_path.display()))?;
    let (_, root) = tls::signing_root_pem(&materials.ca_pem, &served)?;
    let chain = format!("{}\n{root}", String::from_utf8_lossy(&served).trim_end());
    store
        .create_certificate(UPLOADED_BEFORE_THE_LIST, "", &chain, &key)
        .map_err(|e| e.to_string())
}

/// The fingerprint of the certificate the leader serves, where an operator chose it from the list rather than the
/// leader's own authority signing it.
fn served_from_list(data_dir: &std::path::Path) -> Option<String> {
    let (materials, served) = tls::in_force(&data_dir.join("tls")).ok()?;
    materials.supplied.then_some(())?;
    tls::describe(&served, "the certificate")
        .ok()
        .map(|served| served.fingerprint)
}

async fn list_certificates(
    State(state): State<AppState>,
) -> Result<Json<Vec<CertificateOut>>, (axum::http::StatusCode, String)> {
    let data_dir = state.data_dir.clone();
    run_store(state.store, move |s| {
        let served = served_from_list(&data_dir);
        let mut out = Vec::new();
        for r in s.list_secrets()?.into_iter().filter(|r| r.certificate) {
            let chain = newest_chain(s, &r.name)?
                .and_then(|(_, chain)| tls::describe_each(chain.as_bytes(), "the certificate").ok())
                .unwrap_or_default();
            let SecretOut {
                name,
                description,
                versions,
                ..
            } = SecretOut::from(r);
            let served = served.is_some() && chain.first().map(|leaf| &leaf.fingerprint) == served.as_ref();
            out.push(CertificateOut {
                name,
                description,
                versions,
                chain,
                served,
            });
        }
        Ok(out)
    })
    .await
    .map(Json)
    .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn create_certificate(State(state): State<AppState>, Json(req): Json<CertificateRequest>) -> Json<Envelope> {
    if let Err(e) = req.check() {
        return Json(Envelope::failed_one(e));
    }
    match run_store(state.store, move |s| {
        s.create_certificate(&req.name, &req.description, &req.certificate, &req.private_key)
    })
    .await
    {
        Ok(()) => Json(Envelope::done()),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Stores a new version of a certificate. What the leader serves is the copy it took when it was chosen, so a new
/// version reaches it when it is chosen again, as one reaches a worker on its next deploy.
async fn replace_certificate(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<CertificateRequest>,
) -> Json<Envelope> {
    if let Err(e) = req.check() {
        return Json(Envelope::failed_one(e));
    }
    match is_certificate(&state, &name).await {
        Ok(true) => store_value(state, name, req.private_key, Some(req.certificate)).await,
        Ok(false) => Json(Envelope::failed_one(format!("no certificate named {name:?}"))),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Deleting the certificate the leader serves is refused, as deleting a secret the fleet runs is: the leader would
/// serve a certificate the list no longer shows.
async fn delete_certificate(State(state): State<AppState>, Path(name): Path<String>) -> Json<Envelope> {
    let data_dir = state.data_dir.clone();
    let wanted = name.clone();
    let served = run_store(state.store.clone(), move |s| {
        let leaf = newest_fingerprint(s, &wanted)?;
        Ok(leaf.is_some() && leaf == served_from_list(&data_dir))
    })
    .await;
    match served {
        Ok(false) => delete_secret(State(state), Path(name)).await,
        Ok(true) => Json(Envelope::failed_one(format!(
            "the leader serves {name:?}; choose another certificate for it first"
        ))),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Deleting a credential the fleet runs, or would run once released, would strand the groups that use it, with no way
/// back: the value is gone. So the one case that cannot be undone is refused, and everything else deletes.
async fn delete_secret(State(state): State<AppState>, Path(name): Path<String>) -> Json<Envelope> {
    let data_dir = state.data_dir.clone();
    let entitlements = state.entitlements.clone();
    let wanted = name.clone();
    let running = running_deployments(&state).await;
    let users = tokio::task::spawn_blocking(move || {
        kept_versions(&data_dir, &entitlements, &running).map(|mut deployed| {
            let mut groups: Vec<String> = deployed
                .remove(&wanted)
                .unwrap_or_default()
                .into_iter()
                .map(|(group, _)| group)
                .collect();
            groups.dedup();
            groups
        })
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(|users| users);
    let users = match users {
        Ok(users) => users,
        Err(e) => return Json(Envelope::failed_one(e)),
    };
    if !users.is_empty() {
        return Json(Envelope::failed_one(format!(
            "{name:?} is named by the deployed bundle or by main's head; take the reference out first, or the \
             workers that run it lose a credential they cannot get back"
        )));
    }
    match run_store(state.store, move |s| s.delete_secret(&name)).await {
        Ok(true) => Json(Envelope::done()),
        Ok(false) => Json(Envelope::failed_one("no such secret")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Whether a candidate is the stored value -- what tells an admin a credential still needs rotating without showing
/// it to them. Every attempt is logged whatever the answer, and the rate is bounded (see [`VerifyLimiter`]).
async fn verify_secret(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<VerifySecretRequest>,
) -> Result<Json<VerifySecretResponse>, (axum::http::StatusCode, String)> {
    if !state.verify_limiter.allow(&name, std::time::Instant::now()) {
        tracing::warn!(
            target: "leader.secret_verify_refused",
            subject = %name,
            "refused a secret verification: too many attempts"
        );
        return Err((
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            format!(
                "{name:?} has been asked about {VERIFY_ATTEMPTS} times in the last {} seconds",
                VERIFY_WINDOW.as_secs()
            ),
        ));
    }
    let asked = name.clone();
    let version = req.version;
    let matches = run_store(state.store, move |s| s.verify_secret(&name, version, &req.candidate))
        .await
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Two sites, so an outcome is never folded into its opposite or lost among the other's count.
    if matches {
        tracing::info!(target: "leader.secret_verified", subject = %asked, version, "a secret verification matched");
    } else {
        tracing::info!(
            target: "leader.secret_verify_failed",
            subject = %asked,
            version,
            "a secret verification did not match"
        );
    }
    Ok(Json(VerifySecretResponse { matches }))
}

/// What one call to [`point_secret_references`] rewrote.
pub struct Repointed {
    /// `None` when nothing named the secret at another version, so nothing was written.
    pub commit: Option<String>,
    /// Paths rewritten, in the order they were walked.
    pub files: Vec<String>,
    /// Groups touched -- `shared` among them when the shared bundle names it.
    pub groups: Vec<String>,
    pub references: usize,
}

/// Points every reference to `name` at `version`, across every group's bundle and `shared/`, as one commit on
/// `branch`. When nothing names it at another version, nothing is written, on any branch.
///
/// The leader never writes `main`: what this produces is work in Changes, reviewed and deployed like anything
/// else. Asked on `main`, it answers whether a branch would be needed: refused when there is something to move. Editing is a DTO round-trip, so everything the loader does not know about a file fails the parse rather
/// than being dropped on the way back out.
///
/// Blocking; the caller is inside `spawn_blocking`.
fn point_secret_references(
    data_dir: &std::path::Path,
    branch: &str,
    name: &str,
    version: i64,
    actor: &store::Actor,
    store: &dyn Store,
) -> Result<Repointed, String> {
    let repo = git::repo_path(data_dir);
    // The branch tip when there is one, so a bump lands on top of edits already there rather than reverting them.
    let expected = git::branch_tip(&repo, branch);
    let parent = expected
        .clone()
        .or_else(|| git::main_head(&repo))
        .ok_or_else(|| format!("{} has no commit to build on", git::MAIN_BRANCH))?;

    let mut bundles: Vec<String> = tree::groups_at(&repo, &parent)
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|g| format!("{}/{g}", crate::config_file::GROUPS_DIR))
        .collect();
    bundles.sort();
    bundles.insert(0, crate::config_file::SHARED_DIR.to_string());

    let mut edits = Vec::new();
    let mut files = Vec::new();
    let mut groups = Vec::new();
    let mut references = 0;
    for prefix in bundles {
        let mut moved_here = 0;
        for (file, moved) in [
            repoint_array::<crate::config_file::SourceDto>(
                &repo,
                &parent,
                &prefix,
                "sources.json",
                name,
                version,
                store,
            )?,
            repoint_array::<crate::config_file::DestinationDto>(
                &repo,
                &parent,
                &prefix,
                "destinations.json",
                name,
                version,
                store,
            )?,
            repoint_array::<crate::config_file::IdentityDto>(
                &repo,
                &parent,
                &prefix,
                "identities.json",
                name,
                version,
                store,
            )?,
        ]
        .into_iter()
        .flatten()
        {
            moved_here += moved;
            files.push(file.path.clone());
            edits.push(file);
        }
        if moved_here > 0 {
            references += moved_here;
            groups.push(prefix.rsplit('/').next().unwrap_or(&prefix).to_string());
        }
    }

    if edits.is_empty() {
        return Ok(Repointed {
            commit: None,
            files,
            groups,
            references,
        });
    }
    if branch == git::MAIN_BRANCH {
        return Err(format!(
            "{} is what deploys move and is not edited directly -- name a branch and deploy it",
            git::MAIN_BRANCH
        ));
    }

    let message = format!("Point {name} at version {version}");
    match git::commit_edits(&repo, branch, &parent, expected.as_deref(), &edits, &message, actor)
        .map_err(|e| e.to_string())?
    {
        git::Committed::At(commit) => Ok(Repointed {
            commit: Some(commit),
            files,
            groups,
            references,
        }),
        // The read said something to change, so an unchanged tree means the edits raced a save that made them.
        git::Committed::Unchanged => Err(format!("{name:?} already names version {version} on {branch:?}")),
        git::Committed::Moved(at) => Err(moved_since_read(&parent, at.as_deref())),
    }
}

/// One bundle file rewritten, or `None` when it holds no reference that moved. The file is re-serialized from the
/// DTOs it was parsed into, so nothing else in it moves.
fn repoint_array<T>(
    repo: &std::path::Path,
    commit: &str,
    prefix: &str,
    file: &str,
    name: &str,
    version: i64,
    store: &dyn Store,
) -> Result<Option<(git::FileEdit, usize)>, String>
where
    T: serde::de::DeserializeOwned + serde::Serialize + PointsAtSecret,
{
    let path = format!("{prefix}/{file}");
    let Some(bytes) = git::read_blobs(repo, commit, std::slice::from_ref(&path))
        .map_err(|e| e.to_string())?
        .pop()
        .flatten()
    else {
        return Ok(None);
    };
    let mut items: Vec<T> =
        serde_json::from_slice(&bytes).map_err(|e| format!("{path} does not load, so it cannot be edited: {e}"))?;
    let moved: usize = items.iter_mut().map(|i| i.point_secret_at(name, version)).sum();
    if moved == 0 {
        return Ok(None);
    }
    let refusals: Vec<String> = items
        .iter()
        .flat_map(|i| i.refuses_version(store, name, version))
        .collect();
    if !refusals.is_empty() {
        return Err(format!("{path}: {}", refusals.join("; ")));
    }
    let content = serde_json::to_vec_pretty(&items).map_err(|e| format!("{path}: {e}"))?;
    Ok(Some((
        git::FileEdit {
            path,
            content: Some(content),
        },
        moved,
    )))
}

/// What every bundle array has in common: somewhere inside, a credential names a secret at a version.
trait PointsAtSecret {
    fn secrets_mut(&mut self) -> Vec<&mut crate::config_file::SecretRefDto>;

    /// How many of this item's credentials moved onto `version`.
    fn point_secret_at(&mut self, name: &str, version: i64) -> usize {
        self.secrets_mut()
            .into_iter()
            .map(|r| usize::from(r.point_at(name, version)))
            .sum()
    }

    /// Why this item cannot run at `version`, once the leader has looked at the value there. Empty for an item
    /// whose meaning does not depend on what the value is, which is every one but an identity: one password is as
    /// good as another at any version, and a private key is only ever its own certificate's.
    fn refuses_version(&self, _store: &dyn Store, _name: &str, _version: i64) -> Vec<String> {
        Vec::new()
    }
}

impl PointsAtSecret for crate::config_file::SourceDto {
    fn secrets_mut(&mut self) -> Vec<&mut crate::config_file::SecretRefDto> {
        crate::config_file::SourceDto::secrets_mut(self)
    }
}

impl PointsAtSecret for crate::config_file::DestinationDto {
    fn secrets_mut(&mut self) -> Vec<&mut crate::config_file::SecretRefDto> {
        crate::config_file::DestinationDto::secrets_mut(self)
    }
}

impl PointsAtSecret for crate::config_file::IdentityDto {
    fn secrets_mut(&mut self) -> Vec<&mut crate::config_file::SecretRefDto> {
        crate::config_file::IdentityDto::secrets_mut(self)
    }

    fn refuses_version(&self, store: &dyn Store, name: &str, version: i64) -> Vec<String> {
        let Some(certificate) = &self.certificate else {
            return Vec::new();
        };
        // A worker-local key is on a host this leader never sees, so there is nothing here to check.
        if !matches!(&self.key, Some(crate::config_file::SecretRefDto::LeaderStored(k)) if k.secret == name) {
            return Vec::new();
        }
        match store.certificate_matches(name, version, certificate) {
            Ok(true) => Vec::new(),
            Ok(false) => vec![format!(
                "version {version} is not the private key the identity {:?} presents a certificate for. Rotating \
                 a client key is one change with the certificate it belongs to, not a version on its own",
                self.id
            )],
            Err(e) => vec![format!("the identity {:?} could not be checked: {e}", self.id)],
        }
    }
}

fn moved_since_read(read: &str, at: Option<&str>) -> String {
    match at {
        Some(now) => format!("the branch moved since this was read ({read} -> {now}); nothing was written"),
        None => format!("the branch went away since this was read ({read}); nothing was written"),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PointSecretRequest {
    /// Which version every reference should name. Any the leader holds -- naming an older one is how a credential
    /// is rolled back, as a reviewed change rather than as a side effect of reverting a commit.
    pub version: i64,
    /// Where the change is drafted. Created off `main` when it does not exist; `main` itself is refused.
    pub branch: String,
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PointedSecret {
    pub commit: Option<String>,
    /// What was rewritten, so the page can say the blast radius without opening the diff.
    pub files: Vec<String>,
    pub groups: Vec<String>,
    pub references: usize,
    /// Whether this call opened the change, or joined one already open on that branch.
    pub opened_change: bool,
}

/// Drafts the change that points a secret's references at one version.
///
/// Two steps in one call, because they are useless apart: a commit nobody opened a change over is work sitting on
/// a branch, and `git::open_change` refuses a branch that changes nothing. A branch that already carries an open
/// change keeps its own title -- this joins it rather than renaming somebody's work.
async fn point_secret(
    State(state): State<AppState>,
    // Inserted by the operator gate. A person is pointing a bundle at a different credential version, and the
    // branch and the change this opens are theirs.
    axum::Extension(actor): axum::Extension<store::Actor>,
    Path(name): Path<String>,
    Json(req): Json<PointSecretRequest>,
) -> Json<Envelope<PointedSecret>> {
    let held = {
        let wanted = name.clone();
        let version = req.version;
        run_store(state.store.clone(), move |s| {
            Ok(s.list_secrets()?
                .into_iter()
                .any(|r| r.name == wanted && r.versions.iter().any(|v| v.version == version)))
        })
        .await
    };
    // Never write a reference to a value the leader cannot produce: that is a bundle the fleet would refuse.
    match held {
        Ok(true) => {}
        Ok(false) => return Json(Envelope::failed_one(format!("{name:?} has no version {}", req.version))),
        Err(e) => return Json(Envelope::failed_one(e)),
    }

    let data_dir = state.data_dir.clone();
    // The store comes along: moving an identity onto a key that is not its certificate's is the one repoint the
    // value itself refuses, and only the leader can see both halves.
    let pointing = state.store.clone();
    let drafted = blocking(move || {
        let done = point_secret_references(&data_dir, &req.branch, &name, req.version, &actor, pointing.as_ref())?;
        // Nothing moved, so there is no change to open.
        if done.commit.is_none() {
            return Ok((done, false));
        }
        let repo = git::repo_path(&data_dir);
        let already_open = git::read_change(&repo, &req.branch)
            .map_err(|e| e.to_string())?
            .is_some_and(|c| c.state == git::ChangeState::Open);
        if !already_open {
            let title = format!("Point {name} at version {}", req.version);
            let description = format!(
                "{} reference{} across {}.",
                done.references,
                if done.references == 1 { "" } else { "s" },
                done.groups.join(", ")
            );
            git::open_change(&repo, &req.branch, &title, &description, &actor).map_err(|e| e.to_string())?;
        }
        Ok::<_, String>((done, !already_open))
    })
    .await;

    match drafted {
        Ok((done, opened_change)) => Json(Envelope::ok(PointedSecret {
            commit: done.commit,
            files: done.files,
            groups: done.groups,
            references: done.references,
            opened_change,
        })),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// Forgets one version on purpose -- what answers a leak, rather than waiting for retention to reach it.
///
/// Refused while the deployed bundle or `main`'s head quotes that version, for the same reason deleting a whole
/// secret is: the workers that run it would be left with a credential they cannot get back.
async fn forget_secret_version(
    State(state): State<AppState>,
    Path((name, version)): Path<(String, i64)>,
) -> Json<Envelope> {
    let data_dir = state.data_dir.clone();
    let entitlements = state.entitlements.clone();
    let wanted = name.clone();
    let running = running_deployments(&state).await;
    let users = tokio::task::spawn_blocking(move || {
        kept_versions(&data_dir, &entitlements, &running).map(|mut deployed| {
            deployed
                .remove(&wanted)
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, v)| *v == version)
                .map(|(group, _)| group)
                .collect::<Vec<String>>()
        })
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(|users| users);
    let users = match users {
        Ok(users) => users,
        Err(e) => return Json(Envelope::failed_one(e)),
    };
    if !users.is_empty() {
        return Json(Envelope::failed_one(format!(
            "{name:?} v{version} is named by the deployed bundle or by main's head; point it at another version first"
        )));
    }
    match run_store(state.store, move |s| s.delete_secret_version(&name, version)).await {
        Ok(true) => Json(Envelope::done()),
        Ok(false) => Json(Envelope::failed_one("no such version")),
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// A roster row. Never carries the token hash.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerOut {
    pub worker_id: String,
    pub revoked: bool,
    /// `permanent` or `ephemeral`: what its being gone means, from the enrollment code that brought it in.
    pub kind: String,
    /// What it can be asked now: empty unless it is heartbeating to this leader.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepts: Vec<crate::heartbeat::DirectiveAction>,
    /// What it has been asked and has not yet taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asked: Option<crate::heartbeat::DirectiveAction>,
}

impl From<store::WorkerCredentialRecord> for WorkerOut {
    fn from(c: store::WorkerCredentialRecord) -> WorkerOut {
        WorkerOut {
            worker_id: c.worker_id,
            revoked: c.revoked,
            kind: c.kind.as_str().to_string(),
            accepts: Vec::new(),
            asked: None,
        }
    }
}

/// What `GET /api/workers` serves: every worker that has ever enrolled, heartbeating or not, less the ephemeral
/// workers gone long enough to be forgotten. A store error is returned, not an empty list.
pub async fn worker_credentials_snapshot(state: &AppState) -> Result<Vec<WorkerOut>, StoreError> {
    // The look for gone workers rides every read of the fleet: one scaled to nothing sends no heartbeat to carry
    // it, and the roster may be the only page anybody opens.
    forget::sweep(state, warnings::now_millis()).await;
    let creds = run_store(state.store.clone(), |s| s.list_worker_credentials()).await?;
    Ok(creds
        .into_iter()
        .map(|c| {
            let id = c.worker_id.clone();
            let heartbeating = state.registry.heartbeating(&id, heartbeat_window());
            WorkerOut {
                accepts: if heartbeating {
                    state.directives.accepts(&id)
                } else {
                    Vec::new()
                },
                asked: state.directives.pending(&id),
                ..WorkerOut::from(c)
            }
        })
        .collect())
}

async fn list_workers(State(state): State<AppState>) -> Result<Json<Vec<WorkerOut>>, (axum::http::StatusCode, String)> {
    worker_credentials_snapshot(&state)
        .await
        .map(Json)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// The worker a presented token was issued to, put on the request by [`require_worker_auth`].
///
/// Enrollment binds one token to one `worker_id`, so the token *is* the identity. A handler that records or serves
/// per-worker state reads it here rather than from the request body, which any holder of any valid token can write.
#[derive(Clone)]
struct AuthenticatedWorker(String);

/// `401` unless the request carries a valid, unrevoked worker token; the credential's `worker_id` rides on as
/// [`AuthenticatedWorker`].
async fn require_worker_auth(
    State(state): State<AppState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let token = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    let Some(token) = token else {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    };

    let token_hash = auth::hash_secret(&token);
    // Three answers, because a worker acts on each differently. Unknown -- never issued, or its worker forgotten -- is
    // 401, and a worker holding an enrollment code enrolls again. Revoked is 403, and it stays out. A store that could
    // not say is neither, and must not read as either.
    let credential = match run_store(state.store.clone(), move |s| {
        s.get_worker_credential_by_hash(&token_hash)
    })
    .await
    {
        Ok(credential) => credential,
        Err(_) => return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE),
    };
    let Some(credential) = credential else {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    };
    if credential.revoked {
        return Err(axum::http::StatusCode::FORBIDDEN);
    }
    // The first request a credential authenticates closes the window in which a retried enrollment may replace
    // it. Refused rather than let through when that cannot be written down: a credential in use that a retry can
    // still replace is one a replayed enrollment code could take over.
    if !credential.used {
        let token_hash = credential.token_hash.clone();
        if run_store(state.store.clone(), move |s| s.mark_credential_used(&token_hash))
            .await
            .is_err()
        {
            return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
        }
    }
    req.extensions_mut().insert(AuthenticatedWorker(credential.worker_id));
    req.extensions_mut().insert(credential.kind);
    Ok(next.run(req).await)
}

// -- wiring -----------------------------------------------------------------

/// Heartbeat, git fetch, lookup files and ledgers, gated by [`require_worker_auth`].
fn worker_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route(
            "/api/heartbeat",
            // Stated rather than inherited from axum's default; [`crate::heartbeat::HEARTBEAT_BODY_LIMIT`] says
            // what it is measured against.
            post(heartbeat).layer(axum::extract::DefaultBodyLimit::max(
                crate::heartbeat::HEARTBEAT_BODY_LIMIT,
            )),
        )
        .route(
            "/api/capture-report",
            post(capture_report).layer(axum::extract::DefaultBodyLimit::max(capture::REPORT_BODY_LIMIT)),
        )
        .route("/repo/*path", get(git_http).post(git_http))
        .route("/api/lookup-store/:oid", get(lookup_file))
        .route("/api/ledger", get(ledger))
        .route_layer(axum::middleware::from_fn_with_state(state, require_worker_auth))
}

/// What a worker can reach before it holds anything: the authority it will pin to, and the exchange that buys it
/// a credential. Neither carries a worker token nor an operator credential.
///
/// The authority is given to anybody who asks. A certificate authority is public by construction -- it is what
/// every client is meant to hold -- and it authorizes nothing on its own. What makes the copy a worker fetches
/// worth having is the fingerprint in its enrollment code, checked against these bytes, not the secrecy of this route.
fn enroll_router() -> Router<AppState> {
    Router::new()
        .route("/api/enroll", post(enroll))
        .route("/api/authority", get(authority))
}

/// The trust store to hand this worker, when what it reports holding is not what this leader publishes.
///
/// Whole, not the roots it lacks: a worker takes this as everything it will verify by. Sending only what is
/// missing would drop the root it is verifying this very connection with -- and a root leaves a worker's trust
/// store only by being absent from a set handed whole, which is what carries a retirement out to the fleet.
///
/// Answered off the published bundle rather than off an open rotation, because a retirement outlives the
/// rotation that ordered it: the intent is gone by the time the shortened bundle needs to reach anybody.
///
/// `takes_authority` is false for a worker started with `SMELTR_LEADER_CA`. Its trust is its operator's, it
/// reports what they gave it and refuses what this leader offers, so it would differ here on every beat forever.
///
/// The bundle is the authority of whatever set is in force, an operator's uploaded one included: a switch from
/// one source to another is a rotation like any other, and the worker that follows it is the one handed the
/// incoming root here.
fn authority_to_hand(data_dir: &std::path::Path, held: &[String], takes_authority: bool) -> Option<String> {
    if !takes_authority {
        return None;
    }
    let dir = tls::dir_of(data_dir);
    // Agreement and the bundle as one answer. Asked separately, a beat can be told the socket agrees and then be
    // handed a set an upload swapped in underneath -- a worker pinning what nothing presents.
    let materials = tls::current_if_agreed(&dir).ok()?;
    let published: std::collections::HashSet<String> = tls::describe_each(&materials.ca_pem, tls::CA_CERT)
        .ok()?
        .into_iter()
        .map(|root| root.fingerprint)
        .collect();
    if published == held.iter().cloned().collect() {
        return None;
    }
    String::from_utf8(materials.ca_pem).ok()
}

/// Every worker an open rotation waits for, or `None` where the roster could not be read.
async fn expected_workers(state: &AppState) -> Result<std::collections::HashSet<String>, String> {
    // Every worker enrolled, which is the roster rather than whoever has spoken: one that enrolled and has never
    // reported a thread, or has not beaten since this leader started, is exactly the worker a rotation must not
    // sign away from. Read here rather than kept, because an enrollment or a revocation during a rotation counts.
    //
    // A revoked worker is refused at the gate and can never report anything, so waiting on one is waiting on
    // nobody.
    // A roster this leader could not read is not a fleet with nobody in it. Taken as one, every remaining entry
    // reads as ready and the rotation signs away from every worker enrolled. So readiness goes unanswered -- but
    // only readiness: a store that stays unreadable would otherwise hold the rotation open past the deadline an
    // operator set for the case where waiting is not working, while the outgoing authority runs down.
    let credentials = match run_store(state.store.clone(), |s| s.list_worker_credentials()).await {
        Ok(credentials) => Ok(credentials),
        Err(e) => {
            tracing::warn!(
                target: "leader.rotation_roster_unreadable",
                error = %e,
                "could not read which workers are enrolled, so this leader cannot tell whether its fleet holds the \
                 authority it is rotating to. The deadline still decides"
            );
            Err(format!("could not read which workers are enrolled: {e}"))
        }
    };
    credentials.map(|credentials| {
        let gone: std::collections::HashSet<&str> = credentials
            .iter()
            .filter(|c| c.revoked)
            .map(|c| c.worker_id.as_str())
            .collect();
        let mut expected: std::collections::HashSet<String> = credentials
            .iter()
            .filter(|c| !c.revoked)
            .map(|c| c.worker_id.clone())
            .collect();
        // Beside the roster: a worker heartbeating is evidence no roster read can contradict.
        expected.extend(
            state
                .registry
                .known_workers()
                .into_iter()
                .filter(|worker| !gone.contains(worker.as_str())),
        );
        expected
    })
}

/// Each worker an open rotation waits for, by id, and whether it holds `incoming` yet. An error where the roster
/// could not be read, which is not a fleet with nobody in it.
pub async fn rotation_workers(state: &AppState, incoming: &str) -> Result<Vec<(String, bool)>, String> {
    let expected = expected_workers(state).await?;
    Ok(state
        .registry
        .holders(&expected)
        .into_iter()
        .map(|(worker, held)| (worker, held.iter().any(|root| root == incoming)))
        .collect())
}

/// Signs under the incoming root once the fleet holds it, or once the operator's deadline says to stop waiting.
///
/// Called wherever the leader has a reason to look: the heartbeat that carries the evidence, and the page an
/// operator watches it on -- a leader with no workers left has no beats and would otherwise wait out a deadline
/// nobody is coming to satisfy.
pub async fn cut_over_if_due(state: &AppState, now_ms: i64) {
    let dir = tls::dir_of(&state.data_dir);
    // Asked first, and cheaply: no rotation is the ordinary state, and everything below -- a roster read included
    // -- is worth doing only while one is open.
    let Ok(Some(rotation)) = tls::rotation(&dir) else {
        return;
    };
    let holders = expected_workers(state)
        .await
        .ok()
        .map(|expected| state.registry.authorities_held(&expected));
    let cut_over = match tls::cut_over_if_due(
        &dir,
        crate::startup_config::leader_certificate_names(&state.origin_hosts),
        crate::startup_config::leader_ca_validity(),
        holders.as_deref(),
        now_ms,
    ) {
        Ok(cut_over) => cut_over,
        Err(e) => {
            tracing::warn!(
                target: "leader.authority_not_cut_over",
                error = %e,
                "this leader could not sign under the authority it is rotating to, and keeps serving the one it \
                 was signing under"
            );
            return;
        }
    };
    if cut_over {
        tls::reload(&dir);
        // Nobody asked for this one: it is the deadline arriving, or the fleet ready. The operator's own button
        // records itself, before it acts; this one is recorded after, because it is known only once it happened.
        let event = accounts::event_for(
            &store::Actor::system(),
            "authority-cut-over",
            "leader",
            &rotation.incoming,
        );
        if let Err(e) = run_store(state.store.clone(), move |s| s.record_security_event(&event)).await {
            tracing::warn!(
                target: "leader.cut_over_unrecorded",
                error = %e,
                "this leader cut over to the authority it was rotating to, and the audit row for it did not land"
            );
        }
        enrollment_codes::withdraw_off_authority(state, &store::Actor::system()).await;
    }
}

/// `GET /api/authority`: `ca.pem`, for a worker to pin against and an operator to hand around.
async fn authority(State(state): State<AppState>) -> Response {
    // This file's whole purpose is to be pinned. While the socket is still presenting the certificate before the
    // last one, the disk describes a leader no handshake reaches: a worker that fetched this would pin a root its
    // very next request refuses, and treat that as permanent.
    // Asked as one answer: a check that the socket agrees, followed by a separate read, can be told yes and then
    // hand back a set an upload swapped in underneath -- which is the pinning failure above by another route.
    let materials = match tls::current_if_agreed(&tls::dir_of(&state.data_dir)) {
        Ok(materials) => materials,
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CACHE_CONTROL, "no-store")],
                e.message().to_string(),
            )
                .into_response();
        }
    };
    (
        [
            (header::CONTENT_TYPE, "application/x-pem-file"),
            (header::CACHE_CONTROL, "no-store"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"smeltr-leader-ca.pem\"",
            ),
        ],
        materials.ca_pem,
    )
        .into_response()
}

/// Operator routes: groups, enrollment codes, revocation, health and the roster. Unauthenticated on its own;
/// [`compose`] takes this back with whatever gate the caller wrapped around it.
pub fn admin_router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/system/info", get(system_info))
        .route("/api/warnings", get(list_warnings))
        .route("/api/flow", get(get_flow))
        .route("/api/streams", get(get_streams))
        .route("/api/logs", get(get_logs))
        .route(
            "/api/enrollment-codes",
            get(enrollment_codes::list).post(enrollment_codes::mint),
        )
        .route(
            "/api/enrollment-codes/:id",
            axum::routing::delete(enrollment_codes::withdraw),
        )
        .route("/api/workers", get(list_workers))
        .route("/api/workers/:worker_id/revoke", post(revoke_worker))
        .route("/api/workers/:worker_id/restart", post(restart_worker))
        .route("/api/workers/:worker_id/shutdown", post(shutdown_worker))
        .route("/api/workers/:worker_id/remove", post(remove_worker))
        .route(
            "/api/settings/workers",
            get(forget::get_settings).put(forget::put_settings),
        )
        .route(
            "/api/settings/warnings",
            get(throughput::get_settings).put(throughput::put_settings),
        )
        .route(
            "/api/settings/updates",
            get(updates::get_settings).put(updates::put_settings),
        )
        .route(
            "/api/settings/git-remote",
            get(remote::get_settings).put(remote::put_settings),
        )
        .route("/api/settings/git-remote/sync", post(remote::post_sync))
        .route("/api/branches/:name/overwrite", post(remote::overwrite))
        .route("/api/secrets", get(list_secrets).post(create_secret))
        .route("/api/secrets/:name", put(store_secret_value).delete(delete_secret))
        .route("/api/certificates", get(list_certificates).post(create_certificate))
        .route(
            "/api/certificates/:name",
            put(replace_certificate).delete(delete_certificate),
        )
        .route(
            "/api/secrets/:name/versions/:version",
            axum::routing::delete(forget_secret_version),
        )
        .route("/api/secrets/:name/verify", post(verify_secret))
        .route("/api/secrets/:name/references", put(point_secret))
        .route("/api/encryption-key", get(encryption_key))
        .route("/api/encryption-key/regenerate", post(regenerate_encryption_key))
}

/// Every leader route. `admin` is [`admin_router`], wrapped in the caller's operator gate and any routes that share it.
/// Worker and enrollment routes never carry that gate: a worker has no operator secret.
pub fn compose(state: AppState, admin: Router<AppState>) -> Router {
    worker_router(state.clone())
        .merge(enroll_router())
        .merge(admin)
        .with_state(state)
}

/// [`compose`] with no operator gate, for the integration tests. Every request is an administrator's, since the
/// handlers that answer per area read the caller's access the gate would otherwise have set.
pub fn router(state: AppState) -> Router {
    compose(
        state.clone(),
        admin_router().layer(axum::Extension(access::Access::Administrator)),
    )
}

/// Where a store would write its secrets encryption key. No fixture below stores a secret, so nothing is written --
/// but two must not name one file between them if one ever does.
#[cfg(test)]
mod identity_expiry_tests {
    use super::*;
    use crate::config_file::test_fixtures::example_json;

    /// A self-signed certificate good for `days`, in PEM.
    fn certificate(days: i64) -> String {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.not_before = time::OffsetDateTime::now_utc() - std::time::Duration::from_secs(60);
        params.not_after = time::OffsetDateTime::now_utc() + std::time::Duration::from_secs(days as u64 * 24 * 60 * 60);
        params.self_signed(&key).unwrap().pem()
    }

    /// A bundle whose Kafka source names an identity trusting `authorities`.
    fn deployed(authorities: &str) -> crate::plan::topology::TopologyConfig {
        let identity = format!(
            r#"{{"id": "acme", "authorities": {}}}"#,
            serde_json::to_string(authorities).unwrap()
        );
        let json = example_json()
            .replacen(
                r#""groupId": "smeltr""#,
                r#""groupId": "smeltr", "properties": {"security.protocol": "SSL"}, "identity": "acme""#,
                1,
            )
            .replacen(
                r#""version": 1,"#,
                &format!(r#""version": 1, "identities": [{identity}],"#),
                1,
            );
        crate::config_file::load_str(&json)
            .expect("a bundle naming an identity")
            .topology
    }

    /// The window comes off the certificate itself, so a warning about it is about a date rather than about when
    /// anybody looked.
    #[test]
    fn a_deployed_identitys_certificates_are_read_off_the_bundle() {
        let found = certificates_of(&deployed(&certificate(90)), "g", "c0");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].group, "g");
        assert_eq!(found[0].identity, "acme");
        assert_eq!(found[0].half, warnings::Half::Trusted);
        let life = found[0].not_after_millis - found[0].not_before_millis;
        assert!(
            (89..=91).contains(&(life / (24 * 60 * 60 * 1000))),
            "read a life of {life}ms off a 90-day certificate"
        );
    }

    /// A trust store holds more than one root, and each is its own answer about when trust runs out.
    #[test]
    fn every_authority_in_a_store_is_read_out_of_it() {
        let store = format!("{}{}", certificate(30), certificate(3650));
        let found = certificates_of(&deployed(&store), "g", "c0");
        assert_eq!(found.len(), 2);
        assert!(found.iter().all(|c| c.half == warnings::Half::Trusted));
    }

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-leader-deployed-certs-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// A group's own commit on `main`, naming an identity that trusts `authorities`.
    fn commit_group(repo: &std::path::Path, parent: &str, group: &str, authorities: &str, message: &str) -> String {
        let identity = format!(
            r#"[{{"id": "acme", "authorities": {}}}]"#,
            serde_json::to_string(authorities).unwrap()
        );
        let edits = [
            (
                format!("groups/{group}/system.json"),
                br#"{"version":1}"#.to_vec(),
            ),
            (
                format!("groups/{group}/sources.json"),
                br#"[{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g","properties":{"security.protocol":"SSL"},"identity":"acme"}]"#.to_vec(),
            ),
            (format!("groups/{group}/identities.json"), identity.into_bytes()),
        ]
        .map(|(path, content)| git::FileEdit {
            path,
            content: Some(content),
        });
        match git::commit_edits(
            repo,
            git::MAIN_BRANCH,
            parent,
            Some(parent),
            &edits,
            message,
            &store::Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(commit) => commit,
            _ => panic!("{message} did not land"),
        }
    }

    /// A key's condition comes from its source's kind where the group's deployed bundle declares that source, and is
    /// left out where the group or the source is not deployed.
    #[test]
    fn a_streams_condition_is_its_deployed_sources() {
        let data = temp_dir("stream-conditions");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let root = git::main_head(&repo).unwrap();
        let commit = commit_group(&repo, &root, "g", &certificate(30), "add a kafka source");
        git::deploy_commit(&data, &commit, &store::Actor::system()).unwrap();

        let unmatched = |group: &str, source: &str| series::StoredUnmatched {
            group_id: group.into(),
            source: source.into(),
            rows: 1,
            keys_dropped: 0,
            first_seen_ms: 0,
            last_seen_ms: 0,
            keys: vec![series::StoredStreamKey {
                key: "orders".into(),
                rows: 1,
                first_seen_ms: 0,
                last_seen_ms: 0,
                condition: None,
                recent: false,
            }],
        };
        let mut streams = [unmatched("g", "s"), unmatched("g", "gone"), unmatched("other", "s")];
        stream_conditions(&data, &mut streams);
        let conditions: Vec<_> = streams.iter().map(|s| s.keys[0].condition.as_deref()).collect();
        assert_eq!(conditions, [Some(r#"__source.topic == "orders""#), None, None]);
    }

    /// The commit changed underneath a worker that has not caught up: `deployed_certificates` must keep answering
    /// about what that worker actually runs, not about `main`.
    #[test]
    fn a_certificate_is_read_off_the_commit_a_worker_reports_not_off_main() {
        let data = temp_dir("stale-worker");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let root = git::main_head(&repo).unwrap();

        let old = commit_group(&repo, &root, "g", &certificate(3), "issue a short-lived certificate");
        // `main` moves on to a replacement, but the worker in this test has not redeployed onto it yet.
        commit_group(
            &repo,
            &old,
            "g",
            &certificate(3650),
            "rotate onto a long-lived certificate",
        );
        assert_ne!(
            git::main_head(&repo).unwrap(),
            old,
            "the fixture must have actually moved main"
        );

        let running = std::collections::HashSet::from([("g".to_string(), old.clone())]);
        let found = deployed_certificates(&data, &Expiries::default(), running);

        assert_eq!(found.len(), 1, "{found:?}");
        let life = found[0].not_after_millis - found[0].not_before_millis;
        assert!(
            life < 4 * 24 * 60 * 60 * 1000,
            "read the long-lived replacement off main instead of the commit the worker reports: {life}ms"
        );
    }

    /// A group split across a rollout has workers on two commits, and a certificate on either is real: replacing
    /// one on `main` must not silently drop the warning for a worker still running the one it replaced.
    #[test]
    fn a_group_split_across_two_commits_reads_both() {
        let data = temp_dir("split-rollout");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let root = git::main_head(&repo).unwrap();

        let old = commit_group(&repo, &root, "g", &certificate(3), "issue a short-lived certificate");
        let new = commit_group(
            &repo,
            &old,
            "g",
            &certificate(3650),
            "rotate onto a long-lived certificate",
        );

        let running = std::collections::HashSet::from([("g".to_string(), old), ("g".to_string(), new)]);
        let found = deployed_certificates(&data, &Expiries::default(), running);

        assert_eq!(
            found.len(),
            2,
            "a worker on each commit contributes its own certificate: {found:?}"
        );
    }

    /// A commit nothing has deployed yet -- `main` moved, no worker has heartbeated onto it -- must not raise a
    /// warning about material nobody is running.
    #[test]
    fn a_commit_no_worker_reports_running_contributes_nothing() {
        let data = temp_dir("nobody-running");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let root = git::main_head(&repo).unwrap();
        commit_group(
            &repo,
            &root,
            "g",
            &certificate(3),
            "issue a certificate nobody has deployed",
        );

        let found = deployed_certificates(&data, &Expiries::default(), std::collections::HashSet::new());
        assert!(found.is_empty(), "{found:?}");
    }

    /// Two sources sharing one identity name its key twice -- once each, so `missing_secrets` can say which
    /// connector is stuck if either is. Entitlement is a different question: how many times the leader seals and
    /// sends the same key, which is once, however many connectors named it.
    #[test]
    fn a_key_two_sources_share_is_entitled_once() {
        let data = temp_dir("shared-identity-key");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let root = git::main_head(&repo).unwrap();

        let edits = [
            (
                "groups/g/system.json".to_string(),
                br#"{"version":1}"#.to_vec(),
            ),
            (
                "groups/g/sources.json".to_string(),
                br#"[
                    {"id":"s1","type":"kafka","brokers":"b:9092","topics":["t1"],"groupId":"g1","properties":{"security.protocol":"SSL"},"identity":"acme"},
                    {"id":"s2","type":"kafka","brokers":"b:9092","topics":["t2"],"groupId":"g2","properties":{"security.protocol":"SSL"},"identity":"acme"}
                ]"#
                .to_vec(),
            ),
            (
                "groups/g/identities.json".to_string(),
                format!(
                    r#"[{{"id":"acme","certificate":{},"key":{{"secret":"acme-key","version":1}}}}]"#,
                    serde_json::to_string(&certificate(365)).unwrap()
                )
                .into_bytes(),
            ),
        ]
        .map(|(path, content)| git::FileEdit {
            path,
            content: Some(content),
        });
        match git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &root,
            Some(&root),
            &edits,
            "two sources share one identity",
            &store::Actor::system(),
        )
        .unwrap()
        {
            git::Committed::At(commit) => {
                git::deploy_commit(&data, &commit, &store::Actor::system()).unwrap();
            }
            _ => panic!("the bundle did not land"),
        }

        let entitled = entitled_secrets(&data, &Entitlements::default(), "g");
        assert_eq!(
            entitled,
            vec![("acme-key".to_string(), 1)],
            "the same key named by two connectors is entitled once, not once per connector: {entitled:?}"
        );
    }

    /// A worker still on an older commit runs the version that commit quotes, so retention and the catalog both see
    /// it beside the deployed commit's.
    #[test]
    fn a_version_an_older_running_commit_quotes_is_deployed() {
        let data = temp_dir("older-commit-secret");
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let quote = |version: i64| {
            let head = git::main_head(&repo).unwrap();
            let edits = [
                ("groups/g/system.json".to_string(), br#"{"version":1}"#.to_vec()),
                (
                    "groups/g/sources.json".to_string(),
                    br#"[{"id":"s1","type":"kafka","brokers":"b:9092","topics":["t1"],"groupId":"g1","properties":{"security.protocol":"SSL"},"identity":"acme"}]"#.to_vec(),
                ),
                (
                    "groups/g/identities.json".to_string(),
                    format!(
                        r#"[{{"id":"acme","certificate":{},"key":{{"secret":"acme-key","version":{version}}}}}]"#,
                        serde_json::to_string(&certificate(365)).unwrap()
                    )
                    .into_bytes(),
                ),
            ]
            .map(|(path, content)| git::FileEdit {
                path,
                content: Some(content),
            });
            match git::commit_edits(
                &repo,
                git::MAIN_BRANCH,
                &head,
                Some(&head),
                &edits,
                "quote",
                &store::Actor::system(),
            )
            .unwrap()
            {
                git::Committed::At(commit) => commit,
                _ => panic!("the bundle did not land"),
            }
        };
        let older = quote(1);
        let newer = quote(2);
        git::deploy_commit(&data, &newer, &store::Actor::system()).unwrap();

        let cache = Entitlements::default();
        let deployed = deployed_versions(&data, &cache, &Default::default()).unwrap();
        assert_eq!(deployed["acme-key"], vec![("g".to_string(), 2)]);
        let running = [("g".to_string(), older)].into();
        let with_older = deployed_versions(&data, &cache, &running).unwrap();
        assert_eq!(with_older["acme-key"], vec![("g".to_string(), 1), ("g".to_string(), 2)]);

        let unreadable = [("g".to_string(), "0".repeat(40))].into();
        assert!(
            deployed_versions(&data, &cache, &unreadable).is_err(),
            "a running bundle that cannot be read pins nothing it might quote, so nothing may be forgotten"
        );
    }
}

#[cfg(test)]
fn test_secret_key(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("smeltr-leader-mod-{label}-{}.key", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

#[cfg(test)]
mod secrets_delivery_tests {
    use std::sync::Arc;

    use crate::heartbeat::{HeartbeatRequest, ThreadHeartbeat, ThreadKind};
    use crate::leader::store::Custody;
    use sqlite_store::SqliteStore;

    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-secrets-delivery-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One group's bundle, naming one credential of each custody. `version` is what a commit moves to deploy a
    /// rotated value, so the fixture takes it rather than fixing it.
    fn destinations(version: i64) -> String {
        format!(
            r#"[
        {{"id":"es","type":"elasticsearch","url":"https://es:9200","defaultIndex":"i",
         "auth":{{"secret":"es-auth","version":{version}}}}},
        {{"id":"es2","type":"elasticsearch","url":"https://es2:9200","defaultIndex":"i",
         "auth":"ES2_AUTH"}}]"#
        )
    }

    /// `main` moved to a bundle quoting `version` -- the commit an admin reviews and merges to deploy a rotation.
    fn deploy_bundle(data_dir: &std::path::Path, version: i64) {
        let repo = git::repo_path(data_dir);
        let parent = git::main_head(&repo).unwrap();
        let edits = [git::FileEdit {
            path: "groups/g/destinations.json".to_string(),
            content: Some(destinations(version).into_bytes()),
        }];
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &edits,
            "rotate",
            &store::Actor::system(),
        )
        .unwrap();
        git::deploy_commit(data_dir, &git::main_head(&repo).unwrap(), &store::Actor::system()).unwrap();
    }

    pub(super) fn state(label: &str) -> AppState {
        let data_dir = temp_dir(label);
        let repo = git::repo_path(&data_dir);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let edits = [
            git::FileEdit {
                path: "groups/g/system.json".to_string(),
                content: Some(br#"{"version":1}"#.to_vec()),
            },
            git::FileEdit {
                path: "groups/g/destinations.json".to_string(),
                content: Some(destinations(1).into_bytes()),
            },
        ];
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &edits,
            "deploy",
            &store::Actor::system(),
        )
        .unwrap();
        git::deploy_commit(&data_dir, &git::main_head(&repo).unwrap(), &store::Actor::system()).unwrap();

        let store = SqliteStore::open(std::path::Path::new(":memory:"), &data_dir.with_extension("key")).unwrap();
        store
            .create_secret("es-auth", Custody::LeaderStored, "", Some("ApiKey abc"))
            .unwrap();
        store
            .create_secret("unrelated", Custody::LeaderStored, "", Some("swordfish"))
            .unwrap();
        // Deliberately the same name the bundle names worker-locally. Custody is the bundle's word, not the
        // catalog's: naming a variable must not reach for a stored value that happens to share its spelling.
        store
            .create_secret("ES2_AUTH", Custody::LeaderStored, "", Some("not this one"))
            .unwrap();
        AppState {
            store: Arc::new(store),
            registry: Arc::new(Registry::default()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir,
        }
    }

    pub(super) fn beat(group: &str, held: Vec<(String, i64)>) -> HeartbeatRequest {
        HeartbeatRequest {
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
            group_id: group.to_string(),
            worker_id: "w1".to_string(),
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            authority_expires_millis: None,
            secret_versions: held,
            supervising: None,
            resources: None,
            threads: vec![ThreadHeartbeat {
                thread_id: "w1-bootstrap".to_string(),
                group_id: group.to_string(),
                kind: Some(ThreadKind::Worker),
                partitions_owned: vec![],
                metrics: vec![],
            }],
            accepts: Vec::new(),
            took: None,
        }
    }

    pub(super) async fn answer(state: &AppState, req: HeartbeatRequest) -> crate::heartbeat::HeartbeatResponse {
        heartbeat(
            State(state.clone()),
            Extension(AuthenticatedWorker("w1".to_string())),
            Extension(store::WorkerKind::Permanent),
            Json(req),
        )
        .await
        .expect("the heartbeat is answered")
        .0
    }

    /// The leader lists every capture it still wants from a worker on every beat, and stops once it is over or
    /// gone: that absence is what tells the worker to let go.
    #[tokio::test]
    async fn a_capture_is_listed_on_every_beat_until_it_is_over() {
        let state = state("capture-listed");
        let request = || {
            serde_json::from_value::<capture::CaptureRequest>(serde_json::json!({
                "group": "g",
                "source": {"id": "m", "type": "metrics", "bucketSeconds": 10}
            }))
            .unwrap()
        };
        let listed = |resp: crate::heartbeat::HeartbeatResponse| -> Vec<String> {
            resp.captures.iter().map(|c| c.session().to_string()).collect()
        };

        state.captures.open(
            "c1".into(),
            request(),
            ["w1".to_string()].into(),
            Default::default(),
            Vec::new(),
        );
        assert_eq!(listed(answer(&state, beat("g", vec![])).await), vec!["c1".to_string()]);
        assert_eq!(
            listed(answer(&state, beat("g", vec![])).await),
            vec!["c1".to_string()],
            "listed again on the next beat"
        );
        state.captures.stop("c1");
        assert!(
            listed(answer(&state, beat("g", vec![])).await).is_empty(),
            "a stopped capture is left off"
        );

        state.captures.open(
            "c2".into(),
            request(),
            ["w1".to_string()].into(),
            Default::default(),
            Vec::new(),
        );
        assert_eq!(listed(answer(&state, beat("g", vec![])).await), vec!["c2".to_string()]);
        state.captures.close("c2");
        assert!(
            listed(answer(&state, beat("g", vec![])).await).is_empty(),
            "a closed capture is left off"
        );
    }

    /// What a worker gets is what its own group's bundle names, and only the leader-stored half of that: a
    /// worker-local reference has no value here to send, and a secret no bundle names reaches nobody.
    #[tokio::test]
    async fn a_worker_is_sent_the_stored_credentials_its_group_names() {
        let state = state("entitled");
        let resp = answer(&state, beat("g", Vec::new())).await;

        let sent: Vec<(String, i64, String)> = resp.secrets.into_iter().map(|s| (s.name, s.version, s.value)).collect();
        assert_eq!(sent, vec![("es-auth".to_string(), 1, "ApiKey abc".to_string())]);
    }

    /// A worker already holding it is told nothing, which is every heartbeat but the few after a rotation.
    #[tokio::test]
    async fn a_worker_that_holds_it_is_sent_nothing() {
        let state = state("held");
        let resp = answer(&state, beat("g", vec![("es-auth".to_string(), 1)])).await;
        assert!(resp.secrets.is_empty());
    }

    /// Entitlement is the group's own bundle, so a worker reporting another group is sent nothing -- one group's
    /// broker credentials are not another's to receive.
    #[tokio::test]
    async fn a_worker_in_another_group_is_sent_nothing() {
        let state = state("other-group");
        let resp = answer(&state, beat("somewhere-else", Vec::new())).await;
        assert!(resp.secrets.is_empty());
    }

    // -- pointing a bundle's references at a version ---------------------

    async fn point(state: &AppState, name: &str, version: i64, branch: &str) -> Envelope<PointedSecret> {
        point_secret(
            State(state.clone()),
            axum::Extension(store::Actor::system()),
            Path(name.to_string()),
            Json(PointSecretRequest {
                version,
                branch: branch.to_string(),
            }),
        )
        .await
        .0
    }

    /// What the branch says `es`'s auth is, read back out of the commit rather than out of the response.
    fn auth_on(state: &AppState, branch: &str) -> crate::config_file::SecretRefDto {
        let repo = git::repo_path(&state.data_dir);
        let head = git::branch_tip(&repo, branch).expect("the branch exists");
        let bytes = git::read_blobs(&repo, &head, &["groups/g/destinations.json".to_string()])
            .unwrap()
            .pop()
            .flatten()
            .expect("the file is there");
        let dtos: Vec<crate::config_file::DestinationDto> = serde_json::from_slice(&bytes).unwrap();
        match &dtos[0].kind {
            crate::config_file::DestinationKindDto::Elasticsearch { auth, .. } => auth.clone().expect("an auth"),
            other => panic!("expected elasticsearch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pointing_moves_every_reference_and_opens_a_change() {
        let state = state("point");
        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();

        let out = point(&state, "es-auth", 2, "rotate-es").await;
        assert!(out.ok, "{:?}", out.errors);
        assert_eq!(out.references, 1);
        assert_eq!(out.files, vec!["groups/g/destinations.json".to_string()]);
        assert_eq!(out.groups, vec!["g".to_string()]);
        assert!(out.opened_change);

        assert_eq!(
            auth_on(&state, "rotate-es"),
            crate::config_file::SecretRefDto::LeaderStored(crate::config_file::LeaderSecretDto {
                secret: "es-auth".to_string(),
                version: 2,
            })
        );

        let change = git::read_change(&git::repo_path(&state.data_dir), "rotate-es")
            .unwrap()
            .expect("a change is open");
        assert_eq!(change.state, git::ChangeState::Open);
        assert!(change.title.contains("es-auth"), "{}", change.title);
    }

    /// `main` moves by deploy, and the leader never edits it: a rotation is work somebody reviews.
    #[tokio::test]
    async fn pointing_refuses_to_write_main() {
        let state = state("point-main");
        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();

        let out = point(&state, "es-auth", 2, git::MAIN_BRANCH).await;
        assert!(!out.ok);
        assert!(out.errors[0].message.contains(git::MAIN_BRANCH));
        assert_eq!(
            auth_on(&state, git::MAIN_BRANCH),
            crate::config_file::SecretRefDto::LeaderStored(crate::config_file::LeaderSecretDto {
                secret: "es-auth".to_string(),
                version: 1,
            }),
            "main did not move"
        );
    }

    /// Writing a reference to a value the leader does not hold would be drafting a bundle the fleet refuses.
    #[tokio::test]
    async fn pointing_at_a_version_that_is_not_held_is_refused() {
        let state = state("point-absent");
        let out = point(&state, "es-auth", 9, "rotate-es").await;
        assert!(!out.ok);
        assert!(out.errors[0].message.contains("version 9"));
        assert!(git::branch_tip(&git::repo_path(&state.data_dir), "rotate-es").is_none());
    }

    /// Nothing to move writes nothing: no branch, and no empty change for somebody to review.
    #[tokio::test]
    async fn pointing_at_what_is_already_named_writes_nothing() {
        let state = state("point-noop");
        let out = point(&state, "es-auth", 1, "rotate-es").await;
        assert!(out.ok, "{:?}", out.errors);
        assert_eq!(
            (out.references, out.commit.as_deref(), out.opened_change),
            (0, None, false)
        );
        assert!(git::branch_tip(&git::repo_path(&state.data_dir), "rotate-es").is_none());
    }

    /// Asked on `main` with nothing to move, it answers that no branch is needed: a new value nothing names.
    #[tokio::test]
    async fn pointing_nothing_on_main_needs_no_branch() {
        let state = state("point-main-nothing");
        state
            .store
            .create_secret("unused", Custody::LeaderStored, "", Some("v1"))
            .unwrap();

        let main = git::main_head(&git::repo_path(&state.data_dir));
        let out = point(&state, "unused", 1, git::MAIN_BRANCH).await;
        assert!(out.ok, "{:?}", out.errors);
        assert_eq!((out.references, out.commit.as_deref()), (0, None));
        assert_eq!(
            git::main_head(&git::repo_path(&state.data_dir)),
            main,
            "main did not move"
        );
    }

    /// A worker-local reference names a variable on a host, where a version means nothing -- so a stored secret
    /// sharing its spelling must not drag it into a rotation.
    #[tokio::test]
    async fn pointing_leaves_a_worker_local_reference_alone() {
        let state = state("point-local");
        state
            .store
            .add_secret_version("ES2_AUTH", "not this one either")
            .unwrap();

        let out = point(&state, "ES2_AUTH", 2, "rotate-es2").await;
        assert!(out.ok, "{:?}", out.errors);
        assert_eq!(out.references, 0, "nothing names it at a version");
        assert!(git::branch_tip(&git::repo_path(&state.data_dir), "rotate-es2").is_none());
    }

    /// A branch already under review keeps its own title: this is a bump added to somebody's work, not a rename.
    #[tokio::test]
    async fn pointing_joins_a_change_already_open() {
        let state = state("point-join");
        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();
        assert!(point(&state, "es-auth", 2, "wip").await.ok);

        state.store.add_secret_version("es-auth", "ApiKey ghi").unwrap();
        let out = point(&state, "es-auth", 3, "wip").await;
        assert!(out.ok, "{:?}", out.errors);
        assert!(!out.opened_change, "the change was already open");

        let change = git::read_change(&git::repo_path(&state.data_dir), "wip")
            .unwrap()
            .unwrap();
        assert!(change.title.contains("version 2"), "kept its title: {}", change.title);
    }

    /// Retention keeps the newest few. The second clause is the one that matters: a version the fleet is running
    /// is kept whatever the count says, or the next redeploy of a bundle nobody touched would find a credential
    /// the leader cannot produce.
    #[test]
    fn retention_keeps_the_newest_and_whatever_is_running() {
        assert_eq!(versions_to_forget(&[1, 2, 3, 4, 5], 3, &[]), vec![1, 2]);
        assert_eq!(versions_to_forget(&[1, 2, 3], 5, &[]), Vec::<i64>::new());
        assert_eq!(versions_to_forget::<i64>(&[], 3, &[]), Vec::<i64>::new());

        assert_eq!(
            versions_to_forget(&[1, 2, 3, 4, 5], 3, &[1]),
            vec![2],
            "the running one survives being oldest"
        );
        assert_eq!(versions_to_forget(&[1, 2, 3, 4, 5], 3, &[1, 2]), Vec::<i64>::new());

        // Unsorted input, since nothing promises the store hands them over in order.
        assert_eq!(versions_to_forget(&[5, 1, 3, 2, 4], 2, &[]), vec![1, 2, 3]);
        // `-1` in the setting, which keeps every version.
        assert_eq!(versions_to_forget(&[1, 2], usize::MAX, &[]), Vec::<i64>::new());
    }

    /// The name is whatever was in the URL, so a caller who asks about names that do not exist must not be able to
    /// grow this without bound.
    #[test]
    fn a_name_stops_being_tracked_once_its_attempts_age_out() {
        let limiter = VerifyLimiter::default();
        let t0 = std::time::Instant::now();
        for i in 0..20 {
            limiter.allow(&format!("no-such-secret-{i}"), t0);
        }
        assert_eq!(limiter.tracked(), 20);

        limiter.allow("es-auth", t0 + VERIFY_WINDOW + Duration::from_millis(1));
        assert_eq!(limiter.tracked(), 1, "the aged-out names went with their attempts");
    }

    /// The oracle's bound. The window is what makes verify a check rather than a way to guess a value, and a
    /// refused attempt still counts -- otherwise spending the window and waiting out only the refusals would
    /// buy back a free attempt every second.
    #[test]
    fn a_secret_can_only_be_asked_about_so_often() {
        let limiter = VerifyLimiter::default();
        let t0 = std::time::Instant::now();

        for i in 0..VERIFY_ATTEMPTS {
            assert!(limiter.allow("es-auth", t0), "attempt {i} should be allowed");
        }
        assert!(!limiter.allow("es-auth", t0), "the bound holds");
        assert!(
            limiter.allow("kafka-west-sasl", t0),
            "another secret has its own window"
        );
        // Hammering through a full window refills it with refusals, so waiting out only the allowed attempts
        // buys nothing: a caller who keeps asking keeps themselves locked out.
        let halfway = t0 + VERIFY_WINDOW / 2;
        for _ in 0..VERIFY_ATTEMPTS {
            assert!(!limiter.allow("es-auth", halfway));
        }
        assert!(!limiter.allow("es-auth", t0 + VERIFY_WINDOW + Duration::from_millis(1)));

        // A caller who stops asking gets the window back.
        assert!(limiter.allow("es-auth", halfway + VERIFY_WINDOW + Duration::from_millis(1)));
    }

    /// A deployed bundle naming the secret is the one delete that cannot be undone, so it is the one refused.
    #[tokio::test]
    async fn a_credential_a_deployed_bundle_names_is_not_deleted() {
        let state = state("delete-in-use");

        let refused = delete_secret(State(state.clone()), Path("es-auth".to_string())).await;
        assert!(!refused.0.ok, "a deployed group names it");
        assert!(refused.0.errors[0].message.contains("es-auth"));
        assert!(
            state.store.verify_secret("es-auth", 1, "ApiKey abc").unwrap(),
            "still there"
        );

        let gone = delete_secret(State(state.clone()), Path("unrelated".to_string())).await;
        assert!(gone.0.ok, "nothing names it: {:?}", gone.0.errors);
        assert!(!state.store.verify_secret("unrelated", 1, "swordfish").unwrap());
    }

    /// A rollback holds the fleet on an older bundle, and releasing it deploys `main`'s head again, so the version
    /// that head names is kept while nothing runs it.
    #[tokio::test]
    async fn a_version_main_s_head_names_is_kept_while_a_rollback_holds_the_fleet() {
        let state = state("held-version");
        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();
        deploy_bundle(&state.data_dir, 2);
        let repo = git::repo_path(&state.data_dir);
        let first = git::log(&repo, git::MAIN_BRANCH, 10).unwrap()[1].commit.clone();
        git::deploy_commit(&state.data_dir, &first, &store::Actor::system()).unwrap();

        let refused = forget_secret_version(State(state.clone()), Path(("es-auth".to_string(), 2))).await;
        assert!(!refused.0.ok, "main's head names v2");
        assert!(
            state.store.verify_secret("es-auth", 2, "ApiKey def").unwrap(),
            "still there"
        );
    }

    /// Storing a value stages it and nothing more: until a commit quotes the new number, the fleet is not told it
    /// exists, and any redeploy in the meantime still runs the version its bundle names.
    #[tokio::test]
    async fn storing_a_value_reaches_no_worker_until_a_commit_quotes_it() {
        let state = state("rotate");
        let held = vec![("es-auth".to_string(), 1)];
        assert!(answer(&state, beat("g", held.clone())).await.secrets.is_empty());

        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();
        assert!(
            answer(&state, beat("g", held.clone())).await.secrets.is_empty(),
            "v2 is stored, and the bundle still runs v1"
        );

        deploy_bundle(&state.data_dir, 2);
        let resp = answer(&state, beat("g", held)).await;
        assert_eq!(resp.secrets.len(), 1, "{:?}", resp.secrets);
        assert_eq!(resp.secrets[0].version, 2);
        assert_eq!(resp.secrets[0].value, "ApiKey def");
    }

    /// A rollback is the same path run backward: the commit quotes the earlier version and the worker is sent it.
    #[tokio::test]
    async fn a_bundle_rolled_back_is_sent_the_version_it_names() {
        let state = state("rollback");
        state.store.add_secret_version("es-auth", "ApiKey def").unwrap();
        deploy_bundle(&state.data_dir, 2);

        let on_v2 = vec![("es-auth".to_string(), 2)];
        assert!(answer(&state, beat("g", on_v2.clone())).await.secrets.is_empty());

        deploy_bundle(&state.data_dir, 1);
        let resp = answer(&state, beat("g", on_v2)).await;
        assert_eq!(resp.secrets.len(), 1, "{:?}", resp.secrets);
        assert_eq!(resp.secrets[0].version, 1);
        assert_eq!(resp.secrets[0].value, "ApiKey abc");
    }
}

#[cfg(test)]
mod warnings_endpoint_tests {
    use std::sync::Arc;

    use crate::heartbeat::{MetricSampleDto, MetricValueDto, ThreadHeartbeat, ThreadKind};
    use sqlite_store::SqliteStore;

    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-leader-mod-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `main` deployed with group `id`'s subtree -- every test here heartbeats that group, and without this it
    /// would also raise `GroupNotDeployed`, which is not what any of them are about.
    fn deploy_group(data_dir: &std::path::Path, id: &str) {
        let repo = git::repo_path(data_dir);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let parent = git::main_head(&repo).unwrap();
        let edit = git::FileEdit {
            path: format!("groups/{id}/system.json"),
            content: Some(br#"{"version":1}"#.to_vec()),
        };
        git::commit_edits(
            &repo,
            git::MAIN_BRANCH,
            &parent,
            Some(&parent),
            &[edit],
            "deploy",
            &store::Actor::system(),
        )
        .unwrap();
        git::deploy_commit(data_dir, &git::main_head(&repo).unwrap(), &store::Actor::system()).unwrap();
    }

    fn state(label: &str) -> AppState {
        let data_dir = temp_dir(label);
        deploy_group(&data_dir, "g");
        AppState {
            store: Arc::new(SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key(label)).unwrap()),
            registry: Arc::new(Registry::default()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir,
        }
    }

    /// A roster the store cannot read is an error, never a fleet with nobody in it: that would show an operator
    /// a rotation with nothing left to wait for.
    #[tokio::test]
    async fn an_unreadable_roster_is_an_error_not_an_empty_fleet() {
        let dir = temp_dir("roster-unreadable");
        let db = dir.join("leader.db");
        let store = SqliteStore::open(&db, &dir.join("sealing.key")).unwrap();
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("DROP TABLE worker_credentials")
            .unwrap();
        let state = AppState {
            store: Arc::new(store),
            ..state("roster-unreadable")
        };
        let read = rotation_workers(&state, "incoming").await;
        assert!(
            read.as_ref()
                .is_err_and(|e| e.starts_with("could not read which workers are enrolled: ")),
            "{read:?}"
        );
    }

    #[tokio::test]
    async fn a_healthy_registry_and_roster_serve_no_warnings() {
        let state = state("healthy");
        state.registry.record_heartbeat(
            "w1",
            vec![ThreadHeartbeat {
                thread_id: "reader-1".to_string(),
                group_id: "g".to_string(),
                kind: Some(ThreadKind::Reader),
                partitions_owned: vec![],
                metrics: vec![],
            }],
            None,
            None,
            crate::version::VERSION.to_string(),
            None,
            None,
            None,
            None,
            false,
            0,
        );
        assert!(warnings_snapshot(&state).await.unwrap().is_empty());
    }

    /// The endpoint reads the worker roster from `Store`, not only the registry -- the one entry `health_snapshot`
    /// cannot produce, since it never touches `Store` at all.
    #[tokio::test]
    async fn an_enrolled_worker_with_no_thread_is_reported_by_the_endpoint() {
        let state = state("enrolled-no-thread");
        run_store(state.store.clone(), |s| s.store_worker_credential("w1", "hash"))
            .await
            .unwrap();

        let warnings = warnings_snapshot(&state).await.unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].subject_id, "w1");
        assert_eq!(warnings[0].severity, warnings::Severity::Warning);
    }

    /// A fleet scaled to nothing sends no heartbeat, and the Workers page reads the roster and never the warnings.
    /// So reading the roster is itself a look for gone ephemeral workers.
    #[tokio::test]
    async fn reading_the_roster_forgets_a_gone_ephemeral_worker() {
        let mut state = state("roster-forgets");
        // A leader up long enough to have heard the silence: one just started forgets nobody for it.
        state.registry =
            Arc::new(Registry::default().started_at(std::time::Instant::now() - 2 * forget::DEFAULT_FORGET_AFTER));
        let long_ago = warnings::now_millis() - 2 * forget::DEFAULT_FORGET_AFTER.as_millis() as i64;
        run_store(state.store.clone(), move |s| {
            s.create_enrollment_code(&store::NewEnrollmentCode {
                id: "j1".to_string(),
                code_hash: "h1".to_string(),
                fingerprint: "fp".to_string(),
                group_id: "g".to_string(),
                minted_by: "admin".to_string(),
                minted_at: long_ago,
                expires_at: None,
                max_uses: None,
                kind: store::WorkerKind::Ephemeral,
            })?;
            s.spend_enrollment_code("h1", "pod-1", "t-1", long_ago, "fp", &["fp"])
                .map(|_| ())
        })
        .await
        .unwrap();

        let roster = worker_credentials_snapshot(&state).await.unwrap();
        assert!(
            roster.is_empty(),
            "{:?}",
            roster.iter().map(|w| &w.worker_id).collect::<Vec<_>>()
        );
    }

    /// A viewer is served the warnings of the groups they hold. A worker that has never heartbeated has named no
    /// group, so only an administrator is told of it.
    #[tokio::test]
    async fn a_viewer_is_served_only_the_warnings_of_their_groups() {
        let state = state("visible-to");
        state.registry.record_heartbeat(
            "w1",
            vec![ThreadHeartbeat {
                thread_id: "reader-1".to_string(),
                group_id: "g".to_string(),
                kind: Some(ThreadKind::Reader),
                partitions_owned: vec![],
                metrics: vec![MetricSampleDto {
                    metric: "smeltr_source_blocked_reason".to_string(),
                    labels: vec![("source".to_string(), "7".to_string())],
                    value: MetricValueDto::Text("held".to_string()),
                }],
            }],
            None,
            None,
            crate::version::VERSION.to_string(),
            None,
            None,
            None,
            None,
            false,
            0,
        );
        run_store(state.store.clone(), |s| s.store_worker_credential("w2", "hash"))
            .await
            .unwrap();
        let viewer = |group: &str| {
            access::Access::Areas(std::collections::BTreeMap::from([(
                group.to_string(),
                access::Level::Viewer,
            )]))
        };
        let served = |access: access::Access| {
            let state = state.clone();
            async move {
                list_warnings(
                    State(state),
                    axum::Extension(access),
                    Query(WarningsQuery { info: false }),
                )
                .await
                .unwrap()
                .0
                .into_iter()
                .map(|w| w.subject_id)
                .collect::<Vec<_>>()
            }
        };

        assert_eq!(served(viewer("g")).await, vec!["7"]);
        assert!(served(viewer("search")).await.is_empty());
        let mut everything = served(access::Access::Administrator).await;
        everything.sort();
        assert_eq!(everything, vec!["7", "w2"]);
    }

    /// The fact clock lives on the registry, not this function's own stack, so a source blocked across two calls keeps
    /// the start time from the first -- the endpoint can be polled without resetting what it reports.
    #[tokio::test]
    async fn a_sources_start_time_survives_across_polls() {
        let state = state("source-start-time");
        let held = ThreadHeartbeat {
            thread_id: "reader-1".to_string(),
            group_id: "g".to_string(),
            kind: Some(ThreadKind::Reader),
            partitions_owned: vec![],
            metrics: vec![MetricSampleDto {
                metric: "smeltr_source_blocked_reason".to_string(),
                labels: vec![("source".to_string(), "7".to_string())],
                value: MetricValueDto::Text("held".to_string()),
            }],
        };
        state.registry.record_heartbeat(
            "w1",
            vec![held.clone()],
            None,
            None,
            crate::version::VERSION.to_string(),
            None,
            None,
            None,
            None,
            false,
            0,
        );

        let first = warnings_snapshot(&state).await.unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        state.registry.record_heartbeat(
            "w1",
            vec![held],
            None,
            None,
            String::new(),
            None,
            None,
            None,
            None,
            false,
            0,
        );
        let second = warnings_snapshot(&state).await.unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(
            first[0].since_millis, second[0].since_millis,
            "the same occurrence, not a new one"
        );
    }
}

#[cfg(test)]
mod logs_endpoint_tests {
    use std::sync::Arc;

    use sqlite_store::SqliteStore;

    use super::*;

    fn state() -> AppState {
        AppState {
            store: Arc::new(SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("logs")).unwrap()),
            registry: Arc::new(Registry::open(std::path::Path::new(":memory:")).unwrap()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir: std::env::temp_dir(),
        }
    }

    fn said(seq: u64, level: &str, site: &str, message: &str) -> crate::heartbeat::LogDto {
        crate::heartbeat::LogDto {
            seq,
            timestamp_ms: 1_000 + seq as i64,
            level: level.to_string(),
            site: site.to_string(),
            message: message.to_string(),
            thread: Some("main".to_string()),
            boot_id: None,
            fields: vec![(
                "subject".to_string(),
                crate::heartbeat::FieldValueDto::Str("orders".to_string()),
            )],
        }
    }

    async fn get(state: &AppState, query: LogsQuery) -> LogsResponse {
        get_logs(State(state.clone()), Query(query)).await.unwrap().0
    }

    #[tokio::test]
    async fn a_workers_logs_come_back_newest_first_with_their_fields() {
        let state = state();
        state.registry.record_logs(
            "w-1",
            7,
            Some("west"),
            &[
                said(0, "info", "worker.started", "worker starting"),
                said(1, "error", "runtime.rows_rejected", "a destination refused rows"),
            ],
        );

        let answer = get(&state, LogsQuery::default()).await;
        assert_eq!(answer.rows.len(), 2);
        assert_eq!(answer.rows[0].site, "runtime.rows_rejected", "newest first");
        assert_eq!(answer.rows[0].group.as_deref(), Some("west"));
        assert_eq!(answer.rows[0].worker_id, "w-1");
        // The fields pass through as delivered rather than reshaped, so what a call site said is what is indexed.
        assert_eq!(answer.rows[0].fields[0][0], "subject");
        assert_eq!(answer.rows[0].fields[0][1]["value"], "orders");
        // A reader is told where search stops, rather than quietly finding nothing past it.
        assert_eq!(answer.message_limit, crate::heartbeat::MAX_TEXT_LEN);
    }

    #[tokio::test]
    async fn the_query_narrows_and_the_limit_is_clamped_rather_than_refused() {
        let state = state();
        state.registry.record_logs(
            "w-1",
            7,
            Some("west"),
            &[
                said(0, "info", "worker.started", "worker starting"),
                said(1, "error", "runtime.rows_rejected", "a destination refused rows"),
            ],
        );

        let only_errors = get(
            &state,
            LogsQuery {
                level: Some("error".to_string()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(only_errors.rows.len(), 1);
        assert_eq!(only_errors.rows[0].level, "error");

        // Asked for more than a page may carry, and told what it was given rather than refused.
        let huge = get(
            &state,
            LogsQuery {
                limit: Some(usize::MAX),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(huge.limit, LOGS_PAGE_MAX);
        // And zero is not a way to ask for nothing.
        let zero = get(
            &state,
            LogsQuery {
                limit: Some(0),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(zero.limit, 1);
    }

    /// A leader with no durable registry has no logs to serve, which is a state rather than a failure.
    #[tokio::test]
    async fn a_leader_with_no_durable_registry_answers_empty() {
        let state = AppState {
            registry: Arc::new(Registry::default()),
            ..state()
        };
        assert!(get(&state, LogsQuery::default()).await.rows.is_empty());
    }

    /// The pager, end to end: a page with rows below it offers `before`, following it offers `after`, and
    /// `after` reconstructs the page it was reached from.
    #[tokio::test]
    async fn walking_older_then_newer_reconstructs_the_page_it_started_from() {
        let state = state();
        state.registry.record_logs(
            "w-1",
            7,
            None,
            &[
                said(0, "info", "a", "row 0"),
                said(1, "info", "a", "row 1"),
                said(2, "info", "a", "row 2"),
                said(3, "info", "a", "row 3"),
                said(4, "info", "a", "row 4"),
                said(5, "info", "a", "row 5"),
                said(6, "info", "a", "row 6"),
            ],
        );
        let msgs = |r: &LogsResponse| r.rows.iter().map(|row| row.message.clone()).collect::<Vec<_>>();

        let page1 = get(
            &state,
            LogsQuery {
                limit: Some(3),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(msgs(&page1), ["row 6", "row 5", "row 4"]);
        assert!(page1.next_after.is_none(), "the head page has nothing newer to page to");
        let before = page1.next_before.expect("a full page may hold more below it");

        let page2 = get(
            &state,
            LogsQuery {
                limit: Some(3),
                before: Some(before),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(msgs(&page2), ["row 3", "row 2", "row 1"]);
        let after = page2
            .next_after
            .expect("reached by a cursor, so a page above it exists");

        // Walking back up reconstructs page 1's rows exactly, `next_before` included: going older from here
        // is still possible, and nothing about paging newer changes that.
        let back = get(
            &state,
            LogsQuery {
                limit: Some(3),
                after: Some(after),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(msgs(&back), msgs(&page1));
        assert_eq!(back.next_before, page1.next_before);
        // Exactly `limit` rows, the same shape it had going down, and nothing above it: the row fetched
        // past the page is what tells those apart.
        assert!(
            back.next_after.is_none(),
            "walked back to the head, so there is nothing newer to offer"
        );
    }

    /// The same question at the other end: a filter whose rows number exactly one page has nothing below it,
    /// which the page's own length cannot say.
    #[tokio::test]
    async fn a_page_that_ends_on_the_last_row_offers_nothing_older() {
        let state = state();
        state.registry.record_logs(
            "w-1",
            3,
            None,
            &[
                said(0, "info", "a", "row 0"),
                said(1, "info", "a", "row 1"),
                said(2, "info", "a", "row 2"),
            ],
        );
        let page = get(
            &state,
            LogsQuery {
                limit: Some(3),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(page.rows.len(), 3);
        assert!(page.next_before.is_none() && page.next_after.is_none());
    }
}

#[cfg(test)]
mod flow_endpoint_tests {
    use std::sync::Arc;

    use sqlite_store::SqliteStore;

    use super::*;

    /// A durable registry (`:memory:`, so nothing to clean up) -- unlike `warnings_endpoint_tests::state`, `flow`
    /// has nothing to serve without one.
    fn state() -> AppState {
        AppState {
            store: Arc::new(SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("flow")).unwrap()),
            registry: Arc::new(Registry::open(std::path::Path::new(":memory:")).unwrap()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir: std::env::temp_dir(),
        }
    }

    #[test]
    fn no_data_is_empty_series_not_an_error() {
        let flow = flow_snapshot(&state(), Duration::from_secs(3600)).unwrap();
        assert!(flow.rows_in.is_empty());
        assert!(flow.rows_out.is_empty());
    }

    #[test]
    fn a_registry_with_no_durable_store_answers_empty_rather_than_failing() {
        let state = AppState {
            store: Arc::new(
                SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("flow-no-store")).unwrap(),
            ),
            registry: Arc::new(Registry::default()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir: std::env::temp_dir(),
        };
        let flow = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert!(flow.rows_in.is_empty());
        assert!(flow.rows_out.is_empty());
    }

    #[test]
    fn rows_in_and_rows_out_read_separate_metrics_keyed_by_source_and_destination() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","prod-west"],["source","7"],["worker","worker-1"]]"#,
                now,
                5.0,
            )
            .unwrap();
        store
            .record_delta(
                ROWS_OUT_METRIC,
                r#"[["group","prod-west"],["destination","2"]]"#,
                now,
                9.0,
            )
            .unwrap();

        let flow = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert_eq!(flow.rows_in.len(), 1);
        assert_eq!(flow.rows_in[0].subject_id, "7");
        assert_eq!(flow.rows_in[0].group, "prod-west");
        assert_eq!(flow.rows_in[0].worker_id.as_deref(), Some("worker-1"));
        assert_eq!(flow.rows_in[0].points[0].value, 5.0);
        assert_eq!(flow.rows_out.len(), 1);
        assert_eq!(flow.rows_out[0].subject_id, "2");
        assert_eq!(flow.rows_out[0].group, "prod-west");
        assert_eq!(flow.rows_out[0].points[0].value, 9.0);
    }

    /// A viewer of one group reads that group's flow and not another's.
    #[tokio::test]
    async fn a_viewer_reads_only_the_flow_of_their_groups() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        for group in ["payments", "search"] {
            store
                .record_delta(
                    ROWS_IN_METRIC,
                    &format!(r#"[["group","{group}"],["source","orders"]]"#),
                    now,
                    5.0,
                )
                .unwrap();
            store
                .record_delta(
                    BYTES_DELIVERED_METRIC,
                    &format!(r#"[["group","{group}"],["destination","es"]]"#),
                    now,
                    5.0,
                )
                .unwrap();
        }
        let payments = access::Access::Areas(std::collections::BTreeMap::from([(
            "payments".to_string(),
            access::Level::Viewer,
        )]));

        let flow = get_flow(
            State(state),
            axum::Extension(payments),
            Query(FlowQuery { window_minutes: 60 }),
        )
        .await
        .unwrap();
        let groups = |series: &[FlowSeries]| series.iter().map(|s| s.group.clone()).collect::<Vec<_>>();
        assert_eq!(groups(&flow.rows_in), vec!["payments"]);
        assert_eq!(groups(&flow.bytes_out), vec!["payments"]);
    }

    /// The weight of the same flow, for the panel's hover readout. `bytes_out` comes off the
    /// delivered metric, so a destination that has taken rows but not yet acknowledged them
    /// reports rows out with no bytes beside them -- a lag, not a missing series.
    #[test]
    fn bytes_in_and_bytes_out_read_the_polled_and_delivered_metrics() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(
                BYTES_POLLED_METRIC,
                r#"[["group","prod-west"],["source","7"],["worker","worker-1"]]"#,
                now,
                4096.0,
            )
            .unwrap();
        store
            .record_delta(
                ROWS_OUT_METRIC,
                r#"[["group","prod-west"],["destination","2"]]"#,
                now,
                9.0,
            )
            .unwrap();

        let flow = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert_eq!(flow.bytes_in.len(), 1);
        assert_eq!(flow.bytes_in[0].subject_id, "7");
        assert_eq!(flow.bytes_in[0].group, "prod-west");
        assert_eq!(flow.bytes_in[0].points[0].value, 4096.0);
        // Rows were sent; nothing has been acknowledged, so there is no weight to report yet.
        assert_eq!(flow.rows_out.len(), 1);
        assert!(flow.bytes_out.is_empty());

        store
            .record_delta(
                BYTES_DELIVERED_METRIC,
                r#"[["group","prod-west"],["destination","2"]]"#,
                now,
                8192.0,
            )
            .unwrap();
        let flow = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert_eq!(flow.bytes_out.len(), 1);
        assert_eq!(flow.bytes_out[0].subject_id, "2");
        assert_eq!(flow.bytes_out[0].points[0].value, 8192.0);
    }

    /// An id is unique only within a group. Two groups running a source of the same name are two
    /// series, not one line whose height is their sum.
    #[test]
    fn one_id_in_two_groups_stays_two_series() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","prod-east"],["source","orders"]]"#,
                now,
                3.0,
            )
            .unwrap();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","prod-west"],["source","orders"]]"#,
                now,
                5.0,
            )
            .unwrap();

        let flow = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert_eq!(
            flow.rows_in
                .iter()
                .map(|s| (s.group.as_str(), s.subject_id.as_str(), s.points[0].value))
                .collect::<Vec<_>>(),
            vec![("prod-east", "orders", 3.0), ("prod-west", "orders", 5.0)]
        );
    }

    /// A key with no group is not a subject this can attribute, so it is dropped rather than
    /// drawn under a group it may not belong to.
    #[test]
    fn a_subject_key_without_a_group_is_dropped() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(ROWS_IN_METRIC, r#"[["source","orders"]]"#, now, 5.0)
            .unwrap();

        assert!(
            flow_snapshot(&state, Duration::from_secs(3600))
                .unwrap()
                .rows_in
                .is_empty()
        );
    }

    #[test]
    fn a_point_older_than_the_window_is_absent() {
        let state = state();
        let store = state.registry.series().unwrap();
        let now = warnings::now_millis();
        store
            .record_delta(
                ROWS_IN_METRIC,
                r#"[["group","prod-west"],["source","7"]]"#,
                now - 120_000,
                5.0,
            )
            .unwrap();

        let flow = flow_snapshot(&state, Duration::from_secs(60)).unwrap();
        assert!(
            flow.rows_in.is_empty(),
            "the point is two minutes old and the window is one"
        );
    }

    #[test]
    fn the_bucket_width_reflects_the_tier_the_window_chose() {
        let state = state();
        let hour = flow_snapshot(&state, Duration::from_secs(3600)).unwrap();
        assert_eq!(hour.bucket_millis, 10_000, "sixty minutes fits the finest tier");

        let past_a_day = flow_snapshot(&state, Duration::from_secs(3600 * 25)).unwrap();
        assert_eq!(
            past_a_day.bucket_millis,
            5 * 60_000,
            "past twenty-four hours needs the five-minute tier"
        );
    }

    #[tokio::test]
    async fn the_route_clamps_an_out_of_range_window_rather_than_failing() {
        let state = state();
        let huge = get_flow(
            State(state.clone()),
            axum::Extension(access::Access::Administrator),
            Query(FlowQuery {
                window_minutes: 1_000_000,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            huge.bucket_millis,
            60 * 60_000,
            "clamped to thirty days, the coarsest tier"
        );

        let zero = get_flow(
            State(state),
            axum::Extension(access::Access::Administrator),
            Query(FlowQuery { window_minutes: 0 }),
        )
        .await
        .unwrap();
        assert_eq!(
            zero.bucket_millis, 10_000,
            "clamped up to at least one minute, the finest tier"
        );
    }
}

#[cfg(test)]
mod authority_delivery_tests {
    use super::*;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "smeltr-authority-delivery-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        // A stand-in socket that takes every replacement the moment it is made, so what is on disk is what this
        // leader is presenting -- which is the condition the bundle is withheld under.
        tls::on_reload(|dir| tls::socket_took(dir, tls::disk_generation(dir)));
        dir
    }

    fn published(data_dir: &std::path::Path) -> Vec<String> {
        let materials = tls::current(&tls::dir_of(data_dir)).expect("an authority");
        tls::describe_each(&materials.ca_pem, tls::CA_CERT)
            .expect("readable")
            .into_iter()
            .map(|root| root.fingerprint)
            .collect()
    }

    fn a_leader_with_an_authority(label: &str) -> std::path::PathBuf {
        let data_dir = temp_dir(label);
        tls::ensure(
            &tls::dir_of(&data_dir),
            vec!["localhost".to_string()],
            std::time::Duration::from_secs(365 * 24 * 60 * 60),
            &[],
        )
        .expect("an authority");
        data_dir
    }

    /// The bundle goes out whenever a worker's trust store is not this leader's, rotation or no rotation. A
    /// retirement is only ever this: a root the worker holds and the bundle no longer names.
    #[test]
    fn a_worker_holding_something_else_is_handed_the_whole_bundle() {
        let data_dir = a_leader_with_an_authority("differs");
        let published = published(&data_dir);

        assert_eq!(
            authority_to_hand(&data_dir, &published, true),
            None,
            "a worker already holding what is published was sent a kilobyte it had"
        );
        let retired = vec![published[0].clone(), "a root this leader has dropped".to_string()];
        assert!(
            authority_to_hand(&data_dir, &retired, true).is_some(),
            "a worker still holding a retired root was left holding it"
        );
        assert!(
            authority_to_hand(&data_dir, &[], true).is_some(),
            "a worker holding nothing this leader published was left with nothing"
        );
    }

    /// The root behind a certificate an operator supplied is handed out like the leader's own: a switch of
    /// source is a rotation, and the worker that follows it is the one given the incoming root on its beat.
    #[test]
    fn the_roots_behind_a_certificate_an_operator_supplied_are_handed_out_too() {
        let data_dir = a_leader_with_an_authority("supplied");
        let dir = tls::dir_of(&data_dir);
        let materials = tls::current(&dir).expect("an authority");
        let chain = std::fs::read(&materials.server_cert_path).unwrap();
        let key = std::fs::read(&materials.server_key_path).unwrap();
        tls::install(&dir, &chain, &key, &materials.ca_pem, &["localhost".to_string()], &[]).expect("a supplied set");

        assert_eq!(
            authority_to_hand(&data_dir, &[], true).map(|pem| pem.into_bytes()),
            Some(materials.ca_pem)
        );
    }

    /// Trust an operator manages is not this leader's to replace, and a worker holding it never comes into line --
    /// so asking whether it has would send it the bundle on every beat for the life of the fleet.
    #[test]
    fn a_worker_given_its_authority_by_its_operator_is_handed_nothing() {
        let data_dir = a_leader_with_an_authority("theirs");
        assert_eq!(authority_to_hand(&data_dir, &[], false), None);
    }

    /// A cutover nobody pressed for, on the deadline, is in the audit log as the system's.
    #[tokio::test]
    async fn a_cutover_on_the_deadline_is_recorded_as_the_systems() {
        let data_dir = a_leader_with_an_authority("cut-over-recorded");
        let dir = tls::dir_of(&data_dir);
        let now_ms = warnings::now_millis();
        let rotation = tls::begin_rotation(&dir, Duration::from_secs(365 * 24 * 60 * 60), now_ms - 1000, now_ms)
            .expect("a rotation past its deadline");
        let state = AppState {
            store: Arc::new(
                sqlite_store::SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("cut-over"))
                    .unwrap(),
            ),
            registry: Arc::new(Registry::default()),
            captures: Arc::new(capture::Captures::default()),
            entitlements: Arc::new(Entitlements::default()),
            verify_limiter: Arc::new(VerifyLimiter::default()),
            expiries: Default::default(),
            throughput: Default::default(),
            deploy_checks: Default::default(),
            origin_hosts: Vec::new(),
            remote: Default::default(),
            updates: Default::default(),
            directives: Default::default(),
            data_dir,
        };
        cut_over_if_due(&state, now_ms).await;
        assert!(
            tls::rotation(&dir).unwrap().is_none(),
            "the deadline did not end the rotation"
        );
        let events = state.store.list_security_events(10).unwrap();
        assert!(
            events.iter().any(|e| e.action == "authority-cut-over"
                && e.outcome == rotation.incoming
                && e.actor == store::Actor::SYSTEM_ID),
            "{events:?}"
        );
    }
}

#[cfg(test)]
mod worker_lifecycle_tests {
    use super::secrets_delivery_tests::{answer, beat, state};
    use super::*;
    use crate::heartbeat::DirectiveAction;

    fn heard(state: &AppState) {
        state.store.store_worker_credential("w1", "hash-w1").unwrap();
    }

    fn accepting(took: Option<&str>) -> HeartbeatRequest {
        HeartbeatRequest {
            accepts: vec![DirectiveAction::Restart, DirectiveAction::Shutdown],
            took: took.map(str::to_string),
            ..beat("g", Vec::new())
        }
    }

    #[tokio::test]
    async fn a_restart_rides_every_answer_until_the_worker_takes_it() {
        let state = state("lifecycle-restart");
        heard(&state);
        assert!(answer(&state, accepting(None)).await.directive.is_none());
        assert!(restart_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
        let sent = answer(&state, accepting(None)).await.directive.expect("asked");
        assert_eq!(sent.action, DirectiveAction::Restart);
        assert_eq!(answer(&state, accepting(None)).await.directive, Some(sent.clone()));
        assert!(answer(&state, accepting(Some(&sent.id))).await.directive.is_none());
    }

    #[tokio::test]
    async fn a_worker_that_has_not_said_it_shuts_down_is_not_asked_to() {
        let state = state("lifecycle-older");
        heard(&state);
        answer(&state, beat("g", Vec::new())).await;
        assert!(!shutdown_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
    }

    #[tokio::test]
    async fn a_revoked_worker_is_removed_without_waiting_out_its_last_heartbeat() {
        let state = state("lifecycle-remove-revoked");
        heard(&state);
        answer(&state, accepting(None)).await;
        assert!(revoke_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
        assert!(
            state.registry.heartbeating("w1", heartbeat_window()),
            "its last beat is still recent"
        );
        assert!(remove_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
        assert!(state.store.list_worker_credentials().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_worker_is_removed_only_once_it_stops_heartbeating_and_its_id_stays_refused() {
        let state = state("lifecycle-remove");
        heard(&state);
        answer(&state, accepting(None)).await;
        assert!(
            !remove_worker(State(state.clone()), Path("w1".to_string())).await.0.ok,
            "a heartbeating worker is not removed"
        );
        answer(
            &state,
            HeartbeatRequest {
                stopping: true,
                ..accepting(None)
            },
        )
        .await;
        assert!(remove_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
        assert!(state.store.list_worker_credentials().unwrap().is_empty());
        assert!(state.registry.snapshot().is_empty());
        assert!(
            state.store.store_worker_credential("w1", "hash-again").is_err(),
            "a removed worker's id is never enrolled again"
        );
        assert!(
            state
                .store
                .get_worker_credential_by_hash("hash-w1")
                .unwrap()
                .unwrap()
                .revoked,
            "its credential is refused"
        );
    }
}

#[cfg(test)]
mod single_worker_tests {
    use std::sync::Arc;

    use super::secrets_delivery_tests::{beat, state};
    use super::*;
    use crate::heartbeat::{LedgerEntry, SourceKey, SourceProgress};

    fn key() -> SourceKey {
        SourceKey {
            source: "logs".to_string(),
            place: "bucket/prefix".to_string(),
        }
    }

    /// A source is read where the deployed commit, `main`'s head, or a commit a worker still runs reads it, and no
    /// other place's progress is kept for it.
    #[test]
    fn a_source_is_read_where_a_kept_bundle_reads_it() {
        let data = std::env::temp_dir().join(format!("smeltr-read-sources-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let read_at = |prefix: &str| {
            let head = git::main_head(&repo).unwrap();
            let edits = [
                ("groups/g/system.json".to_string(), br#"{"version":1}"#.to_vec()),
                (
                    "groups/g/sources.json".to_string(),
                    format!(r#"[{{"id":"s1","type":"s3List","bucket":"logs","prefix":"{prefix}"}}]"#).into_bytes(),
                ),
            ]
            .map(|(path, content)| git::FileEdit {
                path,
                content: Some(content),
            });
            match git::commit_edits(
                &repo,
                git::MAIN_BRANCH,
                &head,
                Some(&head),
                &edits,
                "read",
                &store::Actor::system(),
            )
            .unwrap()
            {
                git::Committed::At(commit) => commit,
                _ => panic!("the bundle did not land"),
            }
        };
        let older = read_at("a/");
        let newer = read_at("b/");
        git::deploy_commit(&data, &newer, &store::Actor::system()).unwrap();
        let at = |prefix: &str| {
            (
                "g".to_string(),
                SourceKey {
                    source: "s1".to_string(),
                    place: format!("s3://logs/{prefix}"),
                },
            )
        };

        let read = read_sources(&data, &Default::default()).unwrap();
        assert_eq!(read.single, [at("b/")].into());
        assert_eq!(read.all, Some([("g".to_string(), "s1".to_string())].into()));
        // A worker of a group no commit has a bundle for runs an empty deployment.
        let running = [("g".to_string(), older), ("unauthored".to_string(), newer)].into();
        assert_eq!(
            read_sources(&data, &running).unwrap().single,
            [at("a/"), at("b/")].into()
        );
        let unreadable = [("g".to_string(), "0".repeat(40))].into();
        assert!(
            read_sources(&data, &unreadable).is_err(),
            "a running bundle that cannot be read may read anything, so no progress may be forgotten"
        );
    }

    async fn answer_as(state: &AppState, worker: &str, req: HeartbeatRequest) -> crate::heartbeat::HeartbeatResponse {
        heartbeat(
            State(state.clone()),
            Extension(AuthenticatedWorker(worker.to_string())),
            Extension(store::WorkerKind::Permanent),
            Json(HeartbeatRequest {
                worker_id: worker.to_string(),
                ..req
            }),
        )
        .await
        .expect("the heartbeat is answered")
        .0
    }

    async fn fetch(state: &AppState, worker: &str) -> (StatusCode, Option<String>, String) {
        let response = ledger(
            State(state.clone()),
            Extension(AuthenticatedWorker(worker.to_string())),
            Query(key()),
        )
        .await;
        let status = response.status();
        let kind = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_string());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, kind, String::from_utf8(body.to_vec()).unwrap())
    }

    /// The holder alone can fetch its source's ledger.
    #[tokio::test]
    async fn the_holder_fetches_its_ledger_and_nobody_else_does() {
        let state = state("ledger-route");
        let entries: Vec<LedgerEntry> = (0..LEDGER_PAGE + 2)
            .map(|i| LedgerEntry {
                item: format!("obj-{i:05}"),
                entry: format!("etag-{i}"),
            })
            .collect();
        let first = answer_as(
            &state,
            "w1",
            HeartbeatRequest {
                single_worker: vec![key()],
                ..beat("g", vec![])
            },
        )
        .await;
        assert_eq!(first.holds.len(), 1);
        assert_eq!(first.holds[0].cursor, None);

        answer_as(
            &state,
            "w1",
            HeartbeatRequest {
                single_worker: vec![key()],
                progress: vec![SourceProgress {
                    key: key(),
                    progress: crate::io::Progress {
                        cursor: None,
                        added: entries.clone(),
                        removed: Vec::new(),
                    },
                }],
                ..beat("g", vec![])
            },
        )
        .await;

        let (status, kind, body) = fetch(&state, "w1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(kind.as_deref(), Some("application/x-ndjson"));
        let fetched: Vec<LedgerEntry> = body.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(fetched, entries, "every page, in item order");

        let other = answer_as(
            &state,
            "w2",
            HeartbeatRequest {
                single_worker: vec![key()],
                ..beat("g", vec![])
            },
        )
        .await;
        assert!(other.holds.is_empty(), "the source stays with its live holder");
        assert_eq!(fetch(&state, "w2").await.0, StatusCode::NOT_FOUND);
    }

    /// A beat whose progress cannot be recorded is refused, so the worker keeps it and sends it again.
    #[tokio::test]
    async fn a_beat_the_store_cannot_settle_is_refused() {
        let state = AppState {
            store: Arc::new(holds::store_without_holds("refused")),
            ..state("holds-refused")
        };
        let refused = heartbeat(
            State(state),
            Extension(AuthenticatedWorker("w1".to_string())),
            Extension(store::WorkerKind::Permanent),
            Json(HeartbeatRequest {
                single_worker: vec![key()],
                ..beat("g", vec![])
            }),
        )
        .await;
        assert_eq!(
            refused.err().map(|(status, _)| status),
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
    }

    /// A revoked holder frees its sources at once: it will never report their progress again.
    #[tokio::test]
    async fn revoking_the_holder_frees_its_sources() {
        let state = state("ledger-revoked");
        state.store.store_worker_credential("w1", "hash-w1").unwrap();
        let runs = || HeartbeatRequest {
            single_worker: vec![key()],
            ..beat("g", vec![])
        };
        assert_eq!(answer_as(&state, "w1", runs()).await.holds.len(), 1);
        assert!(answer_as(&state, "w2", runs()).await.holds.is_empty());
        assert!(revoke_worker(State(state.clone()), Path("w1".to_string())).await.0.ok);
        assert_eq!(answer_as(&state, "w2", runs()).await.holds.len(), 1);
        assert_eq!(fetch(&state, "w1").await.0, StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod certificate_list_tests {
    use super::*;
    use crate::leader::sqlite_store::SqliteStore;

    const A_YEAR: Duration = Duration::from_secs(365 * 24 * 60 * 60);

    fn names() -> Vec<String> {
        vec!["localhost".to_string(), "127.0.0.1".to_string()]
    }

    /// A leader's TLS directory serving its own authority's certificate.
    fn own(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-certificate-list-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let tls_dir = dir.join("tls");
        tls::ensure(&tls_dir, names(), A_YEAR, &[]).expect("an authority");
        tls_dir
    }

    fn certificates(store: &dyn Store) -> Vec<String> {
        let all = store.list_secrets().unwrap().into_iter();
        all.filter(|r| r.certificate).map(|r| r.name).collect()
    }

    /// An upload from before the list is listed once, whole enough to be chosen and served again.
    #[test]
    fn an_uploaded_certificate_is_listed_once() {
        let dir = own("uploaded");
        let theirs = tls::ensure(&own("theirs"), names(), A_YEAR, &[]).unwrap();
        let chain = std::fs::read(&theirs.server_cert_path).unwrap();
        let key = std::fs::read(&theirs.server_key_path).unwrap();
        tls::install(&dir, &chain, &key, &theirs.ca_pem, &names(), &[]).expect("an upload");
        let store = SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("uploaded")).unwrap();

        list_uploaded_certificate(&store, &dir).unwrap();
        list_uploaded_certificate(&store, &dir).unwrap();

        assert_eq!(certificates(&store), [UPLOADED_BEFORE_THE_LIST]);
        let (listed, listed_key) = newest_certificate(&store, UPLOADED_BEFORE_THE_LIST).unwrap().unwrap();
        assert_eq!(listed_key.as_bytes(), key);
        let (served, root) = tls::split_root(listed.as_bytes(), b"")
            .unwrap()
            .expect("the chain ends at its root");
        assert_eq!(served, chain);
        assert_eq!(root.as_bytes(), theirs.ca_pem);
    }

    #[test]
    fn the_leaders_own_certificate_is_not_listed() {
        let dir = own("own");
        let store = SqliteStore::open(std::path::Path::new(":memory:"), &test_secret_key("own")).unwrap();
        list_uploaded_certificate(&store, &dir).unwrap();
        assert!(certificates(&store).is_empty());
    }
}
