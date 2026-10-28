//! What the leader serves TLS with, and what a worker must be given to verify it.
//!
//! Behind the gate. Replacing what a leader serves is the one operator action that can put the leader out of reach
//! of the browser performing it.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};

use smeltr::leader::envelope::Refusal;
use smeltr::leader::resolver::Resolver;
use smeltr::leader::store::Actor;
use smeltr::leader::{CertificateRequest, NewCertificate, accounts, acme, blocking, tls};

use crate::gate::{Auth, refuse};
use crate::users_api::no_store;

/// The live TLS material: where it lives, what is serving it, and what a certificate written here must carry.
pub struct Tls {
    pub dir: PathBuf,
    /// What the socket presents. A swap here reaches every handshake after it and no connection already open,
    /// which is why replacing a certificate does not end the session that replaced it.
    pub resolver: Arc<Resolver>,
    /// Where a certificate authority's proof goes while an order is validated (`leader::acme`).
    pub proving: Arc<acme::Proving>,
    pub names: Vec<String>,
    /// The name the worker inside this process reaches the leader by, where one runs. A supplied certificate has
    /// to serve it, not merely one of `names`.
    pub from_inside: Option<String>,
    pub validity: Duration,
    /// Enough of the leader to finish a rotation. A rotation ends on a fact workers report, and the heartbeat is
    /// where that arrives -- but a leader whose workers have all gone quiet receives none, and this page is the
    /// one place somebody is still watching for the ending it was promised.
    pub leader: smeltr::leader::AppState,
}

#[derive(Clone)]
pub struct Certificates {
    auth: Auth,
    /// `None` where this leader serves plain HTTP, behind a proxy that terminates TLS. The routes stay registered
    /// so the page can say so: a tab that answers "there is nothing here, and why" beats one that 404s.
    tls: Option<Arc<Tls>>,
    /// What stores a certificate in the list, whether or not this leader serves TLS.
    leader: smeltr::leader::AppState,
}

/// The certificates routes, gated like every other operator surface. `None` where this leader serves plain HTTP:
/// there is no certificate to show and nothing a worker would verify.
pub fn router(auth: Auth, tls: Option<Arc<Tls>>, leader: smeltr::leader::AppState) -> axum::Router {
    let state = Certificates {
        auth: auth.clone(),
        tls,
        leader,
    };
    axum::Router::new()
        .route("/api/settings/tls", get(show).put(install))
        .route("/api/certificates", get(list).post(create))
        .route("/api/certificates/:name", put(replace))
        .route(
            "/api/settings/tls/rotation",
            post(begin_rotation).delete(abandon_rotation),
        )
        .route("/api/settings/tls/rotation/cutover", post(cut_over))
        .route("/api/settings/tls/rotation/deadline", put(move_deadline))
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            crate::gate::require_principal,
        ))
        .with_state(state)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CertificatesDto {
    /// Whether this leader terminates TLS itself. False behind a proxy, where the certificate a browser checked
    /// belongs to the proxy and this leader holds none.
    serving_tls: bool,
    /// What a worker must be given to verify this leader.
    authority: Option<tls::Described>,
    /// What a client is shown.
    served: Option<tls::Described>,
    /// Whether an operator supplied what is being served. False means it is this leader's own entry, which it
    /// renews itself; true means replacing it is theirs to do.
    supplied: bool,
    /// Where it came from: this leader's own authority, an upload, or an order from a certificate authority.
    source: acme::Source,
    /// The list entry an upload was copied from, where one is recorded.
    entry: Option<String>,
    /// The names this leader answers to. A supplied certificate covers at least one.
    names: Vec<String>,
    /// The name the worker inside this leader reaches it by, where one runs. A supplied certificate covers it.
    worker_name: Option<String>,
    /// Set when a certificate reached the disk and the socket refused it. Everything above is then what `dir`
    /// holds rather than what a client is shown, and only a restart closes the gap.
    restart_required: bool,
    /// Set while the list entry above is not what clients are shown: a start could not serve it, and serves this
    /// leader's own certificate until one is served again (`tls::unserved`). Says why, and what to do.
    unserved: Option<String>,
    /// The authority this leader is rotating to, while it is doing so. Workers are being given it beside the one
    /// above; nothing is signed under it until they have.
    rotation: Option<RotationDto>,
}

/// An open rotation, as the page shows it.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RotationDto {
    /// The authority being moved to, which every worker must hold before this leader signs under it.
    incoming: tls::Described,
    started_millis: i64,
    /// When this leader signs under it whatever the fleet holds.
    deadline_millis: i64,
    /// The latest the deadline can move to.
    latest_deadline_millis: i64,
    /// What the cutover puts in force.
    installs: tls::Installs,
    /// Where that comes from.
    source: acme::Source,
    /// Workers that verify this leader by an authority their operator gave them, and so take none from it. The
    /// cutover does not wait for them, and they keep working only while what they hold vouches for what is
    /// served.
    not_following: Vec<String>,
    /// Each worker the cutover waits for.
    workers: Vec<RotationWorkerDto>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RotationWorkerDto {
    id: String,
    holds_incoming: bool,
}

async fn read(tls: Option<&Arc<Tls>>) -> Result<CertificatesDto, String> {
    let Some(serving) = tls else {
        return Ok(CertificatesDto {
            serving_tls: false,
            authority: None,
            served: None,
            supplied: false,
            source: acme::Source::Own,
            entry: None,
            names: Vec::new(),
            worker_name: None,
            restart_required: false,
            unserved: None,
            rotation: None,
        });
    };
    let materials = tls::current(&serving.dir)?;
    let served = materials.served;
    let rotation = tls::rotation(&serving.dir)?;
    let unserved = tls::unserved(&serving.dir);
    let workers = match &rotation {
        Some(state) => smeltr::leader::rotation_workers(&serving.leader, &state.incoming)
            .await?
            .into_iter()
            .map(|(id, holds_incoming)| RotationWorkerDto { id, holds_incoming })
            .collect(),
        None => Vec::new(),
    };
    Ok(CertificatesDto {
        serving_tls: true,
        // The root that authenticates this leader, of however many the file holds. Showing the first would give an
        // operator mid-rollover the outgoing root's expiry and fingerprint -- the wrong urgency, and the wrong
        // thing to hand to a worker.
        authority: Some(tls::signing_root(&materials.ca_pem, &served)?),
        served: Some(tls::describe(&served, "the certificate")?),
        supplied: materials.supplied,
        source: acme::source(&serving.dir)?,
        entry: tls::served(&serving.dir)?.map(|entry| entry.name),
        names: serving.names.clone(),
        worker_name: serving.from_inside.clone(),
        restart_required: !tls::socket_has_what_is_on_disk(&serving.dir) && unserved.is_none(),
        unserved,
        rotation: rotation
            .map(|state| {
                Ok::<_, String>(RotationDto {
                    incoming: tls::describe(state.incoming_pem.as_bytes(), "the incoming authority")?,
                    started_millis: state.started_millis,
                    deadline_millis: state.deadline_millis,
                    latest_deadline_millis: tls::latest_deadline(&serving.dir, &state)?,
                    installs: state.installs,
                    source: acme::incoming(&state),
                    not_following: serving.leader.registry.not_taking_authority(),
                    workers,
                })
            })
            .transpose()?,
    })
}

/// What this leader is serving TLS with, or why there is nothing here to act on.
fn serving(state: &Certificates) -> Result<Arc<Tls>, Box<Response>> {
    serving_tls(state.tls.as_ref())
}

pub(crate) fn serving_tls(tls: Option<&Arc<Tls>>) -> Result<Arc<Tls>, Box<Response>> {
    tls.cloned().ok_or_else(|| {
        Box::new(refuse(
            StatusCode::CONFLICT,
            "this leader does not serve TLS itself, so it holds no certificate to change. Whatever terminates TLS \
             in front of it is where its certificate lives.",
        ))
    })
}

/// `GET /api/settings/tls`: what is in force, read off the files rather than remembered.
async fn show(State(state): State<Certificates>) -> Response {
    // Before reading, so a rotation whose deadline has passed is over by the time this answers rather than one
    // refresh later. Cheap where nothing is rotating, which is almost always.
    if let Some(serving) = &state.tls {
        smeltr::leader::cut_over_if_due(&serving.leader, smeltr::leader::warnings::now_millis()).await;
    }
    answer(&state).await
}

/// What is in force, as every route here answers with.
async fn answer(state: &Certificates) -> Response {
    match read(state.tls.as_ref()).await {
        Ok(dto) => no_store(dto),
        Err(e) => refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The names this upload must serve, whatever else it carries.
///
/// One name in `names` verifying is enough to say a certificate belongs to this leader, and a public certificate
/// for `leader.example.com` cannot carry `localhost` at all. But a leader reached at several names has some that
/// nobody can be told to stop using: the worker inside this process, and the session performing the upload, which
/// a certificate dropping its name ends over the connection carrying it.
///
/// The uploading session's name only where this leader is configured to answer to it. Reached by a name it was
/// never told about, it is not serving that name today either, and refusing would block an upload that repairs
/// nothing.
pub(crate) fn must_serve(serving: &Tls, headers: &axum::http::HeaderMap) -> Vec<String> {
    let mut required: Vec<String> = serving.from_inside.iter().cloned().collect();
    // The `Host` header, not `X-Forwarded-Host`: a proxy that terminates TLS holds the certificate that matters,
    // and a header anyone can set is not a thing to check a certificate against.
    let arrived_over = headers
        .get(axum::http::header::HOST)
        .and_then(|host| host.to_str().ok())
        .and_then(smeltr::startup_config::split_authority)
        .map(|(host, _)| host);
    if let Some(host) = arrived_over
        && serving.names.iter().any(|name| name.eq_ignore_ascii_case(&host))
        && !required.iter().any(|name| name.eq_ignore_ascii_case(&host))
    {
        required.push(host);
    }
    required
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstallRequest {
    /// The name of the certificate in the list to serve. Its chain ends with its root where the platform does not
    /// hold that root.
    certificate: String,
}

/// `PUT /api/settings/tls`: serve a certificate from the list.
async fn install(
    State(state): State<Certificates>,
    actor: axum::Extension<Actor>,
    headers: axum::http::HeaderMap,
    Json(request): Json<InstallRequest>,
) -> Response {
    let serving = match serving(&state) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    // Held across the whole of it. Two administrators choosing at once would otherwise interleave their checks and
    // writes, and record one certificate against the other's choice.
    let _ordered = state.auth.settings_writes.lock().await;
    match serve_newest(&state, &actor, &serving, &headers, &request.certificate).await {
        Ok(()) => answer(&state).await,
        Err(refusal) => refusal.into_response(),
    }
}

/// A certificate that has passed everything serving it requires.
struct Checked {
    /// The root a worker is given for it, PEM.
    ca: String,
    required: Vec<String>,
    /// Whether it is under another root than the one every worker verifies by.
    rotates: bool,
    fingerprint: String,
}

/// Everything serving `certificate` has to satisfy, before a byte is written, so a refusal leaves this leader
/// serving exactly what it served before.
fn checked(serving: &Tls, headers: &axum::http::HeaderMap, certificate: &str, key: &str) -> Result<Checked, Refusal> {
    let (chain, ca) = match tls::split_root(certificate.as_bytes(), b"") {
        Ok(Some(found)) => found,
        Ok(None) => {
            return Err(Refusal::invalid(
                "the certificate's root is missing. End the chain with the root that issued it.",
            ));
        }
        Err(e) => return Err(Refusal::invalid(e)),
    };
    let key = key.as_bytes().to_vec();
    let required = must_serve(serving, headers);
    if let Err(e) = tls::check_supplied(&chain, &key, ca.as_bytes(), &serving.names, &required) {
        return Err(Refusal::invalid(e));
    }
    // A certificate under the root every worker already verifies by is served at once. One under another root
    // reaches the fleet the way a new authority does: published first, served once the fleet holds it.
    let rotates = tls::supplying_changes_root(&serving.dir, &chain, ca.as_bytes()).map_err(Refusal::unavailable)?;
    // One rotation at a time: an upload that would begin one over an open rotation is refused, where a renewal
    // changes no root and is served whatever is open.
    if rotates && let Err(e) = tls::rotation_may_begin(&serving.dir, tls::Installs::Supplied) {
        return Err(e.into());
    }
    let fingerprint = tls::describe(&chain, "the certificate")
        .map_err(Refusal::invalid)?
        .fingerprint;
    Ok(Checked {
        ca,
        required,
        rotates,
        fingerprint,
    })
}

/// Serves the newest version of the list entry `name`, at once under the root in force, else as the rotation a
/// certificate under another root begins. Under the settings lock.
async fn serve_newest(
    state: &Certificates,
    actor: &Actor,
    serving: &Arc<Tls>,
    headers: &axum::http::HeaderMap,
    name: &str,
) -> Result<(), Refusal> {
    let unavailable = |e: String| Refusal::unavailable(e);
    let store = state.auth.store.clone();
    let wanted = name.to_string();
    let (version, certificate, key) = blocking(move || smeltr::leader::newest_certificate(store.as_ref(), &wanted))
        .await
        .map_err(|e| unavailable(e.to_string()))?
        .ok_or_else(|| Refusal::not_found(format!("there is no certificate named {name:?}")))?;
    let Checked {
        ca,
        required,
        rotates,
        fingerprint,
    } = checked(serving, headers, &certificate, &key)?;
    let entry = tls::Entry {
        name: name.to_string(),
        version,
        ca,
    };
    // Recorded before anything is served, against what the certificate itself says it is: an audit row that cannot
    // be written stops the change rather than trailing it.
    //
    // A switch is recorded as the rotation it begins, against the root it rotates to: the certificate is served
    // only once the fleet holds that root, and the row for serving it would otherwise be written weeks early, or
    // for a switch that is then abandoned.
    let subject = if rotates {
        (
            "authority-rotation-begun",
            root_in_force(&serving.dir).map_err(unavailable)?,
        )
    } else {
        ("certificate-supplied", fingerprint)
    };
    record(&state.auth, actor, subject.0, &subject.1)
        .await
        .map_err(unavailable)?;

    if rotates {
        let now_ms = smeltr::leader::warnings::now_millis();
        // The socket is untouched: what is served has not moved, and the intent that did is what `/api/worker/authority`
        // and the heartbeat read afresh. A fleet with nobody in it to wait for cuts over on the next read.
        tls::begin_supplied_rotation(&serving.dir, &entry, &serving.names, &required, now_ms)
            .map_err(Refusal::conflict)?;
        return Ok(());
    }
    tls::serve(&serving.dir, &entry, &serving.names, &required).map_err(Refusal::invalid)?;
    reload_socket(serving).map_err(unavailable)
}

/// Keeps what this leader issues and orders up to date, for as long as it runs. Each hour, and at once after the
/// encryption key is regenerated ([`smeltr::leader::RENEW_NOW`]), it renews each certificate it issued that is due,
/// serving the new version where it serves that one, then orders over ACME where that is due.
/// A new version reaches the workers presenting it on their next heartbeat (`leader::heartbeat`).
pub async fn keep_renewed(auth: Auth, serving: Arc<Tls>, leader: smeltr::leader::AppState) {
    let state = Certificates {
        auth: auth.clone(),
        tls: Some(serving.clone()),
        leader,
    };
    loop {
        tokio::select! {
            _ = tokio::time::sleep(crate::acme_api::RENEWAL_TICK) => {}
            _ = smeltr::leader::RENEW_NOW.notified() => {}
        }
        renew_issued(&state, &serving).await;
        crate::acme_api::renew_if_due(&auth, &serving).await;
    }
}

/// One pass of [`keep_renewed`] over the certificates this leader issued.
async fn renew_issued(state: &Certificates, serving: &Arc<Tls>) {
    let _ordered = state.auth.settings_writes.lock().await;
    let now_ms = smeltr::leader::warnings::now_millis();
    let tried = match smeltr::leader::renew_issued(&state.leader, now_ms).await {
        Ok(tried) => tried,
        Err(e) => {
            tracing::warn!(target: "leader.certificate_renewal_failed", error = %e, "could not read the certificates to renew");
            return;
        }
    };
    let served = smeltr::leader::served_names(&serving.dir).unwrap_or_default();
    for (name, outcome) in tried {
        let outcome = match outcome {
            Ok(()) if served.contains(&name) => {
                (serve_newest(state, &Actor::system(), serving, &axum::http::HeaderMap::new(), &name).await)
                    .map_err(|refusal| refusal.message())
            }
            outcome => outcome,
        };
        match outcome {
            Ok(()) => {
                tracing::info!(target: "leader.certificate_renewed", certificate = %name, "renewed a certificate this leader issued")
            }
            Err(e) => tracing::warn!(
                target: "leader.certificate_renewal_failed",
                certificate = %name,
                error = %e,
                "could not renew a certificate this leader issued"
            ),
        }
    }
}

/// `GET /api/certificates`: the certificates in the list.
async fn list(State(state): State<Certificates>) -> Response {
    smeltr::leader::list_certificates(State(state.leader))
        .await
        .into_response()
}

/// `POST /api/certificates`: a certificate supplied with its key, or one this leader issues.
async fn create(State(state): State<Certificates>, request: Json<NewCertificate>) -> Response {
    smeltr::leader::create_certificate(State(state.leader), request)
        .await
        .into_response()
}

/// `PUT /api/certificates/:name`: a new version of a certificate in the list. Where the leader serves it, the new
/// version has to be one it can serve, and it is served once stored.
async fn replace(
    State(state): State<Certificates>,
    actor: axum::Extension<Actor>,
    headers: axum::http::HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<CertificateRequest>,
) -> Response {
    let store =
        |request| smeltr::leader::replace_certificate(State(state.leader.clone()), Path(name.clone()), Json(request));
    let Some(serving) = state.tls.clone() else {
        return store(request).await.into_response();
    };
    let _ordered = state.auth.settings_writes.lock().await;
    match smeltr::leader::served_names(&serving.dir) {
        Ok(served) if served.contains(&name) => {}
        Ok(_) => return store(request).await.into_response(),
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
    if let Err(refusal) = checked(&serving, &headers, &request.certificate, &request.private_key) {
        return refusal.into_response();
    }
    let stored = store(request).await;
    if !stored.succeeded() {
        return stored.into_response();
    }
    if let Err(refusal) = serve_newest(&state, &actor, &serving, &headers, &name).await {
        return refusal.into_response();
    }
    stored.into_response()
}

/// The audit row for a certificate change, written while the change is still undone: a row that did not land,
/// however it failed to, stops the change, since a certificate changed with the row saying who did it dropped in
/// silence is what the row is against.
pub(crate) async fn record(auth: &Auth, actor: &Actor, what: &str, subject: &str) -> Result<(), String> {
    let event = accounts::event_for(actor, what, "leader", subject);
    let store = auth.store.clone();
    let recorded = blocking(move || store.record_security_event(&event)).await;
    recorded.map_err(|e| format!("the audit row did not land, so nothing was changed: {e}"))
}

/// Swaps in whatever `tls/` now holds, and says what is in force.
///
/// A swap in `RustlsConfig` reaches every handshake after it and no connection already open, which is why
/// replacing a certificate does not end the session that replaced it.
/// How long an operator is willing to wait for the fleet, in hours, before this leader signs under the incoming
/// authority anyway.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RotateRequest {
    wait_hours: i64,
    /// The incoming authority's lifetime in whole days. It is explicit here:
    /// a rotation is the one time changing an authority's policy takes effect.
    authority_validity_days: Option<i64>,
}

/// At most a fortnight. A rotation held open indefinitely is one an operator forgets, and every hour of it is an
/// hour this leader's certificate is bounded by an authority it is trying to leave.
const MOST_WAIT_HOURS: i64 = 14 * 24;

/// The same guardrails as `startup_config::leader_ca_validity`, expressed in
/// the API's whole-day unit. A CA shorter than a day cannot be usefully
/// distributed; a century is already the configuration ceiling.
const MIN_AUTHORITY_VALIDITY_DAYS: i64 = 1;
const MAX_AUTHORITY_VALIDITY_DAYS: i64 = 100 * 365;

/// `POST /api/settings/tls/rotation`: mint the incoming authority and start the overlap.
///
/// Nothing a client sees changes here. The certificate this leader serves keeps its authority until the fleet has
/// had the new one long enough, which is what the wait is.
/// A refusal answered with the status that says whether asking again could change it.
fn refused(why: tls::Refused) -> Response {
    Refusal::from(why).into_response()
}

async fn begin_rotation(
    State(state): State<Certificates>,
    actor: axum::Extension<Actor>,
    Json(req): Json<RotateRequest>,
) -> Response {
    let serving = match serving(&state) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    let _ordered = state.auth.settings_writes.lock().await;
    if !(1..=MOST_WAIT_HOURS).contains(&req.wait_hours) {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "a rotation waits between an hour and {} days for the fleet.",
                MOST_WAIT_HOURS / 24
            ),
        );
    }
    if matches!(
        req.authority_validity_days,
        Some(days) if !(MIN_AUTHORITY_VALIDITY_DAYS..=MAX_AUTHORITY_VALIDITY_DAYS).contains(&days)
    ) {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "an incoming authority must be valid for between one day and one hundred years.",
        );
    }
    // What `begin_rotation` refuses for, asked before a row is written saying it happened. That call re-asks it
    // under the lock that makes the answer binding; this is only about not recording a rotation that never began.
    // Asked through the same function rather than restated here, because a copy of the list drifted once already.
    if let Err(e) = tls::rotation_may_begin(&serving.dir, tls::Installs::NextKey) {
        return refused(e);
    }
    let current = match root_in_force(&serving.dir) {
        Ok(root) => root,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    if let Err(e) = record(&state.auth, &actor, "authority-rotation-begun", &current).await {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    let now_ms = smeltr::leader::warnings::now_millis();
    let deadline = now_ms + req.wait_hours * 60 * 60 * 1000;
    // Older GUI builds did not send this field. Keep their established
    // configured lifetime rather than turning a UI addition into an API break.
    let authority_validity = req
        .authority_validity_days
        .map(|days| std::time::Duration::from_secs(days as u64 * 24 * 60 * 60))
        .unwrap_or(serving.validity);
    match tls::begin_rotation(&serving.dir, authority_validity, deadline, now_ms) {
        // The socket is untouched: what is served has not moved, and the file that did is the one `/api/worker/authority`
        // and the heartbeat read afresh.
        Ok(_) => answer(&state).await,
        Err(e) => refuse(StatusCode::CONFLICT, &e),
    }
}

/// `POST /api/settings/tls/rotation/cutover`: sign under the incoming authority now.
///
/// The operator's answer to a worker that is not coming back. Every worker without the incoming root stops
/// verifying this leader at this moment, which is why it is a button and not a timer.
async fn cut_over(State(state): State<Certificates>, actor: axum::Extension<Actor>) -> Response {
    let serving = match serving(&state) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    let _ordered = state.auth.settings_writes.lock().await;
    let incoming = match tls::rotation(&serving.dir) {
        Ok(Some(state)) => state.incoming,
        Ok(None) => {
            return refuse(
                StatusCode::CONFLICT,
                "this leader is not rotating its authority, so there is nothing to cut over to.",
            );
        }
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    // Every way `tls::cut_over` refuses, asked before the row saying it happened is written. A late press against
    // an incoming root with no life left is a refusal, and the log recording it as a cutover would be the one
    // account of what this leader did saying the opposite.
    if let Err(e) = tls::cut_over_is_possible(&serving.dir) {
        return refused(e);
    }
    if let Err(e) = record(&state.auth, &actor, "authority-cut-over", &incoming).await {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    if let Err(e) = tls::cut_over(&serving.dir, serving.names.clone(), serving.validity) {
        return refuse(StatusCode::CONFLICT, &e);
    }
    // The same withdrawal the deadline performs, through the same function: an enrollment code naming the root just
    // retired brings nobody in, and which of the two paths reached the cutover is not something it should depend on.
    smeltr::leader::enrollment_codes::withdraw_off_authority(&serving.leader, &actor).await;
    serve_what_is_on_disk(&state, &serving).await
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeadlineRequest {
    deadline_millis: i64,
}

/// `PUT /api/settings/tls/rotation/deadline`: move when the open rotation cuts over without the fleet.
async fn move_deadline(
    State(state): State<Certificates>,
    actor: axum::Extension<Actor>,
    Json(req): Json<DeadlineRequest>,
) -> Response {
    let serving = match serving(&state) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    let _ordered = state.auth.settings_writes.lock().await;
    let now_ms = smeltr::leader::warnings::now_millis();
    let incoming = match tls::deadline_may_move(&serving.dir, req.deadline_millis, now_ms) {
        Ok(rotation) => rotation.incoming,
        Err(e) => return refused(e),
    };
    if let Err(e) = record(&state.auth, &actor, "authority-cutover-moved", &incoming).await {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    if let Err(e) = tls::set_deadline(&serving.dir, req.deadline_millis, now_ms) {
        return refused(e);
    }
    answer(&state).await
}

/// `DELETE /api/settings/tls/rotation`: drop the incoming authority and keep serving what is served.
async fn abandon_rotation(State(state): State<Certificates>, actor: axum::Extension<Actor>) -> Response {
    let serving = match serving(&state) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    let _ordered = state.auth.settings_writes.lock().await;
    // Past the promotion there is nothing to go back to, and `abandon_rotation` says so. Asked before the row is
    // written, so a refused request does not leave the log claiming a rotation was abandoned.
    match tls::cut_over_began(&serving.dir) {
        Ok(true) => {
            return refuse(
                StatusCode::CONFLICT,
                "this leader already signs under the authority it was rotating to, so the rotation cannot be \
                 abandoned. Cut it over to finish it.",
            );
        }
        Ok(false) => {}
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
    // Nothing to record where nothing is being abandoned, and nothing to change either.
    let Some(rotation) = (match tls::rotation(&serving.dir) {
        Ok(rotation) => rotation,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }) else {
        return answer(&state).await;
    };
    let ordered = acme::incoming(&rotation) == acme::Source::Acme;
    if let Err(e) = record(&state.auth, &actor, "authority-rotation-abandoned", &rotation.incoming).await {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    // Named, because `settings_writes` orders these handlers against each other and not against the cutover a
    // heartbeat's deadline runs. Between the check above and here, that cutover can finish this very rotation --
    // and an abandonment reported for it would be the audit log describing the opposite of what happened.
    if let Err(e) = tls::abandon_rotation(&serving.dir, &rotation.incoming) {
        return refused(e);
    }
    if ordered && let Err(e) = acme::abandoned(&serving.dir) {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    answer(&state).await
}

/// The root this leader signs under now, which is what a rotation's audit row names: the authority being left.
/// Of however many the bundle holds, because after a rotation or two it leads with one kept for the workers still
/// verifying by it, and a row naming that one names the wrong authority.
pub(crate) fn root_in_force(dir: &std::path::Path) -> Result<String, String> {
    let materials = tls::current(dir)?;
    Ok(tls::signing_root(&materials.ca_pem, &materials.served)?.fingerprint)
}

async fn serve_what_is_on_disk(state: &Certificates, serving: &Arc<Tls>) -> Response {
    if let Err(e) = reload_socket(serving) {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    answer(state).await
}

/// Makes the socket present whatever `tls/` now holds.
///
/// A reload reaches every handshake after it and no connection already open, which is why replacing a certificate
/// does not end the session that replaced it.
pub(crate) fn reload_socket(serving: &Tls) -> Result<(), String> {
    // The state of the disk this reload is for, taken with the files themselves. A replacement landing while the
    // reload below runs leaves the socket on this one, which is what says the two have parted.
    let (generation, chain, key) = tls::to_serve(&serving.dir)?;
    serving.resolver.reload(&chain, &key).map_err(|e| {
        // Nothing recorded: the socket still presents whatever it had. From now on every read says so, because
        // the disk has moved past it -- a page or a downloaded `ca.pem` taken at face value would describe a
        // certificate nothing is being shown.
        format!("the certificate is on disk and this leader is still serving the previous one: {e}")
    })?;
    tls::socket_took(&serving.dir, generation);
    Ok(())
}

/// What is in force, for a route outside this module that changed it.
pub(crate) async fn answer_in_force(serving: &Arc<Tls>) -> Response {
    match read(Some(serving)).await {
        Ok(dto) => no_store(dto),
        Err(e) => refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}
