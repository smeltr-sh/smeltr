//! A certificate ordered from a certificate authority over ACME, the settings that name the authority, and the
//! renewal that keeps it current.
//!
//! Behind the gate, beside the certificates routes: an ordered certificate is served the way an uploaded one is,
//! and the certificates page is where it shows.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{Actor, SecretSave};
use smeltr::leader::{accounts, acme, acme_dns, blocking, tls};

use crate::certificates_api::{self, Tls};
use crate::gate::{Auth, refuse};

/// The one order a leader runs at a time, and the step it is on. An order runs on its own, past the request that
/// began it: a page reads where it is from the settings it reads anyway.
#[derive(Default)]
pub struct Ordering {
    one: Arc<tokio::sync::Mutex<()>>,
    progress: Arc<acme::Progress>,
}

#[derive(Clone)]
pub struct Acme {
    auth: Auth,
    tls: Option<Arc<Tls>>,
}

pub fn router(auth: Auth, tls: Option<Arc<Tls>>) -> axum::Router {
    let state = Acme {
        auth: auth.clone(),
        tls,
    };
    axum::Router::new()
        .route("/api/settings/tls/acme", get(show).put(put_settings))
        .route("/api/settings/tls/acme/order", post(order))
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            crate::gate::require_principal,
        ))
        .with_state(state)
}

/// The settings, and what the last order left.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct AcmeDto {
    enabled: bool,
    directory: String,
    contact: String,
    names: Vec<String>,
    challenge: acme::Challenge,
    authority: String,
    eab_key_id: String,
    /// Whether an external account HMAC key is stored. The key itself is never sent back.
    eab_hmac_set: bool,
    dns_provider: acme_dns::Kind,
    /// Whether a Cloudflare API token is stored. The token itself is never sent back.
    cloudflare_token_set: bool,
    /// Where an authority's HTTP-01 challenge is answered, for a page saying what has to reach this leader.
    http_port: u16,
    /// The dates of the certificate the last order stored.
    not_after_millis: Option<i64>,
    renew_after_millis: Option<i64>,
    last_attempt_millis: Option<i64>,
    last_error: Option<String>,
    /// How long an order may take before it is given up on, by challenge, for a page saying how long to wait.
    order_within_secs: std::collections::BTreeMap<&'static str, u64>,
    /// The order running now, if one is.
    ordering: Option<OrderingDto>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderingDto {
    step: acme::Step,
    since_millis: i64,
}

async fn settings(auth: &Auth) -> Result<acme::Settings, String> {
    let store = auth.store.clone();
    blocking(move || acme::Settings::read(store.as_ref()))
        .await
        .map_err(|e| e.to_string())
}

async fn read(state: &Acme) -> Result<AcmeDto, String> {
    let settings = settings(&state.auth).await?;
    let issued = match &state.tls {
        Some(serving) => acme::state(&serving.dir)?,
        None => acme::State::default(),
    };
    Ok(AcmeDto {
        enabled: settings.enabled,
        directory: settings.directory,
        contact: settings.contact,
        names: settings.names,
        challenge: settings.challenge,
        authority: settings.authority,
        eab_key_id: settings.eab_key_id,
        eab_hmac_set: settings.eab_hmac.is_some(),
        dns_provider: settings.dns_provider,
        cloudflare_token_set: settings.cloudflare_token.is_some(),
        http_port: smeltr::startup_config::acme_http_port(),
        not_after_millis: issued.not_after_millis,
        renew_after_millis: issued.renew_after_millis,
        last_attempt_millis: issued.last_attempt_millis,
        last_error: issued.last_error.map(|e| e.to_string()),
        order_within_secs: acme::Challenge::ALL
            .iter()
            .map(|c| (c.as_str(), c.order_within().as_secs()))
            .collect(),
        ordering: state
            .tls
            .as_ref()
            .and_then(|serving| serving.ordering.progress.now())
            .map(|(step, since_millis)| OrderingDto { step, since_millis }),
    })
}

async fn answer(state: &Acme) -> Response {
    match read(state).await {
        Ok(dto) => Envelope::ok(dto).into_response(),
        Err(e) => refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// `GET /api/settings/tls/acme`.
async fn show(State(state): State<Acme>) -> Response {
    answer(&state).await
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PutSettings {
    enabled: bool,
    directory: String,
    #[serde(default)]
    contact: String,
    #[serde(default)]
    names: Vec<String>,
    challenge: acme::Challenge,
    #[serde(default)]
    authority: String,
    #[serde(default)]
    eab_key_id: String,
    /// `None` keeps the stored key; empty removes it.
    eab_hmac: Option<String>,
    /// `None` keeps the stored provider.
    #[serde(default)]
    dns_provider: Option<acme_dns::Kind>,
    /// `None` keeps the stored token; empty removes it.
    #[serde(default)]
    cloudflare_token: Option<String>,
}

/// The most a directory URL, a contact or a name may be; an authority bundle is bounded apart.
const MOST_TEXT: usize = 1024;
const MOST_AUTHORITY: usize = 64 * 1024;

/// `PUT /api/settings/tls/acme`. Recorded with who set it and what changed, in the transaction that sets it.
async fn put_settings(
    State(state): State<Acme>,
    actor: axum::Extension<Actor>,
    Json(request): Json<PutSettings>,
) -> Response {
    let directory = request.directory.trim().to_string();
    let contact = request.contact.trim().to_string();
    let names: Vec<String> = request
        .names
        .iter()
        .map(|n| n.trim().to_ascii_lowercase())
        .filter(|n| !n.is_empty())
        .collect();
    let authority = request.authority.trim().to_string();
    let eab_key_id = request.eab_key_id.trim().to_string();

    if [&directory, &contact, &eab_key_id]
        .iter()
        .chain(names.iter().collect::<Vec<_>>().iter())
        .any(|text| text.len() > MOST_TEXT)
    {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("a directory URL, contact, name or key id is at most {MOST_TEXT} bytes"),
        );
    }
    if authority.len() > MOST_AUTHORITY {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("the authority is at most {} KiB", MOST_AUTHORITY / 1024),
        );
    }
    if request.eab_hmac.as_ref().is_some_and(|k| k.len() > MOST_TEXT) {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "an external account key is at most 1024 bytes",
        );
    }
    if request.cloudflare_token.as_ref().is_some_and(|k| k.len() > MOST_TEXT) {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "an API token is at most 1024 bytes");
    }
    if request.challenge == acme::Challenge::Dns01
        && let Some(ip) = names.iter().find(|name| name.parse::<std::net::IpAddr>().is_ok())
    {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("DNS-01 cannot prove an IP address: {ip}"),
        );
    }
    if !directory.is_empty() && !directory.starts_with("https://") {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "the directory is an https:// URL");
    }
    if names
        .iter()
        .any(|name| name.contains(',') || name.contains(char::is_whitespace))
    {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, "a name carries no comma or space");
    }
    if request.enabled
        && let Err(e) = acme::orderable(&directory, &names)
    {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e);
    }
    if !authority.is_empty()
        && let Err(e) = tls::describe_each(authority.as_bytes(), "the authority")
    {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e);
    }

    let _ordered = state.auth.settings_writes.lock().await;
    let was = match settings(&state.auth).await {
        Ok(was) => was,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let moved = |new: Option<&str>, was: Option<&str>| match new {
        None => false,
        Some("") => was.is_some(),
        Some(new) => was != Some(new),
    };
    let key_moved = moved(request.eab_hmac.as_deref(), was.eab_hmac.as_deref());
    let token_moved = moved(request.cloudflare_token.as_deref(), was.cloudflare_token.as_deref());
    let dns_provider = request.dns_provider.unwrap_or(was.dns_provider);
    let token_held = match request.cloudflare_token.as_deref() {
        None => was.cloudflare_token.is_some(),
        Some(token) => !token.is_empty(),
    };
    if request.enabled
        && request.challenge == acme::Challenge::Dns01
        && dns_provider == acme_dns::Kind::Cloudflare
        && !token_held
    {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "DNS-01 through Cloudflare needs an API token",
        );
    }
    let changed: Vec<&str> = [
        (request.enabled && !was.enabled, "turned on"),
        (!request.enabled && was.enabled, "turned off"),
        (directory != was.directory, "directory"),
        (contact != was.contact, "contact"),
        (names != was.names, "names"),
        (request.challenge != was.challenge, "challenge"),
        (authority != was.authority, "authority"),
        (eab_key_id != was.eab_key_id, "external account key id"),
        (key_moved, "external account key"),
        (dns_provider != was.dns_provider, "DNS provider"),
        (token_moved, "Cloudflare API token"),
    ]
    .into_iter()
    .filter_map(|(moved, name)| moved.then_some(name))
    .collect();
    let what = match changed.is_empty() {
        true => "nothing".to_string(),
        false => changed.join(", "),
    };
    let event = accounts::event_for(&actor, "acme-change", &what, "ok");
    let store = state.auth.store.clone();
    let challenge = request.challenge;
    let names_joined = names.join(",");
    let written = blocking(move || {
        let mut entries = vec![
            (acme::ENABLED_KEY, request.enabled.to_string()),
            (acme::DIRECTORY_KEY, directory),
            (acme::CONTACT_KEY, contact),
            (acme::NAMES_KEY, names_joined),
            (acme::CHALLENGE_KEY, challenge.as_str().to_string()),
            (acme::AUTHORITY_KEY, authority),
            (acme::EAB_KEY_ID_KEY, eab_key_id),
            (acme::DNS_PROVIDER_KEY, dns_provider.as_str().to_string()),
        ];
        for (key, value) in [
            (acme::EAB_HMAC_KEY, request.eab_hmac),
            (acme::CLOUDFLARE_TOKEN_KEY, request.cloudflare_token),
        ] {
            entries.extend(SecretSave::sent(value, false).entry(store.as_ref(), key)?);
        }
        store.put_settings(&entries, Some(&event))
    })
    .await;
    if let Err(e) = written {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e.to_string());
    }
    answer(&state).await
}

/// `POST /api/settings/tls/acme/order`: begin ordering a certificate, served once issued, and answer the settings
/// with the step it is on. Where an order already runs, that one goes on and nothing else begins.
///
/// What a renewal does on its own, begun while somebody watches: the failure an authority answers with is theirs
/// to read, as the last error the settings carry.
async fn order(State(state): State<Acme>, actor: axum::Extension<Actor>, headers: axum::http::HeaderMap) -> Response {
    let serving = match certificates_api::serving_tls(state.tls.as_ref()) {
        Ok(serving) => serving.clone(),
        Err(response) => return *response,
    };
    let settings = match settings(&state.auth).await {
        Ok(settings) => settings,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    if !settings.enabled {
        return refuse(StatusCode::CONFLICT, "ACME is turned off. Turn it on before ordering.");
    }
    if let Err(e) = acme::orderable(&settings.directory, &settings.names) {
        return refuse(StatusCode::UNPROCESSABLE_ENTITY, &e);
    }
    let Ok(one) = serving.ordering.one.clone().try_lock_owned() else {
        return refuse(StatusCode::CONFLICT, ALREADY_ORDERING);
    };
    let required = certificates_api::must_serve(&serving, &headers);
    // Said before the answer, so the page it goes to shows the order running.
    serving.ordering.progress.set(acme::Step::Account);
    let auth = state.auth.clone();
    let serving = serving.clone();
    tokio::spawn(async move {
        let _one = one;
        let _ = run(&auth, &serving, &settings, &actor, &required, true).await;
    });
    answer(&state).await
}

const ALREADY_ORDERING: &str = "A certificate is already being ordered. Wait for that order to finish.";
const TURNED_OFF: &str = "ACME was turned off while this order ran, so its certificate was not installed.";
const SETTINGS_MOVED: &str = "The names, directory or challenge changed while this order ran, so its certificate \
                              was not installed. Get a certificate again.";

/// Clears the step said when it is dropped, however the order ended, a panic included.
struct Cleared<'a>(&'a acme::Progress);

impl Drop for Cleared<'_> {
    fn drop(&mut self) {
        self.0.clear();
    }
}

/// `settings_writes`, saying so when it has to wait: an order writes only once issued, under the lock every
/// change to the settings takes.
async fn settings_writes<'a>(auth: &'a Auth, progress: &acme::Progress) -> tokio::sync::MutexGuard<'a, ()> {
    match auth.settings_writes.try_lock() {
        Ok(held) => held,
        Err(_) => {
            progress.set(acme::Step::Queued);
            tracing::info!(target: "leader.acme_queued", "waiting for another change to the settings to finish");
            auth.settings_writes.lock().await
        }
    }
}

/// One order, start to finish, under `serving.ordering`'s one at a time: ordered, then under `settings_writes`
/// recorded, stored in the list, and where `serve` says to, served or rotated to, with the socket reloaded where it
/// is served at once. A failure is recorded in the state the page reads. Its steps are said as it goes.
async fn run(
    auth: &Auth,
    serving: &Arc<Tls>,
    settings: &acme::Settings,
    actor: &Actor,
    required: &[String],
    serve: bool,
) -> Result<(), String> {
    let now_ms = smeltr::leader::warnings::now_millis();
    tracing::info!(target: "leader.acme_order_started", names = %settings.names.join(","), challenge = settings.challenge.as_str(), "ordering a certificate");
    let progress = &serving.ordering.progress;
    let _cleared = Cleared(progress);
    progress.set(acme::Step::Account);
    let mut outcome = order_once(auth, serving, settings, actor, required, serve, now_ms).await;
    if outcome.is_ok() {
        tracing::info!(target: "leader.acme_order_issued", names = %settings.names.join(","), "the certificate authority issued a certificate");
    }
    if let Err(e) = &outcome {
        let _writes = settings_writes(auth, progress).await;
        if let Err(e) = acme::failed(&serving.dir, now_ms, &settings.directory, e) {
            outcome = Err(e);
        }
    }
    outcome
}

async fn order_once(
    auth: &Auth,
    serving: &Arc<Tls>,
    settings: &acme::Settings,
    actor: &Actor,
    required: &[String],
    serve: bool,
    now_ms: i64,
) -> Result<(), String> {
    let progress = &serving.ordering.progress;
    let issuer = acme::Directory::new(settings.clone(), &serving.dir).reporting(progress.clone());
    let publishing;
    let prover: &dyn acme::Prover = match settings.challenge {
        acme::Challenge::Dns01 => {
            publishing = acme_dns::publishing(settings)?.reporting(progress.clone());
            &publishing
        }
        acme::Challenge::TlsAlpn01 | acme::Challenge::Http01 => serving.proving.as_ref(),
    };
    let mut issued = acme::order(&serving.dir, settings, &issuer, prover, &serving.names, required).await?;
    // What is served may have moved while the authority took its time; asked again where nothing else moves it.
    let _writes = settings_writes(auth, progress).await;
    progress.set(acme::Step::Installing);
    let now = self::settings(auth).await?;
    if !now.enabled {
        return Err(TURNED_OFF.to_string());
    }
    if (&now.names, &now.directory, now.challenge) != (&settings.names, &settings.directory, settings.challenge) {
        return Err(SETTINGS_MOVED.to_string());
    }
    acme::against_served(&serving.dir, &mut issued)?;
    issued.rotates &= serve;
    // Recorded before anything is served, as an upload is: a switch of root as the rotation it begins, against
    // the root being left; a certificate served at once against itself.
    let (what, subject) = if issued.rotates {
        (
            "authority-rotation-begun",
            certificates_api::root_in_force(&serving.dir)?,
        )
    } else {
        ("certificate-ordered", issued.certificate.fingerprint.clone())
    };
    certificates_api::record(auth, actor, what, &subject).await?;
    let store = auth.store.clone();
    let listed = issued.clone();
    let entry = tokio::task::spawn_blocking(move || acme::list(store.as_ref(), &listed))
        .await
        .map_err(|e| e.to_string())??;
    // At once under the root in force, else as the rotation a certificate under another root begins.
    if issued.rotates {
        tls::begin_supplied_rotation(&serving.dir, &entry, &serving.names, required, now_ms)?;
    } else if serve {
        tls::serve(&serving.dir, &entry, &serving.names, required)?;
    }
    acme::succeeded(&serving.dir, settings, &issued, now_ms)?;
    if serve && !issued.rotates {
        certificates_api::reload_socket(serving)?;
    }
    Ok(())
}

/// How often a renewal is looked for. A certificate is renewed two thirds through its life, which is weeks away
/// for any authority's; an hour is for the retry after a failure.
pub(crate) const RENEWAL_TICK: Duration = Duration::from_secs(60 * 60);

/// Orders a renewal where one is due. Nothing while ordering is off, or while a renewal would have to wait for a
/// rotation to finish.
pub(crate) async fn renew_if_due(auth: &Auth, serving: &Arc<Tls>) {
    let settings = match settings(auth).await {
        Ok(settings) => settings,
        Err(e) => {
            tracing::warn!(target: "leader.acme_settings_unreadable", error = %e, "could not read the ACME settings");
            return;
        }
    };
    if !settings.enabled || acme::orderable(&settings.directory, &settings.names).is_err() {
        return;
    }
    // An order somebody began is the renewal, or will be retried at the next tick.
    let Ok(_one) = serving.ordering.one.clone().try_lock_owned() else {
        return;
    };
    let now_ms = smeltr::leader::warnings::now_millis();
    let (due, served) = match (acme::state(&serving.dir), acme::source(&serving.dir)) {
        (Ok(state), Ok(source)) => (acme::renews(&state, now_ms), source == acme::Source::Acme),
        (Err(e), _) | (_, Err(e)) => {
            tracing::warn!(target: "leader.acme_state_unreadable", error = %e, "could not read what the last order left");
            return;
        }
    };
    // An order a regenerated key cannot open is ordered again at once, as an issued certificate is issued again.
    let store = auth.store.clone();
    let unopenable = blocking(move || store.key_standing()).await.is_ok_and(|standing| {
        (standing.unopenable).contains(&smeltr::leader::store::Unopenable::Certificate(acme::ENTRY.to_string()))
    });
    if !due && !unopenable {
        return;
    }
    let required: Vec<String> = serving.from_inside.iter().cloned().collect();
    let _ = run(auth, serving, &settings, &Actor::system(), &required, served).await;
}
