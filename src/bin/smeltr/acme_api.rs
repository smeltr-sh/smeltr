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
use axum::response::Response;
use axum::routing::{get, post};

use smeltr::leader::store::Actor;
use smeltr::leader::{accounts, acme, blocking, tls};

use crate::certificates_api::{self, Tls};
use crate::gate::{Auth, refuse};
use crate::users_api::no_store;

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
        .route("/api/settings/acme", get(show).put(put_settings))
        .route("/api/settings/acme/order", post(order))
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
    /// Where an authority's HTTP-01 challenge is answered, for a page saying what has to reach this leader.
    http_port: u16,
    /// The certificate the last order installed, by fingerprint, and its dates.
    issued: Option<String>,
    not_after_millis: Option<i64>,
    renew_after_millis: Option<i64>,
    last_attempt_millis: Option<i64>,
    last_error: Option<String>,
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
        http_port: smeltr::startup_config::acme_http_port(),
        issued: issued.fingerprint,
        not_after_millis: issued.not_after_millis,
        renew_after_millis: issued.renew_after_millis,
        last_attempt_millis: issued.last_attempt_millis,
        last_error: issued.last_error,
    })
}

async fn answer(state: &Acme) -> Response {
    match read(state).await {
        Ok(dto) => no_store(dto),
        Err(e) => refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// `GET /api/settings/acme`.
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
}

/// The most a directory URL, a contact or a name may be; an authority bundle is bounded apart.
const MOST_TEXT: usize = 1024;
const MOST_AUTHORITY: usize = 64 * 1024;

/// `PUT /api/settings/acme`. Recorded with who set it and what changed, in the transaction that sets it.
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
            StatusCode::BAD_REQUEST,
            &format!("a directory URL, contact, name or key id is at most {MOST_TEXT} bytes"),
        );
    }
    if authority.len() > MOST_AUTHORITY {
        return refuse(
            StatusCode::BAD_REQUEST,
            &format!("the authority is at most {} KiB", MOST_AUTHORITY / 1024),
        );
    }
    if request.eab_hmac.as_ref().is_some_and(|k| k.len() > MOST_TEXT) {
        return refuse(StatusCode::BAD_REQUEST, "an external account key is at most 1024 bytes");
    }
    if !directory.is_empty() && !directory.starts_with("https://") {
        return refuse(StatusCode::BAD_REQUEST, "the directory is an https:// URL");
    }
    if names
        .iter()
        .any(|name| name.contains(',') || name.contains(char::is_whitespace))
    {
        return refuse(StatusCode::BAD_REQUEST, "a name carries no comma or space");
    }
    if request.enabled
        && let Err(e) = acme::orderable(&directory, &names)
    {
        return refuse(StatusCode::BAD_REQUEST, &e);
    }
    if !authority.is_empty()
        && let Err(e) = tls::describe_each(authority.as_bytes(), "the authority")
    {
        return refuse(StatusCode::BAD_REQUEST, &e);
    }

    let _ordered = state.auth.settings_writes.lock().await;
    let was = match settings(&state.auth).await {
        Ok(was) => was,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let key_moved = match request.eab_hmac.as_deref() {
        None => false,
        Some("") => was.eab_hmac.is_some(),
        Some(new) => was.eab_hmac.as_deref() != Some(new),
    };
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
        ];
        if let Some(key) = request.eab_hmac {
            let stored = if key.is_empty() {
                key
            } else {
                store.sealed_setting(acme::EAB_HMAC_KEY, &key)?
            };
            entries.push((acme::EAB_HMAC_KEY, stored));
        }
        store.put_settings(&entries, Some(&event))
    })
    .await;
    if let Err(e) = written {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, &e.to_string());
    }
    answer(&state).await
}

/// `POST /api/settings/acme/order`: order a certificate now, and serve it.
///
/// What a renewal does on its own, done while somebody watches: the failure an authority answers with is theirs
/// to read.
async fn order(State(state): State<Acme>, actor: axum::Extension<Actor>, headers: axum::http::HeaderMap) -> Response {
    let serving = match certificates_api::serving_tls(state.tls.as_ref()) {
        Ok(serving) => serving,
        Err(response) => return *response,
    };
    let _ordered = state.auth.settings_writes.lock().await;
    let settings = match settings(&state.auth).await {
        Ok(settings) => settings,
        Err(e) => return refuse(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    if !settings.enabled {
        return refuse(StatusCode::CONFLICT, "ACME is turned off. Turn it on before ordering.");
    }
    if let Err(e) = acme::orderable(&settings.directory, &settings.names) {
        return refuse(StatusCode::BAD_REQUEST, &e);
    }
    let required = certificates_api::must_serve(&serving, &headers);
    match run(&state.auth, &serving, &settings, &actor, &required).await {
        Ok(()) => certificates_api::answer_in_force(&serving).await,
        Err(e) => refuse(StatusCode::BAD_REQUEST, &e),
    }
}

/// One order, start to finish, under `settings_writes`: ordered, recorded, served or rotated to, and the socket
/// reloaded where it is served at once. A failure is recorded in the state the page reads.
async fn run(
    auth: &Auth,
    serving: &Arc<Tls>,
    settings: &acme::Settings,
    actor: &Actor,
    required: &[String],
) -> Result<(), String> {
    let now_ms = smeltr::leader::warnings::now_millis();
    let outcome = order_once(auth, serving, settings, actor, required, now_ms).await;
    if let Err(e) = &outcome {
        tracing::warn!(target: "leader.acme_order_failed", error = %e, "the certificate authority did not issue a certificate");
        acme::failed(&serving.dir, now_ms, e)?;
    }
    outcome
}

async fn order_once(
    auth: &Auth,
    serving: &Arc<Tls>,
    settings: &acme::Settings,
    actor: &Actor,
    required: &[String],
    now_ms: i64,
) -> Result<(), String> {
    let issuer = acme::Directory::new(settings.clone(), &serving.dir);
    let issued = acme::order(
        &serving.dir,
        settings,
        &issuer,
        serving.proving.as_ref(),
        &serving.names,
        required,
    )
    .await?;
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
    acme::take(&serving.dir, settings, &issued, &serving.names, required, now_ms)?;
    if !issued.rotates {
        certificates_api::reload_socket(serving)?;
    }
    Ok(())
}

/// How often a renewal is looked for. A certificate is renewed two thirds through its life, which is weeks away
/// for any authority's; an hour is for the retry after a failure.
const RENEWAL_TICK: Duration = Duration::from_secs(60 * 60);

/// Orders a renewal whenever one is due, for as long as this leader runs. Nothing while ordering is off, or while a
/// renewal would have to wait for a rotation to finish.
pub async fn renew_due(auth: Auth, serving: Arc<Tls>) {
    loop {
        tokio::time::sleep(RENEWAL_TICK).await;
        let settings = match settings(&auth).await {
            Ok(settings) => settings,
            Err(e) => {
                tracing::warn!(target: "leader.acme_settings_unreadable", error = %e, "could not read the ACME settings");
                continue;
            }
        };
        if !settings.enabled || acme::orderable(&settings.directory, &settings.names).is_err() {
            continue;
        }
        let _ordered = auth.settings_writes.lock().await;
        let now_ms = smeltr::leader::warnings::now_millis();
        let due = match (acme::state(&serving.dir), acme::source(&serving.dir)) {
            (Ok(state), Ok(source)) => acme::renews(&state, source, now_ms),
            (Err(e), _) | (_, Err(e)) => {
                tracing::warn!(target: "leader.acme_state_unreadable", error = %e, "could not read what the last order left");
                continue;
            }
        };
        if !due {
            continue;
        }
        let required: Vec<String> = serving.from_inside.iter().cloned().collect();
        let _ = run(&auth, &serving, &settings, &Actor::system(), &required).await;
    }
}
