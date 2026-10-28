//! The agent permissions each person gives their own agent out of what they hold, and the settings AI agents run by.
//!
//! A person's agent permissions are theirs alone to set. No agent changes its own permissions, whatever it holds.
//!
//! None of this acts until an administrator turns AI agents on and names the model endpoint they think with
//! (`model::Settings`). What was set while they were on is kept while they are off.

use axum::Json;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use smeltr::config_file::{SHARED_DIR, group_subtree};
use smeltr::leader::access::{AgentPermissions, ByAgent};
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::model::{self, Provider, Settings};
use smeltr::leader::store::Actor;
use smeltr::leader::{accounts, blocking};

use crate::gate::{Auth, refuse};
use crate::users_api::{no_store, store_call};

/// Why an agent is refused a change to what it may do: only a person sets that.
const OWN_PERMISSIONS: &str = "an agent cannot change its own permissions";

/// The refusal of a change to what agents may do while AI agents are off, or `None` while they are on.
pub async fn refused_while_off(auth: &Auth) -> Option<Response> {
    let store = auth.store.clone();
    match store_call(move || model::on(store.as_ref())).await {
        Ok(true) => None,
        Ok(false) => Some(refuse(StatusCode::CONFLICT, "AI agents are not turned on")),
        Err(response) => Some(*response),
    }
}

/// `GET /api/me/agent-permissions`: what the caller lets their own agent use.
pub async fn get_permissions(State(auth): State<Auth>, actor: Extension<Actor>) -> Response {
    let store = auth.store.clone();
    match store_call(move || store.agent_permissions_of(&actor.id)).await {
        Ok(permissions) => no_store(permissions),
        Err(response) => *response,
    }
}

/// `PUT /api/me/agent-permissions`: replace what the caller lets their own agent use. Refused to their agent.
pub async fn put_permissions(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Json(permissions): Json<AgentPermissions>,
) -> Response {
    if by_agent.is_some() {
        return refuse(StatusCode::FORBIDDEN, OWN_PERMISSIONS);
    }
    if let Some(refused) = refused_while_off(&auth).await {
        return refused;
    }
    if let AgentPermissions::Upto { areas, .. } = &permissions
        && let Some(area) = areas.keys().find(|a| *a != SHARED_DIR && group_subtree(a).is_err())
    {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("{area:?} is neither {SHARED_DIR} nor a worker group id"),
        );
    }
    let event = accounts::event_for(&actor, "agent-permissions-put", &actor.id, "ok");
    let store = auth.store.clone();
    let (user_id, written) = (actor.id.clone(), permissions.clone());
    match store_call(move || store.put_agent_permissions(&user_id, &written, &event)).await {
        Ok(Ok(())) => no_store(permissions),
        Ok(Err(e)) => refuse(StatusCode::NOT_FOUND, &e.to_string()),
        Err(response) => *response,
    }
}

// -- settings -----------------------------------------------------------------

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsDto {
    enabled: bool,
    provider: Provider,
    endpoint: String,
    model: String,
    /// Whether an API key is stored. The key itself is never sent back.
    api_key_set: bool,
    keep_days: u32,
    context_tokens: u64,
}

#[derive(Default, serde::Serialize)]
pub struct AgentSettings {
    settings: Option<SettingsDto>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutSettings {
    enabled: bool,
    provider: Provider,
    endpoint: String,
    model: String,
    /// `None` keeps the stored key while the provider and endpoint stay the same; empty removes it.
    api_key: Option<String>,
    /// `None` keeps the days set.
    keep_days: Option<u32>,
    /// `None` keeps the tokens set. For Anthropic, what its Models API says the model holds is set instead.
    context_tokens: Option<u64>,
}

/// The most a model's name may be. Far beyond any provider's, and short enough to show.
const MOST_MODEL: usize = 256;

/// The most an API key may be.
const MOST_KEY: usize = 4096;

/// The most days a conversation may be kept: ten years.
const MOST_KEEP_DAYS: u32 = 3650;

/// The settings as stored, with `refused` beside them when a write was: the form shows what to correct.
async fn answer(auth: &Auth, refused: Option<Refusal>) -> Envelope<AgentSettings> {
    let store = auth.store.clone();
    let read = blocking(move || Settings::read(store.as_ref())).await;
    match read {
        Ok(settings) => Envelope::with(
            AgentSettings {
                settings: Some(SettingsDto {
                    enabled: settings.enabled,
                    provider: settings.provider,
                    endpoint: settings.endpoint,
                    model: settings.model,
                    api_key_set: settings.api_key.is_some(),
                    keep_days: settings.keep_days,
                    context_tokens: settings.context,
                }),
            },
            refused.as_ref().map_or(StatusCode::OK, |r| r.status),
            refused.map(|r| r.errors).unwrap_or_default(),
        ),
        Err(e) => Envelope::refused(e),
    }
}

/// `GET /api/settings/agent`.
pub async fn get_settings(State(auth): State<Auth>) -> Envelope<AgentSettings> {
    answer(&auth, None).await
}

/// `PUT /api/settings/agent`. Recorded with who set it and what changed, in the transaction that sets it; the API
/// key is sealed before it is stored.
///
/// Turning AI agents on asks the endpoint for a short answer first, so a key, model or URL that cannot work is
/// refused here rather than when an agent first thinks.
pub async fn put_settings(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    Json(request): Json<PutSettings>,
) -> Response {
    write_settings(&auth, &actor, request).await.into_response()
}

async fn write_settings(auth: &Auth, actor: &Actor, request: PutSettings) -> Envelope<AgentSettings> {
    let endpoint = request.endpoint.trim().to_string();
    let model_name = request.model.trim().to_string();
    if !endpoint.is_empty()
        && let Err(e) = model::check_endpoint(&endpoint)
    {
        return answer(auth, Some(Refusal::invalid(e))).await;
    }
    if model_name.len() > MOST_MODEL {
        return answer(
            auth,
            Some(Refusal::invalid(format!("a model is at most {MOST_MODEL} bytes"))),
        )
        .await;
    }
    if request.api_key.as_ref().is_some_and(|k| k.len() > MOST_KEY) {
        return answer(
            auth,
            Some(Refusal::invalid(format!("an API key is at most {MOST_KEY} bytes"))),
        )
        .await;
    }
    if request.keep_days.is_some_and(|d| d == 0 || d > MOST_KEEP_DAYS) {
        return answer(
            auth,
            Some(Refusal::invalid(format!(
                "conversations are kept from 1 to {MOST_KEEP_DAYS} days"
            ))),
        )
        .await;
    }

    if request
        .context_tokens
        .is_some_and(|t| !model::CONTEXT_RANGE.contains(&t))
    {
        return answer(
            auth,
            Some(Refusal::invalid(format!(
                "a context window is from {} to {} tokens",
                model::CONTEXT_RANGE.start(),
                model::CONTEXT_RANGE.end()
            ))),
        )
        .await;
    }

    let one_at_a_time = auth.settings_writes.lock().await;
    let store = auth.store.clone();
    let was = match blocking(move || Settings::read(store.as_ref())).await {
        Ok(was) => was,
        Err(e) => return answer(auth, Some(e.into())).await,
    };
    // A key belongs to the service it was issued by: another provider or endpoint forgets it unless a new one is
    // given.
    let key = match request.api_key {
        None if request.provider != was.provider || endpoint != was.endpoint => Some(String::new()),
        key => key,
    };
    let keep_days = request.keep_days.unwrap_or(was.keep_days);
    let mut context = request.context_tokens.unwrap_or(was.context);
    let will_hold = match &key {
        Some(k) if k.is_empty() => None,
        Some(k) => Some(k.clone()),
        None => was.api_key.clone(),
    };
    if request.enabled {
        let missing: Vec<&str> = [
            (endpoint.is_empty(), "endpoint URL"),
            (model_name.is_empty(), "model"),
            (will_hold.is_none(), "API key"),
        ]
        .into_iter()
        .filter_map(|(lacks, name)| lacks.then_some(name))
        .collect();
        if !missing.is_empty() {
            return answer(
                auth,
                Some(Refusal::invalid(format!(
                    "Turning AI agents on needs: {}.",
                    missing.join(", ")
                ))),
            )
            .await;
        }
        let asked = model::Endpoint {
            provider: request.provider,
            url: endpoint.clone(),
            model: model_name.clone(),
            api_key: will_hold.clone().unwrap_or_default(),
            context,
        };
        if let Some(said) = model::context_of(&asked).await {
            context = said;
        }
        if let Err(e) = model::check(asked).await {
            return answer(auth, Some(Refusal::invalid(e))).await;
        }
    }

    let changed: Vec<&str> = [
        (request.enabled && !was.enabled, "turned on"),
        (!request.enabled && was.enabled, "turned off"),
        (request.provider != was.provider, "provider"),
        (endpoint != was.endpoint, "endpoint"),
        (model_name != was.model, "model"),
        (will_hold != was.api_key, "API key"),
        (keep_days != was.keep_days, "days conversations are kept"),
        (context != was.context, "context window"),
    ]
    .into_iter()
    .filter_map(|(moved, name)| moved.then_some(name))
    .collect();
    let what = match changed.is_empty() {
        true => "nothing".to_string(),
        false => changed.join(", "),
    };
    let event = accounts::event_for(actor, "agents-change", &what, "ok");
    let store = auth.store.clone();
    let written = blocking(move || {
        let mut entries = vec![
            (model::ENABLED_KEY, request.enabled.to_string()),
            (model::PROVIDER_KEY, request.provider.as_str().to_string()),
            (model::ENDPOINT_KEY, endpoint),
            (model::MODEL_KEY, model_name),
            (model::KEEP_DAYS_KEY, keep_days.to_string()),
            (model::CONTEXT_KEY, context.to_string()),
        ];
        if let Some(key) = key {
            let stored = match key.is_empty() {
                true => key,
                false => store.sealed_setting(model::API_KEY_KEY, &key)?,
            };
            entries.push((model::API_KEY_KEY, stored));
        }
        store.put_settings(&entries, Some(&event))
    })
    .await;
    drop(one_at_a_time);
    answer(auth, written.err().map(Refusal::from)).await
}
