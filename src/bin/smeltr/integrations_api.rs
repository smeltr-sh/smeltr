//! Managing integrations: the accounts scripts call the operator API as, and the keys they call with.
//!
//! An integration holds what its roles give, as an OIDC account holds what its groups give, or everything when it
//! is an administrator. A key is answered once, when it is made, and kept only as its hash. Disabling an
//! integration revokes every key it holds, so enabling it again brings none of them back.
//!
//! Behind the gate and needing an administrator, and never reached with a key (`gate`), so no key makes another.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use smeltr::leader::access::Role;
use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{
    AccountError, Actor, ApiKeyRecord, IntegrationChange, IntegrationRecord, NewApiKey, NewIntegration, StoreError,
};
use smeltr::leader::warnings::now_millis;
use smeltr::leader::{accounts, auth as tokens};

use crate::gate::{AccessDto, Auth, integration_access, refuse};
use crate::users_api::{account_error, display_name, store_call};

/// The longest key label, in characters: room for what the key is for, and no more.
const MAX_LABEL_CHARS: usize = 128;

/// An integration as the API carries it. Never a key or its hash.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationDto {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub roles: Vec<String>,
    /// Whether an administrator made it one. What it holds, from this or its roles, is `access`.
    pub administrator: bool,
    pub access: AccessDto,
    pub keys: Vec<KeyDto>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A key in force, as the roster shows it.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyDto {
    pub id: String,
    pub label: String,
    pub created_at: i64,
    pub last_used_at: Option<i64>,
}

/// A key just made: the one answer that carries it.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedKeyDto {
    pub id: String,
    pub label: String,
    pub created_at: i64,
    pub key: String,
}

impl From<ApiKeyRecord> for KeyDto {
    fn from(key: ApiKeyRecord) -> KeyDto {
        KeyDto {
            id: key.id,
            label: key.label,
            created_at: key.created_at,
            last_used_at: key.last_used_at,
        }
    }
}

impl IntegrationDto {
    /// `integration` as the roster shows it, given every role there is.
    fn of(integration: IntegrationRecord, roles: &[Role]) -> IntegrationDto {
        let access = integration_access(&integration, roles);
        IntegrationDto {
            id: integration.id,
            name: integration.name,
            enabled: integration.enabled,
            roles: integration.roles,
            administrator: integration.administrator,
            access: AccessDto::from(&access),
            keys: integration.keys.into_iter().map(KeyDto::from).collect(),
            created_at: integration.created_at,
            updated_at: integration.updated_at,
        }
    }
}

/// `GET /api/integrations`: every integration, oldest first.
pub async fn list_integrations(State(auth): State<Auth>) -> Response {
    let store = auth.store.clone();
    let listed = store_call(move || {
        let roles = store.list_roles()?;
        Ok(store
            .list_integrations()?
            .into_iter()
            .map(|integration| IntegrationDto::of(integration, &roles))
            .collect::<Vec<_>>())
    })
    .await;
    match listed {
        Ok(integrations) => Envelope::ok(serde_json::json!({ "integrations": integrations })).into_response(),
        Err(response) => *response,
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub name: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub administrator: bool,
}

/// `POST /api/integrations`: an enabled integration holding no key yet.
pub async fn create_integration(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Json(request): Json<CreateRequest>,
) -> Response {
    let name = match name_of(&request.name) {
        Ok(name) => name,
        Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &why),
    };
    let roles = match known_roles(&auth, request.roles).await {
        Ok(roles) => roles,
        Err(response) => return *response,
    };
    let id = accounts::generate_user_id();
    let event = accounts::event_for(&actor, "integration-create", &id, "ok");
    let store = auth.store.clone();
    let (created, administrator) = (id.clone(), request.administrator);
    let outcome = store_call(move || {
        store.create_integration(
            NewIntegration {
                id: &created,
                name: &name,
                roles: &roles,
                administrator,
            },
            &event,
        )
    })
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            accounts::record(
                auth.store.as_ref(),
                &actor,
                "integration-create",
                &request.name,
                "refused",
            );
            return account_error(e);
        }
        Err(response) => return *response,
    }
    match current(&auth, &id).await {
        Ok(integration) => (StatusCode::CREATED, Envelope::ok(integration)).into_response(),
        Err(response) => *response,
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchRequest {
    pub name: Option<String>,
    pub roles: Option<Vec<String>>,
    pub administrator: Option<bool>,
    pub enabled: Option<bool>,
}

/// `PATCH /api/integrations/:id`.
///
/// One field at a time, as an account is changed: each is its own write, and a request refused on its second
/// field would already have landed its first.
pub async fn patch_integration(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<PatchRequest>,
) -> Response {
    let named = [
        request.name.is_some(),
        request.roles.is_some(),
        request.administrator.is_some(),
        request.enabled.is_some(),
    ];
    if named.iter().filter(|&&n| n).count() != 1 {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "change one of name, roles, administrator or enabled in each request",
        );
    }

    let store = auth.store.clone();
    let (action, outcome) = if let Some(enabled) = request.enabled {
        let action = if enabled {
            "integration-change"
        } else {
            "integration-disable"
        };
        let event = accounts::event_for(&actor, action, &id, "ok");
        let target = id.clone();
        (
            action,
            store_call(move || store.set_integration_enabled(&target, enabled, &event)).await,
        )
    } else {
        let change = if let Some(name) = &request.name {
            match name_of(name) {
                Ok(name) => IntegrationChange::Name(name),
                Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &why),
            }
        } else if let Some(roles) = request.roles {
            match known_roles(&auth, roles).await {
                Ok(roles) => IntegrationChange::Roles(roles),
                Err(response) => return *response,
            }
        } else {
            IntegrationChange::Administrator(request.administrator.unwrap_or_default())
        };
        let event = accounts::event_for(&actor, "integration-change", &id, "ok");
        let target = id.clone();
        (
            "integration-change",
            store_call(move || store.change_integration(&target, &change, &event)).await,
        )
    };
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            accounts::record(auth.store.as_ref(), &actor, action, &id, "refused");
            return account_error(e);
        }
        Err(response) => return *response,
    }
    match current(&auth, &id).await {
        Ok(integration) => Envelope::ok(integration).into_response(),
        Err(response) => *response,
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateKeyRequest {
    #[serde(default)]
    pub label: String,
}

/// `POST /api/integrations/:id/keys`: a new key for an enabled integration, answered this once.
pub async fn create_key(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(id): Path<String>,
    Json(request): Json<CreateKeyRequest>,
) -> Response {
    let label = request.label.trim().to_string();
    if label.chars().count() > MAX_LABEL_CHARS {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("a key's label is at most {MAX_LABEL_CHARS} characters"),
        );
    }
    if label.chars().any(char::is_control) {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "a key's label holds no control characters",
        );
    }
    let key = tokens::generate_secret();
    let key_id = format!("key_{}", &tokens::generate_secret()[..24]);
    let created_at = now_millis();
    let event = accounts::event_for(&actor, "key-create", &key_id, "ok");
    let store = auth.store.clone();
    let (target, written_id, written_label, token_hash) =
        (id.clone(), key_id.clone(), label.clone(), tokens::hash_secret(&key));
    let outcome = store_call(move || {
        store.create_api_key(
            &target,
            NewApiKey {
                id: &written_id,
                label: &written_label,
                token_hash: &token_hash,
                created_at,
            },
            &event,
        )
    })
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            accounts::record(auth.store.as_ref(), &actor, "key-create", &id, "refused");
            return account_error(e);
        }
        Err(response) => return *response,
    }
    (
        StatusCode::CREATED,
        Envelope::ok(CreatedKeyDto {
            id: key_id,
            label,
            created_at,
            key,
        }),
    )
        .into_response()
}

/// `DELETE /api/integrations/:id/keys/:key`: the key stops letting anyone in from the next request.
pub async fn revoke_key(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path((id, key_id)): Path<(String, String)>,
) -> Response {
    let event = accounts::event_for(&actor, "key-revoke", &key_id, "ok");
    let store = auth.store.clone();
    let (target, revoked) = (id.clone(), key_id.clone());
    match store_call(move || store.revoke_api_key(&target, &revoked, now_millis(), &event)).await {
        Ok(true) => Envelope::done().into_response(),
        Ok(false) => {
            accounts::record(auth.store.as_ref(), &actor, "key-revoke", &key_id, "refused");
            refuse(StatusCode::NOT_FOUND, "that integration holds no such key")
        }
        Err(response) => *response,
    }
}

/// The integration as it stands, for a caller that just changed it.
async fn current(auth: &Auth, id: &str) -> Result<IntegrationDto, Box<Response>> {
    let store = auth.store.clone();
    let target = id.to_string();
    let looked_up = store_call(move || {
        let roles = store.list_roles()?;
        Ok::<_, StoreError>(store.integration(&target)?.map(|i| IntegrationDto::of(i, &roles)))
    })
    .await?;
    looked_up.ok_or_else(|| Box::new(account_error(AccountError::NoSuchUser)))
}

/// `roles`, each named once, if every one is a role that exists.
async fn known_roles(auth: &Auth, mut roles: Vec<String>) -> Result<Vec<String>, Box<Response>> {
    roles.sort();
    roles.dedup();
    let store = auth.store.clone();
    let existing = store_call(move || store.list_roles()).await?;
    match roles
        .iter()
        .find(|name| !existing.iter().any(|role| &role.name == *name))
    {
        Some(unknown) => Err(Box::new(refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("there is no role named {unknown:?}"),
        ))),
        None => Ok(roles),
    }
}

/// An integration's name, checked as a display name is, since that is what it is.
fn name_of(raw: &str) -> Result<String, String> {
    display_name(raw).map_err(|why| why.replacen("a display name", "an integration's name", 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_refused_as_an_integrations() {
        assert_eq!(name_of("  nightly export ").unwrap(), "nightly export");
        assert_eq!(
            name_of("").unwrap_err(),
            "an integration's name is between 1 and 128 characters"
        );
        assert!(name_of("a<b").unwrap_err().starts_with("an integration's name"));
    }

    #[test]
    fn a_change_names_what_it_changes() {
        assert!(serde_json::from_str::<PatchRequest>(r#"{"enabled":false}"#).is_ok());
        assert!(serde_json::from_str::<PatchRequest>(r#"{"admin":true}"#).is_err());
    }
}
