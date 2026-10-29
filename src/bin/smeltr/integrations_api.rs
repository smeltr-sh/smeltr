//! What is an integration's own: the accounts scripts call the operator API as, and the keys they call with.
//!
//! An integration holds what its roles give, as an OIDC account holds what its groups give, or everything when it
//! is an administrator. A key is answered once, when it is made, and kept only as its hash. Disabling an
//! integration revokes every key it holds, so enabling it again brings none of them back.
//!
//! Reached through `/api/accounts` (`accounts_api`), behind the gate and needing an administrator.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{Actor, ApiKeyRecord, IntegrationChange, NewApiKey, NewIntegration};
use smeltr::leader::warnings::now_millis;
use smeltr::leader::{accounts, auth as tokens};

use crate::accounts_api::{PatchRequest, account_error, current_account, display_name, store_call};
use crate::gate::{Auth, refuse};

/// The longest key label, in characters: room for what the key is for, and no more.
const MAX_LABEL_CHARS: usize = 128;

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

/// An enabled integration holding no key yet, answered as the roster shows it.
pub async fn create_integration(
    auth: &Auth,
    actor: &Actor,
    display_name: &str,
    roles: Vec<String>,
    administrator: bool,
) -> Response {
    let name = match self::display_name(display_name) {
        Ok(name) => name,
        Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, why),
    };
    let roles = match known_roles(auth, roles).await {
        Ok(roles) => roles,
        Err(response) => return *response,
    };
    let id = accounts::generate_user_id();
    let event = accounts::event_for(actor, "integration-create", &id, "ok");
    let store = auth.store.clone();
    let created = id.clone();
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
                actor,
                "integration-create",
                display_name,
                "refused",
            );
            return account_error(e);
        }
        Err(response) => return *response,
    }
    (StatusCode::CREATED, current_account(auth, &id).await).into_response()
}

/// One field of the integration `id` changed, `request` naming exactly one.
pub async fn patch_integration(auth: &Auth, actor: &Actor, id: &str, request: PatchRequest) -> Response {
    let store = auth.store.clone();
    let (action, outcome) = if let Some(enabled) = request.enabled {
        let action = if enabled {
            "integration-change"
        } else {
            "integration-disable"
        };
        let event = accounts::event_for(actor, action, id, "ok");
        let target = id.to_string();
        (
            action,
            store_call(move || store.set_integration_enabled(&target, enabled, &event)).await,
        )
    } else {
        let change = if let Some(name) = &request.display_name {
            match display_name(name) {
                Ok(name) => IntegrationChange::Name(name),
                Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, why),
            }
        } else if let Some(roles) = request.roles {
            match known_roles(auth, roles).await {
                Ok(roles) => IntegrationChange::Roles(roles),
                Err(response) => return *response,
            }
        } else {
            IntegrationChange::Administrator(request.administrator.unwrap_or_default())
        };
        let event = accounts::event_for(actor, "integration-change", id, "ok");
        let target = id.to_string();
        (
            "integration-change",
            store_call(move || store.change_integration(&target, &change, &event)).await,
        )
    };
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            accounts::record(auth.store.as_ref(), actor, action, id, "refused");
            return account_error(e);
        }
        Err(response) => return *response,
    }
    current_account(auth, id).await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateKeyRequest {
    #[serde(default)]
    pub label: String,
}

/// `POST /api/accounts/:id/keys`: a new key for an enabled integration, answered this once.
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

/// `DELETE /api/accounts/:id/keys/:key`: the key stops letting anyone in from the next request.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_label_names_only_itself() {
        assert!(serde_json::from_str::<CreateKeyRequest>(r#"{"label":"nightly"}"#).is_ok());
        assert!(serde_json::from_str::<CreateKeyRequest>(r#"{"name":"nightly"}"#).is_err());
    }
}
