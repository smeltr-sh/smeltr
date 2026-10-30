//! What is a key account's own: the accounts scripts call the operator API as, and the keys they call with.
//!
//! A key account holds what an administrator gives it, as a local account does. A key is answered once, when it is
//! made, and kept only as its hash, and may be made to expire. Disabling the account revokes every key it holds
//! (`Store::set_user_enabled`), so enabling it again brings none of them back.
//!
//! Reached through `/api/accounts` (`accounts_api`), behind the gate and needing an administrator.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::{Actor, ApiKeyRecord, GivenAccess, NewApiKey, NewKeyAccount};
use smeltr::leader::warnings::now_millis;
use smeltr::leader::{accounts, auth as tokens};

use crate::accounts_api::{account_error, current_account, display_name, known_roles, store_call};
use crate::gate::{Auth, refuse};

/// The longest key label, in characters: room for what the key is for, and no more.
const MAX_LABEL_CHARS: usize = 128;

/// A day, in the milliseconds a key's expiry is kept in.
const DAY_MILLIS: i64 = 86_400_000;

/// A key not revoked, as the roster shows it, expired or not.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyDto {
    pub id: String,
    pub label: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
}

/// A key just made: the one answer that carries it.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedKeyDto {
    pub id: String,
    pub label: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub key: String,
}

impl From<ApiKeyRecord> for KeyDto {
    fn from(key: ApiKeyRecord) -> KeyDto {
        KeyDto {
            id: key.id,
            label: key.label,
            created_at: key.created_at,
            expires_at: key.expires_at,
            last_used_at: key.last_used_at,
        }
    }
}

/// An enabled key account holding no key yet, answered as the roster shows it.
pub async fn create_key_account(
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
    let event = accounts::event_for(actor, "account-create", &id, "ok");
    let store = auth.store.clone();
    let created = id.clone();
    let outcome = store_call(move || {
        store.create_key_account(
            NewKeyAccount {
                id: &created,
                display_name: &name,
            },
            GivenAccess {
                roles: &roles,
                administrator,
            },
            &event,
        )
    })
    .await;
    if let Err(response) = outcome {
        return *response;
    }
    (StatusCode::CREATED, current_account(auth, &id).await).into_response()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateKeyRequest {
    #[serde(default)]
    pub label: String,
    /// Days from now until the key stops letting anyone in. Absent for a key that never expires.
    pub expires_in_days: Option<u32>,
}

/// `POST /api/accounts/:id/keys`: a new key for an enabled key account, answered this once.
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
    if request.expires_in_days == Some(0) {
        return refuse(
            StatusCode::UNPROCESSABLE_ENTITY,
            "a key expires in one day or more, or never",
        );
    }
    let key = tokens::generate_secret();
    let key_id = format!("key_{}", &tokens::generate_secret()[..24]);
    let created_at = now_millis();
    let expires_at = request
        .expires_in_days
        .map(|days| created_at + i64::from(days) * DAY_MILLIS);
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
                expires_at,
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
            expires_at,
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
            refuse(StatusCode::NOT_FOUND, "that account holds no such key")
        }
        Err(response) => *response,
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
