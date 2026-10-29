//! Managing roles: which provider group gives what, in which area.
//!
//! A role is named for the group exactly as the provider's groups claim sends it, so it takes effect for whoever
//! holds that group on their next request, and for someone newly added to the group at their next sign-in.
//! Behind the gate and needing an administrator, like every other change to who may do what.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use smeltr::leader::envelope::Envelope;

use smeltr::config_file::{SHARED_DIR, group_subtree};
use smeltr::leader::access::{Level, Role};
use smeltr::leader::accounts;
use smeltr::leader::store::Actor;

use crate::gate::{Auth, refuse};
use crate::users_api::store_call;

/// The longest role name, in characters. A provider group can be a distinguished name, which runs long; past this
/// it is not one a person would type into a claim mapping.
const MAX_NAME_CHARS: usize = 256;

/// A role as the API carries it.
#[derive(serde::Serialize)]
pub struct RoleDto {
    pub name: String,
    pub administrator: bool,
    pub areas: BTreeMap<String, Level>,
}

impl From<Role> for RoleDto {
    fn from(role: Role) -> RoleDto {
        RoleDto {
            name: role.name,
            administrator: role.administrator,
            areas: role.areas,
        }
    }
}

/// `PUT /api/roles/:name`'s body. A level is read as a string so an unknown one is answered with a sentence, not
/// a decoder's error.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutRole {
    #[serde(default)]
    pub administrator: bool,
    #[serde(default)]
    pub areas: BTreeMap<String, String>,
}

/// `GET /api/roles`: every role, by name.
pub async fn list_roles(State(auth): State<Auth>) -> Response {
    let store = auth.store.clone();
    match store_call(move || store.list_roles()).await {
        Ok(roles) => {
            Envelope::ok(serde_json::json!({ "roles": roles.into_iter().map(RoleDto::from).collect::<Vec<_>>() }))
                .into_response()
        }
        Err(response) => *response,
    }
}

/// `PUT /api/roles/:name`: create the role, or replace it whole.
pub async fn put_role(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(name): Path<String>,
    Json(request): Json<PutRole>,
) -> Response {
    let role = match role_of(name, request) {
        Ok(role) => role,
        Err(why) => return refuse(StatusCode::UNPROCESSABLE_ENTITY, &why),
    };
    let event = accounts::event_for(&actor, "role-put", &role.name, "ok");
    let store = auth.store.clone();
    let written = role.clone();
    match store_call(move || store.put_role(&written, &event)).await {
        Ok(()) => Envelope::ok(RoleDto::from(role)).into_response(),
        Err(response) => *response,
    }
}

/// `DELETE /api/roles/:name`. Whoever held only this role holds nothing from their next request, and is signed out
/// at their next refresh.
pub async fn delete_role(
    State(auth): State<Auth>,
    actor: axum::Extension<Actor>,
    Path(name): Path<String>,
) -> Response {
    let event = accounts::event_for(&actor, "role-delete", &name, "ok");
    let store = auth.store.clone();
    let target = name.clone();
    match store_call(move || store.delete_role(&target, &event)).await {
        Ok(true) => Envelope::done().into_response(),
        Ok(false) => {
            accounts::record(auth.store.as_ref(), &actor, "role-delete", &name, "refused");
            refuse(StatusCode::NOT_FOUND, "there is no role by that name")
        }
        Err(response) => *response,
    }
}

/// The role `request` describes under `name`, checked. `Err` carries the sentence to answer with.
fn role_of(name: String, request: PutRole) -> Result<Role, String> {
    if name.is_empty() || name.chars().count() > MAX_NAME_CHARS {
        return Err(format!("a role's name is between 1 and {MAX_NAME_CHARS} characters"));
    }
    if name.chars().any(char::is_control) {
        return Err("a role's name holds no control characters".to_string());
    }
    let mut areas = BTreeMap::new();
    for (area, level) in request.areas {
        if area != SHARED_DIR && group_subtree(&area).is_err() {
            return Err(format!("{area:?} is neither {SHARED_DIR} nor a worker group id"));
        }
        let Some(level) = Level::parse(&level) else {
            return Err(format!("{level:?} is not a level: viewer, editor or deployer"));
        };
        areas.insert(area, level);
    }
    Ok(Role {
        name,
        administrator: request.administrator,
        areas,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(areas: &[(&str, &str)]) -> PutRole {
        PutRole {
            administrator: false,
            areas: areas.iter().map(|(a, l)| (a.to_string(), l.to_string())).collect(),
        }
    }

    #[test]
    fn a_role_names_levels_in_shared_and_in_groups() {
        let role = role_of(
            "smeltr-payments".into(),
            request(&[("shared", "viewer"), ("payments", "deployer")]),
        )
        .unwrap();
        assert_eq!(role.name, "smeltr-payments");
        assert_eq!(role.areas["shared"], Level::Viewer);
        assert_eq!(role.areas["payments"], Level::Deployer);
    }

    /// A distinguished name is a provider group too, commas, equals signs and spaces included.
    #[test]
    fn a_name_is_the_group_as_the_provider_sends_it() {
        assert!(role_of("CN=Smeltr Editors,OU=Groups,DC=example,DC=com".into(), request(&[])).is_ok());
    }

    #[test]
    fn a_name_is_bounded() {
        assert!(role_of(String::new(), request(&[])).is_err());
        assert!(role_of("a".repeat(MAX_NAME_CHARS), request(&[])).is_ok());
        assert!(role_of("a".repeat(MAX_NAME_CHARS + 1), request(&[])).is_err());
        assert!(role_of("a\nb".into(), request(&[])).is_err());
    }

    #[test]
    fn an_unknown_level_is_refused() {
        let refused = role_of("r".into(), request(&[("payments", "owner")])).unwrap_err();
        assert!(refused.contains("owner"), "{refused}");
        assert!(role_of("r".into(), request(&[("payments", "Viewer")])).is_err());
    }

    #[test]
    fn an_area_is_shared_or_a_group_id() {
        for bad in ["", "../etc", "a/b", "groups/payments"] {
            assert!(role_of("r".into(), request(&[(bad, "viewer")])).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_body_takes_nothing_it_does_not_know() {
        assert!(serde_json::from_str::<PutRole>(r#"{"administrator":true,"areas":{}}"#).is_ok());
        assert!(serde_json::from_str::<PutRole>(r#"{"administrator":true,"grants":{}}"#).is_err());
    }

    /// The handlers behind the gate, mounted as the leader mounts them.
    mod served {
        use super::*;

        use std::path::Path as FsPath;
        use std::sync::Arc;
        use std::time::Instant;

        use axum::Router;
        use axum::routing::put;
        use smeltr::leader::store::{OidcSignIn, Store, UserRecord};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use crate::gate::{CSRF_HEADER, Origin, require_principal};

        const ORIGIN: &str = "http://127.0.0.1:4000";

        fn store() -> Arc<dyn Store> {
            let key = std::env::temp_dir().join(format!(
                "smeltr-roles-api-{}-{}.key",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            Arc::new(smeltr::leader::sqlite_store::SqliteStore::open(FsPath::new(":memory:"), &key).unwrap())
        }

        /// An OIDC account holding an administrator role.
        fn administrator(store: &dyn Store) -> UserRecord {
            let admins = Role {
                name: "smeltr-admins".into(),
                administrator: true,
                areas: BTreeMap::new(),
            };
            let event = accounts::event_for(&Actor::system(), "role-put", &admins.name, "ok");
            store.put_role(&admins, &event).unwrap();
            let groups = vec![admins.name.clone()];
            let new_id = accounts::generate_user_id();
            let created = accounts::event_for(&Actor::system(), "account-create", &new_id, "ok");
            store
                .oidc_sign_in(
                    OidcSignIn {
                        issuer: "https://idp.example.com",
                        subject: "ada",
                        display_name: "ada",
                        groups: &groups,
                        login: None,
                        new_id: &new_id,
                        at: 0,
                    },
                    &created,
                )
                .unwrap()
        }

        async fn serve(auth: Auth) -> std::net::SocketAddr {
            let app = Router::new()
                .route("/api/roles/:name", put(put_role).delete(delete_role))
                .with_state(auth.clone())
                .route_layer(axum::middleware::from_fn_with_state(auth, require_principal));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let at = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            at
        }

        async fn ask(
            at: std::net::SocketAddr,
            auth: &Auth,
            user: &UserRecord,
            method: &str,
            target: &str,
            body: &str,
        ) -> (u16, String) {
            let issued = auth
                .sessions
                .issue(&user.id, user.generation, false, Instant::now())
                .unwrap();
            let request = format!(
                "{method} {target} HTTP/1.1\r\nHost: {at}\r\nCookie: {}={}\r\nOrigin: {ORIGIN}\r\n{CSRF_HEADER}: {}\r\n\
                 Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                auth.origin.cookie_name(),
                issued.token,
                issued.session.csrf,
                body.len(),
            );
            let mut stream = tokio::net::TcpStream::connect(at).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut answer = String::new();
            stream.read_to_string(&mut answer).await.unwrap();
            let status = answer.split(' ').nth(1).unwrap().parse().unwrap();
            let body = answer
                .split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default();
            (status, body)
        }

        fn auth(store: Arc<dyn Store>) -> Auth {
            let origin = Origin {
                value: ORIGIN.into(),
                aliases: Vec::new(),
                secure: false,
                proxied: false,
            };
            Auth::new(store, origin, Default::default())
        }

        /// Keycloak sends a subgroup as its path, `/team/sub`; percent-encoded, it is one segment of the route.
        #[tokio::test]
        async fn a_group_path_is_one_role_name() {
            let store = store();
            let ada = administrator(store.as_ref());
            let auth = auth(store.clone());
            let at = serve(auth.clone()).await;

            let (status, body) = ask(
                at,
                &auth,
                &ada,
                "PUT",
                "/api/roles/%2Fteam%2Fsub",
                r#"{"areas":{"payments":"editor"}}"#,
            )
            .await;
            assert_eq!(status, 200, "{body}");
            assert!(store.list_roles().unwrap().iter().any(|r| r.name == "/team/sub"));

            let (status, body) = ask(at, &auth, &ada, "DELETE", "/api/roles/%2Fteam%2Fsub", "").await;
            assert_eq!(status, 200, "{body}");
            assert!(!store.list_roles().unwrap().iter().any(|r| r.name == "/team/sub"));
        }

        /// Deleting a role that is not there changes nothing, and the audit says it was refused.
        #[tokio::test]
        async fn deleting_a_missing_role_is_recorded_as_refused() {
            let store = store();
            let ada = administrator(store.as_ref());
            let auth = auth(store.clone());
            let at = serve(auth.clone()).await;

            let (status, body) = ask(at, &auth, &ada, "DELETE", "/api/roles/nobody", "").await;
            assert_eq!(status, 404, "{body}");
            let deletes: Vec<_> = store
                .list_security_events(100)
                .unwrap()
                .into_iter()
                .filter(|e| e.action == "role-delete")
                .collect();
            assert_eq!(deletes.len(), 1, "{deletes:?}");
            assert_eq!(
                (deletes[0].target.as_str(), deletes[0].outcome.as_str()),
                ("nobody", "refused")
            );
        }
    }
}
