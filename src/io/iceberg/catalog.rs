//! A REST catalog (the Iceberg REST catalog API, `/v1/...`): signing in, loading and creating a table, asking for its
//! storage credentials, committing to it. Every call is blocking, on the caller's agent, and bounded by one timeout.
//!
//! ```mermaid
//! sequenceDiagram
//!   participant W as writer
//!   participant T as token endpoint
//!   participant C as catalog
//!   W->>T: client_credentials (when signing in with them)
//!   T-->>W: token, when it expires
//!   W->>C: GET /v1/config?warehouse=
//!   C-->>W: defaults, overrides: the prefix of every path after
//!   W->>C: GET .../tables/{table}, vended credentials asked for
//!   C-->>W: 404, or the metadata and the storage credentials
//!   W->>C: POST .../tables/{table}: requirements, updates
//!   C-->>W: the new metadata, 409 conflict, or 5xx: it may have landed
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::metadata::{PartitionSpec, Schema, Snapshot, SnapshotRef, TableMetadata};
use crate::io::IoError;
use crate::io::client_credentials::ClientCredentials;
use crate::io::http::percent_encode;

/// The most a catalog's answer may hold. A table's answer carries its whole metadata: every unexpired snapshot, every
/// schema and spec it ever had. A table committed every minute with hourly expiry holds a few MB; this is room for one
/// whose expiry falls behind, and still a bound on what a broken server can make a worker hold.
const MAX_ANSWER: u64 = 64 * 1024 * 1024;

/// Asks the catalog to answer a table with credentials for its storage.
const ACCESS_DELEGATION: (&str, &str) = ("X-Iceberg-Access-Delegation", "vended-credentials");

/// The endpoint a catalog advertises when it answers [`Catalog::credentials`].
const CREDENTIALS_ENDPOINT: &str = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

/// How the catalog is signed in to.
#[derive(Clone)]
pub enum Auth {
    None,
    /// A token sent as it is on every call. Nothing renews it.
    Bearer(String),
    /// OAuth 2.0 client credentials, traded for a token renewed before it expires.
    ClientCredentials {
        /// Where the token is asked for; the catalog's own `/v1/oauth/tokens` when `None`.
        token_url: Option<String>,
        client_id: String,
        client_secret: String,
        /// Sent only when given. Polaris wants `PRINCIPAL_ROLE:ALL`.
        scope: Option<String>,
    },
    /// Every call signed on its own, as AWS signs calls to its services.
    Signed(Arc<dyn Signer>),
}

/// Signs one call to the catalog.
pub trait Signer: Send + Sync {
    /// The headers that sign a call of `method` to `url` carrying `body`, to send with it.
    fn sign(&self, method: &str, url: &str, body: &[u8]) -> Result<Vec<(String, String)>, String>;
}

/// A table as the catalog answers it.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Loaded {
    /// Where the metadata is written; `None` for a table staged and not yet committed.
    #[serde(default)]
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    /// The table's settings for a client: a server that vends credentials without `storage-credentials` puts them
    /// here (`s3.access-key-id`, ...).
    #[serde(default)]
    pub config: BTreeMap<String, String>,
    #[serde(default)]
    pub storage_credentials: Vec<StorageCredential>,
}

impl std::fmt::Debug for Loaded {
    /// `config`'s keys only: a catalog may vend credentials in it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loaded")
            .field("metadata_location", &self.metadata_location)
            .field("metadata", &self.metadata)
            .field("config", &self.config.keys().collect::<Vec<_>>())
            .field("storage_credentials", &self.storage_credentials)
            .finish()
    }
}

/// Credentials for the files under `prefix`. The longest prefix that holds a file's path is the one for it.
#[derive(Clone, PartialEq, Deserialize)]
pub struct StorageCredential {
    pub prefix: String,
    pub config: BTreeMap<String, String>,
}

impl std::fmt::Debug for StorageCredential {
    /// The keys only: the values are secrets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCredential")
            .field("prefix", &self.prefix)
            .field("config", &self.config.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// The table after a commit landed.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Committed {
    pub metadata_location: String,
    pub metadata: TableMetadata,
}

/// What must hold of the table for a commit to apply.
#[derive(Clone, Debug, PartialEq)]
pub enum Requirement {
    AssertTableUuid(String),
    /// The ref points at this snapshot; `None` asserts the ref does not exist.
    AssertRefSnapshotId {
        r#ref: String,
        snapshot_id: Option<i64>,
    },
    AssertCurrentSchemaId(i32),
    AssertLastAssignedFieldId(i32),
}

impl Serialize for Requirement {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Requirement::AssertTableUuid(uuid) => json!({"type": "assert-table-uuid", "uuid": uuid}),
            // `snapshot-id` is written even when null: null is what asserts the ref is absent.
            Requirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                json!({"type": "assert-ref-snapshot-id", "ref": r#ref, "snapshot-id": snapshot_id})
            }
            Requirement::AssertCurrentSchemaId(id) => {
                json!({"type": "assert-current-schema-id", "current-schema-id": id})
            }
            Requirement::AssertLastAssignedFieldId(id) => {
                json!({"type": "assert-last-assigned-field-id", "last-assigned-field-id": id})
            }
        }
        .serialize(serializer)
    }
}

/// One change a commit makes to the table.
#[derive(Clone, Debug, PartialEq)]
pub enum Update {
    AddSchema {
        schema: Schema,
        last_column_id: i32,
    },
    /// The schema to make current; -1 is the one last added in this commit.
    SetCurrentSchema(i32),
    AddSnapshot(Snapshot),
    /// Sets the ref `name` to `reference`, replacing it whole.
    SetSnapshotRef {
        name: String,
        reference: SnapshotRef,
    },
}

impl Serialize for Update {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Update::AddSchema { schema, last_column_id } => {
                json!({"action": "add-schema", "schema": schema, "last-column-id": last_column_id})
            }
            Update::SetCurrentSchema(id) => json!({"action": "set-current-schema", "schema-id": id}),
            Update::AddSnapshot(snapshot) => json!({"action": "add-snapshot", "snapshot": snapshot}),
            Update::SetSnapshotRef { name, reference } => {
                #[derive(Serialize)]
                #[serde(rename_all = "kebab-case")]
                struct SetSnapshotRef<'a> {
                    action: &'static str,
                    ref_name: &'a str,
                    #[serde(flatten)]
                    reference: &'a SnapshotRef,
                }
                return SetSnapshotRef {
                    action: "set-snapshot-ref",
                    ref_name: name,
                    reference,
                }
                .serialize(serializer);
            }
        }
        .serialize(serializer)
    }
}

/// Why a commit did not land, as far as the caller can know.
#[derive(Clone, Debug, PartialEq)]
pub enum CommitFailure {
    /// A requirement did not hold (409): another writer committed first. Load the table and commit again.
    Conflict,
    /// The commit may have landed (500, 502, 503, 504, or no answer after the request was sent): the spec's
    /// CommitStateUnknown. Load the table to learn whether it did before committing again.
    Unknown(String),
    /// The commit did not land and was refused, or never reached the catalog.
    Failed(String),
}

/// A blocking client of one REST catalog.
pub struct Catalog {
    uri: String,
    warehouse: Option<String>,
    auth: Auth,
    agent: ureq::Agent,
    timeout: Duration,
    /// The sign-in `auth` names when it is client credentials.
    client_credentials: Option<ClientCredentials>,
    /// The catalog's answer to `/v1/config`, once asked.
    config: Option<Config>,
    /// The token the last load or create of the table answered in its `config`: sent on every call in place of the
    /// catalog's own sign-in, until a call is refused with it.
    table_token: Option<String>,
}

struct Config {
    /// The catalog's defaults, then the overrides over them.
    properties: BTreeMap<String, String>,
    /// The endpoints the catalog serves, when it says; `None` from a server older than the list.
    endpoints: Option<Vec<String>>,
}

/// An answer, whatever its status.
struct Reply {
    status: u16,
    body: Vec<u8>,
}

/// Why a call has no answer.
enum Unanswered {
    /// The request never left: signing in failed, or no connection was made.
    NotSent(IoError),
    /// The request may have reached the catalog.
    Lost(IoError),
}

impl Unanswered {
    fn into_error(self) -> IoError {
        match self {
            Unanswered::NotSent(e) | Unanswered::Lost(e) => e,
        }
    }
}

#[derive(Clone, Copy)]
enum Method {
    Get,
    Post,
}

impl Method {
    fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
        }
    }
}

impl Catalog {
    /// `uri` is the catalog's base, under which `/v1/...` is asked. `timeout` bounds each call, sign-in included.
    pub fn new(uri: String, warehouse: Option<String>, auth: Auth, agent: ureq::Agent, timeout: Duration) -> Catalog {
        let uri = uri.trim_end_matches('/').to_string();
        let client_credentials = client_credentials(&uri, &auth, &agent, timeout);
        Catalog {
            uri,
            warehouse,
            auth,
            agent,
            timeout,
            client_credentials,
            config: None,
            table_token: None,
        }
    }

    /// How the catalog is signed in to.
    #[cfg(test)]
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    /// Asks the catalog for its settings, once: the prefix of every path after, and the base they are asked under when
    /// the catalog overrides it. Every other call opens first.
    pub fn open(&mut self) -> Result<(), IoError> {
        if self.config.is_some() {
            return Ok(());
        }
        let mut url = format!("{}/v1/config", self.uri);
        if let Some(warehouse) = &self.warehouse {
            url = format!("{url}?warehouse={}", percent_encode(warehouse));
        }
        let reply = self
            .call(Method::Get, &url, None, false)
            .map_err(Unanswered::into_error)?;
        if reply.status != 200 {
            return Err(refused("reading the catalog's config", &reply));
        }
        #[derive(Deserialize)]
        struct Answer {
            #[serde(default)]
            defaults: BTreeMap<String, String>,
            #[serde(default)]
            overrides: BTreeMap<String, String>,
            #[serde(default)]
            endpoints: Option<Vec<String>>,
        }
        let answer: Answer = parse("the catalog's config", &reply)?;
        // Only an override moves the base: the configured URI beats a default.
        if let Some(uri) = answer.overrides.get("uri") {
            self.uri = moved_base(&self.uri, uri)?;
            // A default token endpoint is the catalog's own, so it moves with the base.
            if let (Auth::ClientCredentials { token_url: None, .. }, Some(credentials)) =
                (&self.auth, &mut self.client_credentials)
            {
                credentials.sign_in_at(format!("{}/v1/oauth/tokens", self.uri));
            }
        }
        let mut properties = answer.defaults;
        properties.extend(answer.overrides);
        self.config = Some(Config {
            properties,
            endpoints: answer.endpoints,
        });
        Ok(())
    }

    /// The table, with credentials for its storage when the catalog vends them. `None` when it does not exist.
    pub fn load_table(&mut self, namespace: &[String], table: &str) -> Result<Option<Loaded>, IoError> {
        let url = self.table_url(namespace, table)?;
        let reply = self
            .call(Method::Get, &url, None, true)
            .map_err(Unanswered::into_error)?;
        match reply.status {
            200 => self.keep_table_token(parse("the table", &reply)?).map(Some),
            404 => Ok(None),
            _ => Err(refused(&format!("loading the table {table:?}"), &reply)),
        }
    }

    /// Creates the table with `schema` and `spec`, whose source ids are `schema`'s field ids. The catalog may assign
    /// ids of its own: the answer's metadata is the table. A table another writer created first is loaded instead.
    pub fn create_table(
        &mut self,
        namespace: &[String],
        table: &str,
        schema: &Schema,
        spec: &PartitionSpec,
        properties: &BTreeMap<String, String>,
    ) -> Result<Loaded, IoError> {
        if let Some(field) = spec.fields.iter().find(|f| schema.field(f.source_id).is_none()) {
            return Err(IoError::transient(format!(
                "the partition field {:?} names the column {}, which the schema does not hold",
                field.name, field.source_id
            )));
        }
        let url = format!("{}/tables", self.namespace_url(namespace)?);
        let body = json!({
            "name": table,
            "schema": schema,
            "partition-spec": spec,
            "stage-create": false,
            "properties": properties,
        });
        let reply = self
            .call(Method::Post, &url, Some(&body), true)
            .map_err(Unanswered::into_error)?;
        match reply.status {
            200 => self.keep_table_token(parse("the created table", &reply)?),
            409 => self.load_table(namespace, table)?.ok_or_else(|| {
                IoError::transient(format!(
                    "the table {table:?} already existed when created, and not when loaded after"
                ))
            }),
            _ => Err(refused(&format!("creating the table {table:?}"), &reply)),
        }
    }

    /// Fresh credentials for the table's storage. `None` when the catalog does not serve this (servers older than
    /// the credentials endpoint): [`Catalog::load_table`] answers them too.
    pub fn credentials(
        &mut self,
        namespace: &[String],
        table: &str,
    ) -> Result<Option<Vec<StorageCredential>>, IoError> {
        let url = format!("{}/credentials", self.table_url(namespace, table)?);
        if let Some(endpoints) = self.config.as_ref().and_then(|c| c.endpoints.as_ref())
            && !endpoints.iter().any(|e| e == CREDENTIALS_ENDPOINT)
        {
            return Ok(None);
        }
        let reply = self
            .call(Method::Get, &url, None, false)
            .map_err(Unanswered::into_error)?;
        match reply.status {
            200 => {
                #[derive(Deserialize)]
                #[serde(rename_all = "kebab-case")]
                struct Answer {
                    storage_credentials: Vec<StorageCredential>,
                }
                parse::<Answer>("the table's credentials", &reply).map(|a| Some(a.storage_credentials))
            }
            404 | 405 | 501 => Ok(None),
            _ => Err(refused(
                &format!("asking for the table {table:?}'s credentials"),
                &reply,
            )),
        }
    }

    /// Applies `updates` to the table if every one of `requirements` holds of it.
    pub fn commit(
        &mut self,
        namespace: &[String],
        table: &str,
        requirements: &[Requirement],
        updates: &[Update],
    ) -> Result<Committed, CommitFailure> {
        let url = self
            .table_url(namespace, table)
            .map_err(|e| CommitFailure::Failed(e.to_string()))?;
        let body = json!({
            "identifier": {"namespace": namespace, "name": table},
            "requirements": requirements,
            "updates": updates,
        });
        let reply = match self.call(Method::Post, &url, Some(&body), false) {
            Ok(reply) => reply,
            Err(Unanswered::NotSent(e)) => return Err(CommitFailure::Failed(e.to_string())),
            Err(Unanswered::Lost(e)) => return Err(CommitFailure::Unknown(e.to_string())),
        };
        let what = format!("committing to the table {table:?}");
        match reply.status {
            200 => parse("the committed table", &reply).map_err(|e| {
                // It landed; only its answer is unreadable.
                CommitFailure::Unknown(e.to_string())
            }),
            409 => Err(CommitFailure::Conflict),
            500 | 502 | 503 | 504 => Err(CommitFailure::Unknown(refused(&what, &reply).to_string())),
            _ => Err(CommitFailure::Failed(refused(&what, &reply).to_string())),
        }
    }

    /// Keeps the token `loaded`'s config carries for the table's later calls, replacing the last one.
    fn keep_table_token(&mut self, loaded: Loaded) -> Result<Loaded, IoError> {
        self.table_token = loaded.config.get("token").cloned();
        Ok(loaded)
    }

    /// `/v1/{prefix}/namespaces/{namespace}`, the levels joined by the unit separator as the spec writes them.
    fn namespace_url(&mut self, namespace: &[String]) -> Result<String, IoError> {
        self.open()?;
        let prefix = self
            .config
            .as_ref()
            .and_then(|c| c.properties.get("prefix"))
            .filter(|p| !p.is_empty())
            .map(|p| format!("{}/", p.trim_matches('/')))
            .unwrap_or_default();
        Ok(format!(
            "{}/v1/{prefix}namespaces/{}",
            self.uri,
            percent_encode(&namespace.join("\u{1f}"))
        ))
    }

    fn table_url(&mut self, namespace: &[String], table: &str) -> Result<String, IoError> {
        Ok(format!(
            "{}/tables/{}",
            self.namespace_url(namespace)?,
            percent_encode(table)
        ))
    }

    /// One call, signed in. A token refused (401, or 419 for one expired) is replaced and the call asked again, once.
    /// A table token refused is dropped for the catalog's own sign-in, so a stale one cannot wedge the table.
    fn call(&mut self, method: Method, url: &str, body: Option<&Value>, delegate: bool) -> Result<Reply, Unanswered> {
        let mut renewed = false;
        // Serialized once: a signature covers the bytes sent.
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        loop {
            let signed = match (&self.table_token, &self.auth) {
                (Some(token), _) => vec![("Authorization".to_string(), format!("Bearer {token}"))],
                (None, Auth::Signed(signer)) => signer
                    .sign(method.as_str(), url, body.as_bytes())
                    .map_err(|e| Unanswered::NotSent(IoError::transient(format!("signing a call to {url}: {e}"))))?,
                (None, _) => {
                    let authorization = self.authorization().map_err(Unanswered::NotSent)?;
                    authorization
                        .map(|a| ("Authorization".to_string(), a))
                        .into_iter()
                        .collect()
                }
            };
            let reply = self.send(method, url, &body, delegate, &signed)?;
            if matches!(reply.status, 401 | 419) {
                if self.table_token.take().is_some() {
                    continue;
                }
                if let Some(credentials) = &mut self.client_credentials
                    && !renewed
                {
                    credentials.forget();
                    renewed = true;
                    continue;
                }
            }
            return Ok(reply);
        }
    }

    /// The `Authorization` header's value, signing in first when the token is missing or about to expire.
    fn authorization(&mut self) -> Result<Option<String>, IoError> {
        match &self.auth {
            Auth::None | Auth::Signed(_) => Ok(None),
            Auth::Bearer(token) => Ok(Some(format!("Bearer {token}"))),
            Auth::ClientCredentials { .. } => self
                .client_credentials
                .as_mut()
                .map(ClientCredentials::bearer)
                .transpose(),
        }
    }

    fn send(
        &self,
        method: Method,
        url: &str,
        body: &str,
        delegate: bool,
        signed: &[(String, String)],
    ) -> Result<Reply, Unanswered> {
        let sent = match method {
            Method::Get => {
                let mut request = self
                    .agent
                    .get(url)
                    .config()
                    .timeout_global(Some(self.timeout))
                    .http_status_as_error(false)
                    .max_redirects(0)
                    .build()
                    .header("Accept", "application/json");
                for (name, value) in signed {
                    request = request.header(name, value);
                }
                if delegate {
                    request = request.header(ACCESS_DELEGATION.0, ACCESS_DELEGATION.1);
                }
                request.call()
            }
            Method::Post => {
                // No redirect is followed: ureq would follow a POST's as a GET with no body.
                let mut request = self
                    .agent
                    .post(url)
                    .config()
                    .timeout_global(Some(self.timeout))
                    .http_status_as_error(false)
                    .max_redirects(0)
                    .build()
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json");
                for (name, value) in signed {
                    request = request.header(name, value);
                }
                if delegate {
                    request = request.header(ACCESS_DELEGATION.0, ACCESS_DELEGATION.1);
                }
                request.send(body.as_bytes())
            }
        };
        let mut response = sent.map_err(|e| {
            let failed = IoError::transient(format!("{url} failed: {e}"));
            match e {
                ureq::Error::HostNotFound
                | ureq::Error::ConnectionFailed
                | ureq::Error::BadUri(_)
                | ureq::Error::RequireHttpsOnly(_)
                | ureq::Error::InvalidProxyUrl
                | ureq::Error::ConnectProxyFailed(_)
                | ureq::Error::Timeout(ureq::Timeout::Resolve | ureq::Timeout::Connect) => Unanswered::NotSent(failed),
                _ => Unanswered::Lost(failed),
            }
        })?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_ANSWER)
            .read_to_vec()
            .map_err(|e| Unanswered::Lost(IoError::transient(format!("could not read {url}'s answer: {e}"))))?;
        Ok(Reply { status, body })
    }
}

/// The sign-in `auth` names when it is client credentials, at its token endpoint, else the catalog's own under `uri`.
fn client_credentials(uri: &str, auth: &Auth, agent: &ureq::Agent, timeout: Duration) -> Option<ClientCredentials> {
    let Auth::ClientCredentials {
        token_url,
        client_id,
        client_secret,
        scope,
    } = auth
    else {
        return None;
    };
    Some(ClientCredentials::new(
        token_url.clone().unwrap_or_else(|| format!("{uri}/v1/oauth/tokens")),
        client_id.clone(),
        client_secret.clone(),
        scope.clone(),
        agent.clone(),
        timeout,
    ))
}

/// Reads an answer the catalog gave as `what`.
fn parse<T: serde::de::DeserializeOwned>(what: &str, reply: &Reply) -> Result<T, IoError> {
    serde_json::from_slice(&reply.body)
        .map_err(|e| IoError::transient(format!("the catalog answered {what} unreadably: {e}")))
}

/// The catalog refused `what`: its status, and the reason its error model gives, or the start of its answer.
fn refused(what: &str, reply: &Reply) -> IoError {
    let answer: Value = serde_json::from_slice(&reply.body).unwrap_or_default();
    let error = &answer["error"];
    let why = match (error["type"].as_str(), error["message"].as_str()) {
        (Some(kind), Some(message)) => format!("{kind}: {message}"),
        (None, Some(message)) => message.to_string(),
        _ => start(&reply.body),
    };
    IoError::transient(format!("the catalog refused {what} ({}): {why}", reply.status))
}

/// The start of an answer, for an error an operator reads.
fn start(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(200)]).trim().to_string()
}

/// The base `from` moves to when the catalog overrides its `uri` with `to`. Held to what a redirect is held to: a
/// whole http:// or https:// URL, and never from https down to plain HTTP.
fn moved_base(from: &str, to: &str) -> Result<String, IoError> {
    let Some(url) = crate::config_file::web_url(to) else {
        return Err(IoError::transient(format!(
            "the catalog moves its base to {to:?}, which is not a whole http:// or https:// URL"
        )));
    };
    if from.get(..8).is_some_and(|s| s.eq_ignore_ascii_case("https://")) && url.scheme() != "https" {
        return Err(IoError::transient(format!(
            "the catalog moves its base from https to {to:?}"
        )));
    }
    Ok(to.trim().trim_end_matches('/').to_string())
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::io::iceberg::metadata::{NestedField, PartitionField, Primitive, StructTag, Transform, Type};

    /// One request as the fake server read it.
    #[derive(Clone, Debug)]
    struct Request {
        /// `GET /v1/config HTTP/1.1`.
        line: String,
        /// Lowercased names.
        headers: Vec<(String, String)>,
        body: String,
    }

    impl Request {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
        }

        fn json(&self) -> Value {
            serde_json::from_str(&self.body).unwrap()
        }
    }

    /// A catalog on 127.0.0.1 that answers each connection with the next of `answers`, in order, and closes it.
    fn serve(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<Request>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in answers {
                let (conn, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(conn);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut headers = Vec::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let header = header.trim_end();
                    if header.is_empty() {
                        break;
                    }
                    let (name, value) = header.split_once(':').unwrap();
                    headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
                }
                let length = headers
                    .iter()
                    .find(|(n, _)| n == "content-length")
                    .map_or(0, |(_, v)| v.parse().unwrap());
                let mut content = vec![0; length];
                reader.read_exact(&mut content).unwrap();
                log.lock().unwrap().push(Request {
                    line: line.trim_end().to_string(),
                    headers,
                    body: String::from_utf8(content).unwrap(),
                });
                let mut conn = reader.into_inner();
                write!(
                    conn,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (url, seen)
    }

    fn catalog(url: &str, warehouse: Option<&str>, auth: Auth) -> Catalog {
        // The environment's proxy would carry a loopback call away.
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        Catalog::new(
            url.to_string(),
            warehouse.map(str::to_string),
            auth,
            agent,
            Duration::from_secs(5),
        )
    }

    fn ok(body: &str) -> (u16, String) {
        (200, body.to_string())
    }

    fn config(prefix: &str) -> (u16, String) {
        ok(&json!({"defaults": {"prefix": "ignored"}, "overrides": {"prefix": prefix}}).to_string())
    }

    const METADATA: &str = r#"{
      "format-version": 2,
      "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
      "location": "s3://bucket/wh/logs",
      "last-column-id": 1,
      "current-schema-id": 0,
      "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "ts", "required": false, "type": "timestamptz"}]}],
      "default-spec-id": 0,
      "partition-specs": [{"spec-id": 0, "fields": []}],
      "last-partition-id": 999
    }"#;

    fn table() -> (u16, String) {
        ok(&format!(
            r#"{{"metadata-location": "s3://bucket/wh/logs/metadata/1.json", "metadata": {METADATA}}}"#
        ))
    }

    fn ns() -> Vec<String> {
        vec!["a".to_string(), "b c".to_string()]
    }

    fn lines(seen: &Arc<Mutex<Vec<Request>>>) -> Vec<String> {
        seen.lock().unwrap().iter().map(|r| r.line.clone()).collect()
    }

    fn client_credentials(expires_in: u64) -> (Auth, (u16, String), (u16, String)) {
        let auth = Auth::ClientCredentials {
            token_url: None,
            client_id: "app".into(),
            client_secret: "s3cr&t".into(),
            scope: None,
        };
        let token =
            |t: &str| ok(&json!({"access_token": t, "token_type": "bearer", "expires_in": expires_in}).to_string());
        (auth, token("one"), token("two"))
    }

    #[test]
    fn the_config_prefix_and_the_bearer_token_go_on_every_call() {
        let (url, seen) = serve(vec![config("wh/1"), table()]);
        let mut catalog = catalog(&url, Some("my wh"), Auth::Bearer("tok".into()));
        let loaded = catalog.load_table(&ns(), "logs x").unwrap().unwrap();
        assert_eq!(loaded.metadata.table_uuid, "9c12d441-03fe-4693-9a96-a0705ddf69c1");
        assert_eq!(
            lines(&seen),
            [
                "GET /v1/config?warehouse=my%20wh HTTP/1.1",
                "GET /v1/wh/1/namespaces/a%1Fb%20c/tables/logs%20x HTTP/1.1",
            ]
        );
        let seen = seen.lock().unwrap();
        assert!(seen.iter().all(|r| r.header("authorization") == Some("Bearer tok")));
        assert_eq!(
            seen[1].header("x-iceberg-access-delegation"),
            Some("vended-credentials")
        );
    }

    /// A signer that answers fixed headers, and keeps what it was asked to sign.
    struct Fixed(Mutex<Vec<(String, String, Vec<u8>)>>);

    impl Signer for Fixed {
        fn sign(&self, method: &str, url: &str, body: &[u8]) -> Result<Vec<(String, String)>, String> {
            self.0.lock().unwrap().push((method.into(), url.into(), body.to_vec()));
            Ok(vec![
                ("Authorization".into(), "AWS4-HMAC-SHA256 signed".into()),
                ("x-amz-date".into(), "20261011T000000Z".into()),
            ])
        }
    }

    #[test]
    fn a_signed_catalog_sends_each_call_with_its_own_signature() {
        let (url, seen) = serve(vec![config(""), table()]);
        let signer = Arc::new(Fixed(Mutex::new(Vec::new())));
        let mut catalog = catalog(&url, None, Auth::Signed(signer.clone()));
        catalog.load_table(&ns(), "logs").unwrap().unwrap();
        let seen = seen.lock().unwrap();
        for request in seen.iter() {
            assert_eq!(request.header("authorization"), Some("AWS4-HMAC-SHA256 signed"));
            assert_eq!(request.header("x-amz-date"), Some("20261011T000000Z"));
        }
        let asked = signer.0.lock().unwrap();
        assert_eq!(asked.len(), 2, "one signature per call");
        assert_eq!(asked[1].0, "GET");
        assert_eq!(asked[1].1, format!("{url}/v1/namespaces/a%1Fb%20c/tables/logs"));
        assert!(asked[1].2.is_empty());
    }

    #[test]
    fn an_overridden_uri_is_the_base_of_every_later_call() {
        let (url, seen) = serve(vec![
            ok(&json!({"defaults": {"uri": "http://default"}, "overrides": {"prefix": "p"}}).to_string()),
            table(),
        ]);
        assert!(catalog_of(&url).load_table(&ns(), "logs").unwrap().is_some());
        assert_eq!(
            lines(&seen)[1],
            "GET /v1/p/namespaces/a%1Fb%20c/tables/logs HTTP/1.1",
            "a default does not move the base"
        );

        let (moved, seen_moved) = serve(vec![table()]);
        let overrides = json!({"overrides": {"uri": format!("{moved}/base/")}}).to_string();
        let (url, seen) = serve(vec![ok(&overrides)]);
        assert!(catalog_of(&url).load_table(&ns(), "logs").unwrap().is_some());
        assert_eq!(lines(&seen), ["GET /v1/config HTTP/1.1"]);
        assert_eq!(
            lines(&seen_moved),
            ["GET /base/v1/namespaces/a%1Fb%20c/tables/logs HTTP/1.1"]
        );
    }

    #[test]
    fn a_table_token_is_sent_until_refused() {
        let with_token = |token: &str| {
            ok(&format!(
                r#"{{"metadata": {METADATA}, "config": {{"token": "{token}"}}}}"#
            ))
        };
        let creds = json!({"storage-credentials": []}).to_string();
        let (url, seen) = serve(vec![
            config(""),
            with_token("t1"),
            ok(&creds),
            (401, "{}".into()),
            ok(&creds),
            with_token("t2"),
            ok(&creds),
        ]);
        let mut catalog = catalog(&url, None, Auth::Bearer("cat".into()));
        catalog.load_table(&ns(), "logs").unwrap().unwrap();
        catalog.credentials(&ns(), "logs").unwrap().unwrap();
        catalog.credentials(&ns(), "logs").unwrap().unwrap();
        catalog.load_table(&ns(), "logs").unwrap().unwrap();
        catalog.credentials(&ns(), "logs").unwrap().unwrap();
        let seen = seen.lock().unwrap();
        let sent: Vec<_> = seen.iter().map(|r| r.header("authorization").unwrap()).collect();
        assert_eq!(
            sent,
            [
                "Bearer cat",
                "Bearer cat",
                "Bearer t1",
                "Bearer t1",
                "Bearer cat",
                "Bearer cat",
                "Bearer t2",
            ]
        );
    }

    #[test]
    fn client_credentials_sign_in_and_sign_in_again_when_refused() {
        let (auth, one, two) = client_credentials(3600);
        let (url, seen) = serve(vec![one, config(""), (401, "{}".into()), two, table()]);
        let mut catalog = catalog(&url, None, auth);
        assert!(catalog.load_table(&ns(), "logs").unwrap().is_some());
        assert_eq!(
            lines(&seen),
            [
                "POST /v1/oauth/tokens HTTP/1.1",
                "GET /v1/config HTTP/1.1",
                "GET /v1/namespaces/a%1Fb%20c/tables/logs HTTP/1.1",
                "POST /v1/oauth/tokens HTTP/1.1",
                "GET /v1/namespaces/a%1Fb%20c/tables/logs HTTP/1.1",
            ]
        );
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[0].body, "grant_type=client_credentials&client_id=app&client_secret=s3cr%26t",
            "no scope is sent unless one is given"
        );
        assert_eq!(seen[1].header("authorization"), Some("Bearer one"));
        assert_eq!(seen[2].header("authorization"), Some("Bearer one"));
        assert_eq!(seen[4].header("authorization"), Some("Bearer two"));
    }

    #[test]
    fn vended_credentials_are_read_from_the_table() {
        let body = format!(
            r#"{{"metadata": {METADATA}, "config": {{"s3.endpoint": "http://minio"}},
                "storage-credentials": [{{"prefix": "s3://bucket/wh", "config": {{"s3.access-key-id": "AK", "s3.secret-access-key": "SK"}}}}]}}"#
        );
        let (url, _) = serve(vec![config(""), ok(&body)]);
        let loaded = catalog(&url, None, Auth::None)
            .load_table(&ns(), "logs")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.metadata_location, None);
        assert_eq!(loaded.config["s3.endpoint"], "http://minio");
        assert_eq!(loaded.storage_credentials.len(), 1);
        assert_eq!(loaded.storage_credentials[0].prefix, "s3://bucket/wh");
        assert_eq!(loaded.storage_credentials[0].config["s3.secret-access-key"], "SK");
        assert!(
            !format!("{loaded:?}").contains("SK"),
            "a credential's value is never shown"
        );
    }

    #[test]
    fn credentials_are_asked_for_where_the_catalog_serves_them() {
        let creds = json!({"storage-credentials": [{"prefix": "s3://b", "config": {"k": "v"}}]}).to_string();
        let (url, seen) = serve(vec![config("p"), ok(&creds), (404, "{}".into())]);
        let mut catalog = catalog(&url, None, Auth::None);
        assert_eq!(catalog.credentials(&ns(), "logs").unwrap().unwrap()[0].prefix, "s3://b");
        assert_eq!(catalog.credentials(&ns(), "logs").unwrap(), None);
        assert_eq!(
            lines(&seen)[1],
            "GET /v1/p/namespaces/a%1Fb%20c/tables/logs/credentials HTTP/1.1"
        );

        let listed = json!({"endpoints": ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"]}).to_string();
        let (url, seen) = serve(vec![ok(&listed)]);
        assert_eq!(catalog_of(&url).credentials(&ns(), "logs").unwrap(), None);
        assert_eq!(
            lines(&seen).len(),
            1,
            "an endpoint the catalog does not list is not asked"
        );
    }

    #[test]
    fn an_overridden_base_is_held_to_what_a_redirect_is() {
        assert_eq!(moved_base("https://a", " https://b/base/ ").unwrap(), "https://b/base");
        assert_eq!(moved_base("http://a", "http://b").unwrap(), "http://b");
        assert_eq!(moved_base("http://a", "https://b").unwrap(), "https://b");
        let refused = |from, to| moved_base(from, to).unwrap_err().to_string();
        assert!(refused("https://a", "HTTP://b").contains("from https to"));
        for to in ["ftp://b", "b/base", "", "http://"] {
            assert!(
                refused("http://a", to).contains("not a whole http:// or https:// URL"),
                "{to}"
            );
        }
    }

    fn catalog_of(url: &str) -> Catalog {
        catalog(url, None, Auth::None)
    }

    fn schema() -> Schema {
        Schema {
            kind: StructTag,
            schema_id: 0,
            identifier_field_ids: vec![],
            fields: vec![NestedField {
                id: 1,
                name: "ts".into(),
                required: false,
                field_type: Type::Primitive(Primitive::Timestamptz),
                doc: None,
            }],
        }
    }

    fn spec(source_id: i32) -> PartitionSpec {
        PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                name: "ts_day".into(),
                transform: Transform::Day,
                source_id,
                field_id: 1000,
            }],
        }
    }

    #[test]
    fn a_table_created_by_another_writer_first_is_loaded() {
        let (url, seen) = serve(vec![config(""), (409, "{}".into()), table()]);
        let properties = BTreeMap::from([("write.format.default".to_string(), "parquet".to_string())]);
        let loaded = catalog_of(&url)
            .create_table(&ns(), "logs", &schema(), &spec(1), &properties)
            .unwrap();
        assert_eq!(
            loaded.metadata_location.as_deref(),
            Some("s3://bucket/wh/logs/metadata/1.json")
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen[1].line, "POST /v1/namespaces/a%1Fb%20c/tables HTTP/1.1");
        assert_eq!(
            seen[1].header("x-iceberg-access-delegation"),
            Some("vended-credentials")
        );
        assert_eq!(
            seen[1].json(),
            json!({
                "name": "logs",
                "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "ts", "required": false, "type": "timestamptz"}]},
                "partition-spec": {"spec-id": 0, "fields": [{"name": "ts_day", "transform": "day", "source-id": 1, "field-id": 1000}]},
                "stage-create": false,
                "properties": {"write.format.default": "parquet"},
            })
        );
        assert_eq!(seen[2].line, "GET /v1/namespaces/a%1Fb%20c/tables/logs HTTP/1.1");
    }

    #[test]
    fn a_spec_naming_a_column_the_schema_lacks_is_not_sent() {
        let (url, seen) = serve(vec![]);
        let error = catalog_of(&url)
            .create_table(&ns(), "logs", &schema(), &spec(7), &BTreeMap::new())
            .unwrap_err();
        assert!(error.to_string().contains("ts_day"), "{error}");
        assert!(lines(&seen).is_empty());
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            snapshot_id: 7,
            parent_snapshot_id: Some(6),
            sequence_number: 3,
            timestamp_ms: 1_700_000_000_000,
            manifest_list: "s3://bucket/wh/logs/metadata/snap-7.avro".into(),
            summary: BTreeMap::from([("operation".to_string(), "append".to_string())]),
            schema_id: Some(1),
        }
    }

    fn commit(answer: (u16, String)) -> (Result<Committed, CommitFailure>, Arc<Mutex<Vec<Request>>>) {
        let (url, seen) = serve(vec![config("p"), answer]);
        let result = catalog_of(&url).commit(
            &ns(),
            "logs",
            &[
                Requirement::AssertTableUuid("u".into()),
                Requirement::AssertRefSnapshotId {
                    r#ref: "main".into(),
                    snapshot_id: Some(6),
                },
                Requirement::AssertRefSnapshotId {
                    r#ref: "audit".into(),
                    snapshot_id: None,
                },
                Requirement::AssertCurrentSchemaId(0),
                Requirement::AssertLastAssignedFieldId(1),
            ],
            &[
                Update::AddSchema {
                    schema: Schema {
                        schema_id: 1,
                        ..schema()
                    },
                    last_column_id: 1,
                },
                Update::SetCurrentSchema(-1),
                Update::AddSnapshot(snapshot()),
                Update::SetSnapshotRef {
                    name: "main".into(),
                    reference: SnapshotRef {
                        snapshot_id: 7,
                        kind: "branch".into(),
                        max_ref_age_ms: Some(86_400_000),
                        ..SnapshotRef::default()
                    },
                },
            ],
        );
        (result, seen)
    }

    #[test]
    fn a_commit_is_sent_as_the_spec_shapes_it() {
        let (result, seen) = commit(table());
        assert_eq!(result.unwrap().metadata_location, "s3://bucket/wh/logs/metadata/1.json");
        let seen = seen.lock().unwrap();
        assert_eq!(seen[1].line, "POST /v1/p/namespaces/a%1Fb%20c/tables/logs HTTP/1.1");
        assert_eq!(seen[1].header("content-type"), Some("application/json"));
        assert_eq!(
            seen[1].json(),
            json!({
                "identifier": {"namespace": ["a", "b c"], "name": "logs"},
                "requirements": [
                    {"type": "assert-table-uuid", "uuid": "u"},
                    {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 6},
                    {"type": "assert-ref-snapshot-id", "ref": "audit", "snapshot-id": null},
                    {"type": "assert-current-schema-id", "current-schema-id": 0},
                    {"type": "assert-last-assigned-field-id", "last-assigned-field-id": 1},
                ],
                "updates": [
                    {"action": "add-schema", "last-column-id": 1, "schema": {"type": "struct", "schema-id": 1,
                        "fields": [{"id": 1, "name": "ts", "required": false, "type": "timestamptz"}]}},
                    {"action": "set-current-schema", "schema-id": -1},
                    {"action": "add-snapshot", "snapshot": {"snapshot-id": 7, "parent-snapshot-id": 6,
                        "sequence-number": 3, "timestamp-ms": 1_700_000_000_000_i64,
                        "manifest-list": "s3://bucket/wh/logs/metadata/snap-7.avro",
                        "summary": {"operation": "append"}, "schema-id": 1}},
                    {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 7, "type": "branch",
                     "max-ref-age-ms": 86_400_000},
                ],
            })
        );
    }

    #[test]
    fn a_commit_refused_says_whether_it_may_have_landed() {
        let error = |kind: &str| json!({"error": {"message": "m", "type": kind, "code": 0}}).to_string();
        assert_eq!(
            commit((409, error("CommitFailedException"))).0,
            Err(CommitFailure::Conflict)
        );
        for status in [500, 502, 503, 504] {
            match commit((status, error("CommitStateUnknownException"))).0 {
                Err(CommitFailure::Unknown(why)) => {
                    assert!(why.contains("CommitStateUnknownException: m"), "{why}")
                }
                other => panic!("{status}: {other:?}"),
            }
        }
        assert!(matches!(
            commit((400, error("BadRequestException"))).0,
            Err(CommitFailure::Failed(_))
        ));
    }

    #[test]
    fn a_commit_with_no_answer_may_have_landed() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                if held.is_empty() {
                    let mut request = [0; 4096];
                    let _ = conn.read(&mut request);
                    let body = r#"{"overrides": {}}"#;
                    write!(
                        conn,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                }
                // The commit is read and never answered.
                held.push(conn);
            }
        });
        let agent = ureq::Agent::config_builder().proxy(None).build().new_agent();
        let mut catalog = Catalog::new(url, None, Auth::None, agent, Duration::from_millis(300));
        assert!(matches!(
            catalog.commit(&ns(), "logs", &[], &[]),
            Err(CommitFailure::Unknown(_))
        ));
    }
}
