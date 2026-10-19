//! What the object store fuzzers share: S3, Cloud Storage, Azure Blob Storage and SFTP access drawn as a bundle
//! names it, each setting breakable, the credentials a worker holds for it, and the check that what loaded is what
//! was drawn.
//!
//! A destination and a source flatten the same access settings into their own JSON object, so each draw here writes
//! into that object.

use serde_json::{Map, Value, json};
use smeltr::plan::topology::{
    AzureAccess, AzureCredential, GcsAccess, HeldSecrets, S3Access, SecretRef, SftpAccess, SftpCredential,
};

pub use crate::harness::destination::{Broken, Reached, check_save, connector_secrets, refusals};
use crate::harness::fuzz::Rng;

pub const AZURE_ACCOUNT: &str = "devstoreaccount1";
/// Azurite's account, and a SAS for its blobs and queues signed with the account's published key, good until 2099.
pub const AZURE_SAS: &str = "sv=2021-08-06&ss=bq&srt=sco&sp=racwdlup&se=2099-01-01T00%3A00%3A00Z&spr=https%2Chttp&\
                             sig=%2BMnHWtCOrvDBlW0BOdq7P%2BNdl9UZ3hedCH6rBnGFXq0%3D";
/// Azurite's published account key.
pub const AZURE_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// A service account key that turns OAuth off, as the emulator takes no token.
pub const GCS_KEY: &str = r#"{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true}"#;
pub const SFTP_HOST_KEY: &str = include_str!("live/sftp/ssh_host_ed25519_key.pub");
pub const SFTP_ED25519: &str = include_str!("live/sftp/client_ed25519");
pub const SFTP_PASSWORD: &str = "smeltr-live-1";

/// Every setting that names a credential.
pub const SECRET_FIELDS: &[&str] = &[
    "accessKeyId",
    "secretAccessKey",
    "serviceAccountKey",
    "accessKey",
    "sasToken",
    "clientSecret",
    "password",
    "privateKey",
];

/// A worker-local credential no test sets, so a client naming it cannot build.
pub const UNSET_VARIABLE: &str = "SMELTR_FUZZ_NEVER_SET";

/// The coverage names of every access option and every break of one, for a fuzzer's own list.
pub const ACCESS_OPTIONS: &[&str] = &[
    "secret.leaderStored",
    "secret.workerLocal",
    "s3.region",
    "s3.endpoint",
    "s3.accessKey",
    "s3.roleArn",
    "s3.externalId",
    "s3.authEndpoint",
    "gcs.endpoint",
    "gcs.serviceAccountKey",
    "azure.endpoint",
    "azure.accessKey",
    "azure.sasToken",
    "azure.servicePrincipal",
    "azure.authEndpoint",
    "azure.managedIdentity",
    "sftp.directory",
    "sftp.port",
    "sftp.password",
    "sftp.privateKey",
    "broken.endpoint",
    "broken.s3.halfKey",
    "broken.s3.roleArn",
    "broken.s3.empty",
    "broken.azure.account",
    "broken.azure.principal",
    "broken.azure.credentials",
    "broken.azure.authEndpoint",
    "broken.azure.managedIdentity",
    "broken.sftp.host",
    "broken.sftp.port",
    "broken.sftp.username",
    "broken.sftp.signIn",
    "broken.sftp.hostKey",
];

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Store {
    S3,
    Gcs,
    Azure,
    Sftp,
}

/// Where a draw is pointed.
#[derive(Clone, Copy)]
pub enum Target<'a> {
    /// Anywhere at all: nothing is reached.
    Offline,
    /// The emulator at this address, signed in as it takes, and only what it serves.
    Live(&'a str),
}

impl Target<'_> {
    pub fn live(&self) -> bool {
        matches!(self, Target::Live(_))
    }
}

/// A credential named `name`: a value the leader holds, or offline sometimes a variable no worker sets.
pub fn secret(rng: &mut Rng, cov: &mut Reached, target: Target, name: &str) -> Value {
    if !target.live() && rng.chance(5) {
        cov.reach("secret.workerLocal");
        return json!(UNSET_VARIABLE);
    }
    cov.reach("secret.leaderStored");
    json!({"secret": name, "version": 1 + rng.below(3)})
}

/// What the credential named `name` holds.
fn secret_value(name: &str) -> &'static str {
    match name {
        "s3-id" | "s3-key" => "test",
        "gcs-key" => GCS_KEY,
        "azure-key" => AZURE_KEY,
        "azure-sas" => AZURE_SAS,
        "azure-client" => "client-secret",
        "sftp-password" => SFTP_PASSWORD,
        "sftp-key" => SFTP_ED25519,
        other => panic!("no value for the credential {other}"),
    }
}

/// An address a cloud service is reached at: https anywhere, or plain http on this host.
pub fn address(rng: &mut Rng) -> String {
    rng.pick(&[
        "https://store.example.test",
        "https://store.example.test:8443/base",
        "http://127.0.0.1:9000",
        "http://localhost:4566",
        "http://[::1]:10000",
    ])
    .to_string()
}

/// Draws `store`'s access into `d`: where it is reached and how it signs in.
pub fn draw_access(rng: &mut Rng, cov: &mut Reached, target: Target, store: Store, d: &mut Map<String, Value>) {
    match store {
        Store::S3 => draw_s3(rng, cov, target, d),
        Store::Gcs => draw_gcs(rng, cov, target, d),
        Store::Azure => draw_azure(rng, cov, target, d),
        Store::Sftp => draw_sftp(rng, cov, target, d),
    }
}

fn draw_s3(rng: &mut Rng, cov: &mut Reached, target: Target, d: &mut Map<String, Value>) {
    if let Target::Live(endpoint) = target {
        // LocalStack takes any key, and answers STS at its one address.
        d.insert("region".into(), json!("us-east-1"));
        d.insert("endpoint".into(), json!(endpoint));
        d.insert("accessKeyId".into(), secret(rng, cov, target, "s3-id"));
        d.insert("secretAccessKey".into(), secret(rng, cov, target, "s3-key"));
        if rng.chance(3) {
            cov.reach("s3.roleArn");
            d.insert("roleArn".into(), json!("arn:aws:iam::000000000000:role/writer"));
            d.insert("authEndpoint".into(), json!(endpoint));
            if rng.chance(2) {
                d.insert("externalId".into(), json!("smeltr-fuzz"));
            }
        }
        return;
    }
    if rng.chance(2) {
        cov.reach("s3.region");
        d.insert(
            "region".into(),
            json!(rng.pick(&["us-east-1", "eu-west-2", "us-gov-west-1"])),
        );
    }
    if rng.chance(2) {
        cov.reach("s3.endpoint");
        d.insert("endpoint".into(), json!(address(rng)));
    }
    if rng.chance(2) {
        cov.reach("s3.accessKey");
        d.insert("accessKeyId".into(), secret(rng, cov, target, "s3-id"));
        d.insert("secretAccessKey".into(), secret(rng, cov, target, "s3-key"));
    }
    if rng.chance(3) {
        cov.reach("s3.roleArn");
        d.insert("roleArn".into(), json!("arn:aws:iam::123456789012:role/writer"));
        if rng.chance(2) {
            cov.reach("s3.externalId");
            d.insert("externalId".into(), json!("ext-1"));
        }
        if rng.chance(2) {
            cov.reach("s3.authEndpoint");
            d.insert("authEndpoint".into(), json!(address(rng)));
        }
    }
}

fn draw_gcs(rng: &mut Rng, cov: &mut Reached, target: Target, d: &mut Map<String, Value>) {
    if let Target::Live(endpoint) = target {
        d.insert("endpoint".into(), json!(endpoint));
        d.insert("serviceAccountKey".into(), secret(rng, cov, target, "gcs-key"));
        return;
    }
    if rng.chance(2) {
        cov.reach("gcs.endpoint");
        d.insert("endpoint".into(), json!(address(rng)));
    }
    if rng.chance(2) {
        cov.reach("gcs.serviceAccountKey");
        d.insert("serviceAccountKey".into(), secret(rng, cov, target, "gcs-key"));
    }
}

fn draw_azure(rng: &mut Rng, cov: &mut Reached, target: Target, d: &mut Map<String, Value>) {
    if let Target::Live(endpoint) = target {
        d.insert("account".into(), json!(AZURE_ACCOUNT));
        d.insert("endpoint".into(), json!(endpoint));
        if rng.chance(2) {
            cov.reach("azure.accessKey");
            d.insert("accessKey".into(), secret(rng, cov, target, "azure-key"));
        } else {
            cov.reach("azure.sasToken");
            d.insert("sasToken".into(), secret(rng, cov, target, "azure-sas"));
        }
        return;
    }
    d.insert("account".into(), json!(rng.pick(&["acme", "devstoreaccount1"])));
    if rng.chance(2) {
        cov.reach("azure.endpoint");
        d.insert("endpoint".into(), json!(address(rng)));
    }
    match rng.below(5) {
        0 => {
            cov.reach("azure.accessKey");
            d.insert("accessKey".into(), secret(rng, cov, target, "azure-key"));
        }
        1 => {
            cov.reach("azure.sasToken");
            d.insert("sasToken".into(), secret(rng, cov, target, "azure-sas"));
        }
        2 => {
            cov.reach("azure.servicePrincipal");
            d.insert("tenantId".into(), json!("00000000-0000-0000-0000-000000000001"));
            d.insert("clientId".into(), json!("00000000-0000-0000-0000-000000000002"));
            d.insert("clientSecret".into(), secret(rng, cov, target, "azure-client"));
            if rng.chance(2) {
                cov.reach("azure.authEndpoint");
                // A sign-in host names no path: the tenant follows it.
                let host = address(rng);
                let host = host.trim_end_matches("/base");
                d.insert("authEndpoint".into(), json!(host));
            }
        }
        3 => {
            cov.reach("azure.managedIdentity");
            d.insert(
                "managedIdentityClientId".into(),
                json!("00000000-0000-0000-0000-000000000003"),
            );
        }
        _ => {}
    }
}

fn draw_sftp(rng: &mut Rng, cov: &mut Reached, target: Target, d: &mut Map<String, Value>) {
    match target {
        Target::Live(address) => {
            let (host, port) = address.rsplit_once(':').expect("host:port");
            d.insert("host".into(), json!(host));
            d.insert("port".into(), json!(port.parse::<u32>().expect("a port")));
            d.insert("username".into(), json!("smeltr"));
            d.insert("hostKey".into(), json!(SFTP_HOST_KEY.trim()));
        }
        Target::Offline => {
            d.insert(
                "host".into(),
                json!(rng.pick(&["sftp.example.test", "10.0.0.5", "::1"])),
            );
            if rng.chance(2) {
                cov.reach("sftp.port");
                d.insert("port".into(), json!(rng.pick(&[1, 22, 2222, 65535])));
            }
            d.insert("username".into(), json!(rng.pick(&["smeltr", "svc.logs"])));
            d.insert("hostKey".into(), json!(SFTP_HOST_KEY.trim()));
            if rng.chance(2) {
                cov.reach("sftp.directory");
                d.insert(
                    "directory".into(),
                    json!(rng.pick(&["upload", "/srv/logs", "upload//a/", " upload/b "])),
                );
            }
        }
    }
    if rng.chance(2) {
        cov.reach("sftp.password");
        d.insert("password".into(), secret(rng, cov, target, "sftp-password"));
    } else {
        cov.reach("sftp.privateKey");
        d.insert("privateKey".into(), secret(rng, cov, target, "sftp-key"));
    }
}

/// The breaks of `store`'s access, by the name `break_access` takes.
pub fn access_breaks(store: Store) -> &'static [&'static str] {
    match store {
        Store::S3 => &["endpoint", "s3.halfKey", "s3.roleArn", "s3.empty"],
        Store::Gcs => &["endpoint"],
        Store::Azure => &[
            "endpoint",
            "azure.account",
            "azure.principal",
            "azure.credentials",
            "azure.authEndpoint",
            "azure.managedIdentity",
        ],
        Store::Sftp => &["sftp.host", "sftp.port", "sftp.username", "sftp.signIn", "sftp.hostKey"],
    }
}

/// Breaks `what`, one of `access_breaks(store)`, in `d`, and answers the field the save must refuse it on. `None` is
/// refused on no one field.
pub fn break_access(rng: &mut Rng, what: &str, d: &mut Map<String, Value>) -> Option<&'static str> {
    let mut set = |k: &str, v: Value| {
        d.insert(k.to_string(), v);
    };
    match what {
        "endpoint" => {
            set("endpoint", json!(rng.pick(&["", " "])));
            Some("endpoint")
        }
        "s3.halfKey" => {
            d.remove("accessKeyId");
            d.remove("secretAccessKey");
            if rng.chance(2) {
                d.insert("accessKeyId".into(), json!("AWS_KEY_ID"));
                Some("secretAccessKey")
            } else {
                d.insert("secretAccessKey".into(), json!("AWS_SECRET"));
                Some("accessKeyId")
            }
        }
        "s3.roleArn" => {
            d.remove("roleArn");
            if rng.chance(2) {
                d.insert("externalId".into(), json!("x"));
            } else {
                d.insert("authEndpoint".into(), json!("https://sts.example.test"));
            }
            Some("roleArn")
        }
        "s3.empty" => {
            let field = *rng.pick(&["region", "roleArn", "externalId"]);
            if field == "externalId" {
                d.insert("roleArn".into(), json!("arn:aws:iam::123456789012:role/writer"));
            }
            d.insert(field.into(), json!(" "));
            Some(match field {
                "region" => "region",
                "roleArn" => "roleArn",
                _ => "externalId",
            })
        }
        "azure.account" => {
            set("account", json!(" "));
            Some("account")
        }
        "azure.principal" => {
            for k in ["accessKey", "sasToken", "authEndpoint", "managedIdentityClientId"] {
                d.remove(k);
            }
            d.insert("tenantId".into(), json!("t"));
            d.insert("clientId".into(), json!("c"));
            d.insert("clientSecret".into(), json!("AZURE_CLIENT_SECRET"));
            let dropped = *rng.pick(&["tenantId", "clientId", "clientSecret"]);
            d.remove(dropped);
            Some(match dropped {
                "tenantId" => "tenantId",
                "clientId" => "clientId",
                _ => "clientSecret",
            })
        }
        "azure.credentials" => {
            for k in [
                "tenantId",
                "clientId",
                "clientSecret",
                "authEndpoint",
                "managedIdentityClientId",
            ] {
                d.remove(k);
            }
            d.insert("accessKey".into(), json!("AZURE_KEY"));
            d.insert("sasToken".into(), json!("AZURE_SAS"));
            Some("accessKey")
        }
        "azure.authEndpoint" => {
            for k in ["tenantId", "clientId", "clientSecret"] {
                d.remove(k);
            }
            d.insert("authEndpoint".into(), json!("https://login.example.test"));
            Some("clientSecret")
        }
        "azure.managedIdentity" => {
            for k in [
                "accessKey",
                "sasToken",
                "tenantId",
                "clientId",
                "clientSecret",
                "authEndpoint",
            ] {
                d.remove(k);
            }
            d.insert(
                "managedIdentityClientId".into(),
                json!(rng.pick(&["writer", "id&resource=x"])),
            );
            Some("managedIdentityClientId")
        }
        "sftp.host" => {
            set("host", json!(" "));
            Some("host")
        }
        "sftp.port" => {
            set("port", json!(rng.pick(&[0, 65536, 4_000_000_000u32])));
            Some("port")
        }
        "sftp.username" => {
            set("username", json!(""));
            Some("username")
        }
        "sftp.signIn" => {
            if rng.chance(2) {
                d.remove("password");
                d.remove("privateKey");
            } else {
                d.insert("password".into(), json!("SFTP_PASSWORD"));
                d.insert("privateKey".into(), json!("SFTP_KEY"));
            }
            Some("password")
        }
        "sftp.hostKey" => {
            set(
                "hostKey",
                json!(rng.pick(&["", "ssh-ed25519", "not a key", "ssh-ed25519 AAAA"])),
            );
            Some("hostKey")
        }
        other => unreachable!("{other}"),
    }
}

/// One authority's PEM, minted once for every seed: what a declared identity trusts.
pub fn authority_pem() -> &'static str {
    static PEM: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PEM.get_or_init(|| crate::certs::Authority::new("smeltr fuzz").pem)
}

/// What the worker holds of the credentials `d` names, and whether it names one it does not hold: it holds every one
/// the leader stores, and never a worker-local one.
pub fn held(d: &Map<String, Value>) -> (HeldSecrets, bool) {
    let mut stored = Vec::new();
    let mut unheld = false;
    for v in SECRET_FIELDS.iter().filter_map(|k| d.get(*k)) {
        match v {
            Value::Object(o) => {
                let name = o["secret"].as_str().unwrap().to_string();
                let value = secret_value(&name).to_string();
                stored.push((name, o["version"].as_i64().unwrap(), value));
            }
            _ => unheld = true,
        }
    }
    let mut secrets = HeldSecrets::default();
    secrets.set_stored(stored);
    (secrets, unheld)
}

/// `d`'s unsigned `key`, or `default` when it names none.
pub fn uint(d: &Map<String, Value>, key: &str, default: u64) -> u64 {
    d.get(key).map_or(default, |v| v.as_u64().unwrap())
}

/// What a draw says against what loaded, a line per difference.
#[derive(Default)]
pub struct Compare(Vec<String>);

impl Compare {
    pub fn expect(&mut self, what: &str, drawn: impl std::fmt::Debug, loaded: impl std::fmt::Debug) {
        let (drawn, loaded) = (format!("{drawn:?}"), format!("{loaded:?}"));
        if drawn != loaded {
            self.0.push(format!("{what}: drawn {drawn}, loaded {loaded}"));
        }
    }

    pub fn done(self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(self.0.join("\n"))
        }
    }
}

/// `d`'s string `key`.
pub fn text(d: &Map<String, Value>, key: &str) -> Option<String> {
    d.get(key).map(|v| v.as_str().unwrap().to_string())
}

pub fn compare_s3(c: &mut Compare, d: &Map<String, Value>, a: &S3Access) {
    c.expect("region", text(d, "region"), &a.region);
    c.expect("endpoint", text(d, "endpoint"), &a.endpoint);
    c.expect("authEndpoint", text(d, "authEndpoint"), &a.auth_endpoint);
    c.expect("accessKey", d.contains_key("accessKeyId"), a.access_key.is_some());
    c.expect("roleArn", text(d, "roleArn"), a.role.as_ref().map(|r| r.arn.clone()));
    c.expect(
        "externalId",
        text(d, "externalId"),
        a.role.as_ref().and_then(|r| r.external_id.clone()),
    );
}

pub fn compare_gcs(c: &mut Compare, d: &Map<String, Value>, a: &GcsAccess) {
    c.expect("endpoint", text(d, "endpoint"), &a.endpoint);
    c.expect(
        "serviceAccountKey",
        d.contains_key("serviceAccountKey"),
        a.service_account_key.is_some(),
    );
}

pub fn compare_azure(c: &mut Compare, d: &Map<String, Value>, a: &AzureAccess) {
    c.expect("account", text(d, "account").unwrap(), &a.account);
    c.expect("endpoint", text(d, "endpoint"), &a.endpoint);
    c.expect("authEndpoint", text(d, "authEndpoint"), &a.auth_endpoint);
    let credential = match &a.credential {
        None => "none",
        Some(AzureCredential::AccessKey(_)) => "accessKey",
        Some(AzureCredential::SasToken(_)) => "sasToken",
        Some(AzureCredential::ClientSecret { .. }) => "clientSecret",
        Some(AzureCredential::ManagedIdentity { .. }) => "managedIdentityClientId",
    };
    let drawn = ["accessKey", "sasToken", "clientSecret", "managedIdentityClientId"]
        .into_iter()
        .find(|k| d.contains_key(*k))
        .unwrap_or("none");
    c.expect("credential", drawn, credential);
    if let Some(AzureCredential::ClientSecret {
        tenant_id, client_id, ..
    }) = &a.credential
    {
        c.expect("tenantId", text(d, "tenantId").unwrap_or_default(), tenant_id);
        c.expect("clientId", text(d, "clientId").unwrap_or_default(), client_id);
    }
    if let Some(AzureCredential::ManagedIdentity { client_id }) = &a.credential {
        c.expect(
            "managedIdentityClientId",
            text(d, "managedIdentityClientId").unwrap_or_default(),
            client_id,
        );
    }
}

/// An SFTP server's access, and `directory` as `d` names it, normalized as load does.
pub fn compare_sftp(c: &mut Compare, d: &Map<String, Value>, directory: &str, a: &SftpAccess) {
    let drawn_directory = text(d, "directory").unwrap_or_default();
    let parts: Vec<&str> = drawn_directory.trim().split('/').filter(|p| !p.is_empty()).collect();
    let lead = if drawn_directory.trim().starts_with('/') {
        "/"
    } else {
        ""
    };
    c.expect("directory", format!("{lead}{}", parts.join("/")), directory);
    c.expect("port", uint(d, "port", 22), a.port);
    let credential = match &a.credential {
        SftpCredential::Password(_) => "password",
        SftpCredential::PrivateKey(_) => "privateKey",
    };
    let drawn = if d.contains_key("password") {
        "password"
    } else {
        "privateKey"
    };
    c.expect("signIn", drawn, credential);
    c.expect("host", text(d, "host").unwrap(), &a.host);
    c.expect("username", text(d, "username").unwrap(), &a.username);
    c.expect("hostKey", text(d, "hostKey").unwrap(), &a.host_key);
}

/// Every credential `d` names against every one the loaded bundle names for its connector, by where each lives.
pub fn compare_credentials(c: &mut Compare, d: &Map<String, Value>, named: &[SecretRef]) {
    let mut drawn: Vec<String> = SECRET_FIELDS
        .iter()
        .filter_map(|k| d.get(*k))
        .map(|v| match v {
            Value::String(var) => format!("{:?}", SecretRef::WorkerLocal(var.clone())),
            v => format!(
                "{:?}",
                SecretRef::LeaderStored {
                    name: v["secret"].as_str().unwrap().to_string(),
                    version: v["version"].as_i64().unwrap(),
                }
            ),
        })
        .collect();
    let mut loaded: Vec<String> = named.iter().map(|r| format!("{r:?}")).collect();
    drawn.sort();
    loaded.sort();
    c.expect("credentials", drawn, loaded);
}
