//! Telling people of warnings where they already look: each alert channel is a webhook or a mailbox, and is told
//! when a warning it takes is raised and again when it clears.
//!
//! The leader looks on its own timer, so a fleet whose workers have all gone silent is still told. Each look reads
//! the catalog, works out what each channel should know, and compares that with what the channel was told
//! (`alerts_told` in the store). A row is written only once the channel took the send, so a send that failed is
//! made again on the next look, and a restarted leader neither repeats nor drops one.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use super::envelope::{Envelope, Refusal};
use super::store::{Actor, AlertTold, Store, StoreError};
use super::warnings::{Condition, Warning};
use super::{AppState, run_store};

/// How often the leader looks, and so how soon a failed send is made again.
const EVERY: Duration = Duration::from_secs(60);

/// How long one send may take, its answer included.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The setting the channels are kept in, as JSON.
pub const CHANNELS_KEY: &str = "alerts.channels";

/// The setting a webhook's secret header value is sealed under.
pub fn secret_key(channel: &str) -> String {
    format!("alerts.channel.{channel}.secret")
}

/// The setting the mail server is kept in, as JSON.
pub const MAIL_KEY: &str = "alerts.mail";

/// The setting the mail server's password is sealed under.
pub const MAIL_PASSWORD_KEY: &str = "alerts.mail.password";

/// The one mail server every email channel sends through.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mail {
    /// Off, no email is sent, and what email channels would be told waits until it is on.
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub encryption: Encryption,
    pub from: String,
    /// Who to sign in as. `None` sends without signing in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// How the connection to the mail server is encrypted. Always encrypted: the password and what is sent cross it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Encryption {
    /// Plain at first, then upgraded; refused if the server cannot upgrade. Usually port 587.
    StartTls,
    /// TLS from the first byte. Usually port 465.
    Tls,
}

/// The mail server as stored, when one is and it is on.
pub fn mail(store: &dyn Store) -> Result<Option<Mail>, StoreError> {
    let Some(text) = store.setting(MAIL_KEY)? else {
        return Ok(None);
    };
    let mail: Mail = serde_json::from_str(&text).map_err(|e| StoreError::failed(format!("{MAIL_KEY}: {e}")))?;
    Ok(mail.enabled.then_some(mail))
}

/// One place warnings are sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Channel {
    /// Names the channel in the store and in its sealed secret. Its name can change; this cannot.
    pub id: String,
    pub name: String,
    pub kind: Kind,
    /// The groups whose warnings it takes. `None` takes every group's, and the warnings about the leader itself,
    /// which name no group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,
    /// Whether it is sent a warning's message, which can quote a source's or destination's error and so a
    /// record's values.
    #[serde(default)]
    pub include_message: bool,
    /// The catalog entries it takes.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Epoch milliseconds until which it is sent nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted_until: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Kind {
    #[serde(rename_all = "camelCase")]
    Webhook {
        url: String,
        format: Format,
        /// The header that carries the sealed secret, when there is one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        secret_header: Option<String>,
    },
    Email {
        to: Vec<String>,
    },
}

/// What a webhook's body looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Format {
    /// Each warning as a JSON object, its fields as the catalog names them.
    GenericJson,
    /// A Slack incoming webhook's message.
    Slack,
    /// A Microsoft Teams workflow's adaptive card.
    Teams,
    /// A PagerDuty Events API v2 event, which opens an incident and resolves it. The channel's secret is its
    /// routing key, sent in the body rather than as a header.
    PagerDuty,
}

/// A send that failed, kept until the channel takes one, for the channel's own warning and the page.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Failing {
    /// The channel's name when it failed.
    pub channel: String,
    /// When the first send in this run of failures failed.
    pub since: i64,
    /// When the last one did.
    pub last: i64,
    pub message: String,
}

/// What the leader holds about its alerts between looks.
#[derive(Default)]
pub struct Alerts {
    failing: Mutex<HashMap<String, Failing>>,
    /// Wakes the timer when the channels change, so a new channel is told at once.
    pub settings_changed: tokio::sync::Notify,
}

impl Alerts {
    fn held(&self) -> std::sync::MutexGuard<'_, HashMap<String, Failing>> {
        self.failing.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Each channel whose last send failed, by id.
    pub fn failing(&self) -> BTreeMap<String, Failing> {
        self.held().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    fn failed(&self, channel: &Channel, message: String, now: i64) {
        let mut held = self.held();
        let since = held.get(&channel.id).map_or(now, |f| f.since);
        let failing = Failing {
            channel: channel.name.clone(),
            since,
            last: now,
            message,
        };
        held.insert(channel.id.clone(), failing);
    }

    fn took(&self, channel: &str) {
        self.held().remove(channel);
    }

    /// Forget the failures of channels that are gone.
    fn keep(&self, ids: &[String]) {
        self.held().retain(|id, _| ids.contains(id));
    }
}

/// The channels as stored. None stored is none at all.
pub fn channels(store: &dyn Store) -> Result<Vec<Channel>, StoreError> {
    match store.setting(CHANNELS_KEY)? {
        None => Ok(Vec::new()),
        Some(text) => serde_json::from_str(&text).map_err(|e| StoreError::failed(format!("{CHANNELS_KEY}: {e}"))),
    }
}

/// Names one warning across looks: the catalog entry and what it is about. Its start and message are left out, so a
/// warning whose message changes is the same warning.
pub fn key(w: &Warning) -> String {
    serde_json::to_string(&(w.condition, w.subject_kind, &w.group, &w.subject_id, &w.site)).unwrap_or_default()
}

/// Whether `channel` takes `w`.
fn takes(channel: &Channel, w: &Warning) -> bool {
    channel.conditions.contains(&w.condition)
        && match (&channel.groups, &w.group) {
            (None, _) => true,
            (Some(groups), Some(group)) => groups.contains(group),
            (Some(_), None) => false,
        }
}

/// The warning as `channel` is told of it: its message only where the channel takes messages.
fn body(channel: &Channel, w: &Warning) -> serde_json::Value {
    let mut value = serde_json::to_value(w).unwrap_or_default();
    if let Some(fields) = value.as_object_mut()
        && !channel.include_message
    {
        fields.remove("message");
    }
    value
}

/// What one look must tell one channel.
#[derive(Debug, Default, PartialEq)]
pub struct Changes {
    /// Warnings it takes and has not been told of, as rows to write once it takes them.
    pub raised: Vec<AlertTold>,
    /// Warnings it was told of that no longer hold.
    pub cleared: Vec<AlertTold>,
    /// Warnings it was told of that cleared while it is muted: forgotten without telling it.
    pub dropped: Vec<AlertTold>,
}

/// What `channel` must be told at `now`, given the warnings that hold and what every channel was told. A muted
/// channel is told nothing: what clears meanwhile is forgotten, and once the mute ends, what then holds is told.
pub fn changes(channel: &Channel, now: i64, warnings: &[Warning], told: &[AlertTold]) -> Changes {
    let muted = channel.muted_until.is_some_and(|until| until > now);
    let told: HashMap<&str, &AlertTold> = told
        .iter()
        .filter(|t| t.channel == channel.id)
        .map(|t| (t.warning.as_str(), t))
        .collect();
    let mut wanted = BTreeMap::new();
    for w in warnings.iter().filter(|w| takes(channel, w)) {
        wanted.entry(key(w)).or_insert(w);
    }
    let raised = wanted
        .iter()
        .filter(|(k, _)| !told.contains_key(k.as_str()))
        .map(|(k, w)| AlertTold {
            channel: channel.id.clone(),
            warning: k.clone(),
            told_at: now,
            body: body(channel, w).to_string(),
        })
        .collect();
    let mut cleared: Vec<AlertTold> = told
        .iter()
        .filter(|(k, _)| !wanted.contains_key(**k))
        .map(|(_, t)| (*t).clone())
        .collect();
    cleared.sort_by(|a, b| a.warning.cmp(&b.warning));
    match muted {
        true => Changes {
            raised: Vec::new(),
            cleared: Vec::new(),
            dropped: cleared,
        },
        false => Changes {
            raised,
            cleared,
            dropped: Vec::new(),
        },
    }
}

/// Whether a send says a warning was raised or has cleared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Raised,
    Cleared,
    /// Sent from the page to check a channel. About no warning.
    Test,
}

/// One line that says what changed, for a reader rather than a program: the issue as the Issues page names it, its
/// subject and group, and the message where the channel takes it.
pub fn summary(status: Status, body: &serde_json::Value) -> String {
    let label = serde_json::from_value::<Condition>(body["condition"].clone()).map_or("Issue", Condition::label);
    let mut line = match status {
        Status::Raised => format!("{label}: {}", body["subjectId"].as_str().unwrap_or_default()),
        Status::Cleared => format!("Cleared: {label}: {}", body["subjectId"].as_str().unwrap_or_default()),
        Status::Test => return "Test from Smeltr.".to_string(),
    };
    if let Some(group) = body["group"].as_str() {
        line.push_str(&format!(" in {group}"));
    }
    if let Some(message) = body["message"].as_str() {
        line.push_str(&format!(". {message}"));
    }
    line
}

/// A webhook's body for one change, in `format`. `routing_key` is a PagerDuty channel's secret.
pub fn webhook_body(format: Format, status: Status, told: &AlertTold, routing_key: Option<&str>) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(&told.body).unwrap_or_default();
    match format {
        Format::GenericJson => {
            if let Some(fields) = value.as_object_mut() {
                fields.insert("status".into(), serde_json::json!(status));
                fields.insert("key".into(), serde_json::json!(told.warning));
            }
            value
        }
        Format::Slack => serde_json::json!({ "text": summary(status, &value) }),
        Format::Teams => serde_json::json!({
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": [{ "type": "TextBlock", "text": summary(status, &value), "wrap": true }],
                },
            }],
        }),
        Format::PagerDuty => {
            let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, told.warning.as_bytes());
            let dedup_key: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
            let severity = match value["severity"].as_str() {
                Some("failed") => "critical",
                _ => "warning",
            };
            let action = match status {
                Status::Raised | Status::Test => "trigger",
                Status::Cleared => "resolve",
            };
            serde_json::json!({
                "routing_key": routing_key.unwrap_or_default(),
                "event_action": action,
                "dedup_key": dedup_key,
                "payload": {
                    "summary": summary(if status == Status::Test { status } else { Status::Raised }, &value),
                    "source": "smeltr",
                    "severity": severity,
                    "custom_details": value,
                },
            })
        }
    }
}

/// Post one change to a webhook.
async fn post(
    url: &str,
    format: Format,
    secret: Option<(&str, &str)>,
    status: Status,
    told: &AlertTold,
) -> Result<u16, String> {
    // The client is judged by the webhook's origin: its query, which Teams and others sign, is the webhook's own.
    let origin = reqwest::Url::parse(url)
        .map_err(|e| format!("{url:?} is not a URL: {e}"))?
        .origin();
    let client = super::outbound::client(
        &origin.ascii_serialization(),
        "the webhook",
        TIMEOUT,
        reqwest::redirect::Policy::none(),
    )?;
    let routing_key = secret.filter(|_| format == Format::PagerDuty).map(|(_, value)| value);
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(webhook_body(format, status, told, routing_key).to_string());
    if let Some((name, value)) = secret.filter(|_| format != Format::PagerDuty) {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("could not reach the webhook: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("the webhook answered {}", response.status()));
    }
    Ok(response.status().as_u16())
}

/// One email that tells of every change in `changes`: its subject and its body.
pub fn email(changes: &Changes) -> (String, String) {
    let lines: Vec<String> = changes
        .raised
        .iter()
        .map(|t| (Status::Raised, t))
        .chain(changes.cleared.iter().map(|t| (Status::Cleared, t)))
        .map(|(status, t)| summary(status, &serde_json::from_str(&t.body).unwrap_or_default()))
        .collect();
    let subject = match lines.as_slice() {
        [one] => format!("Smeltr: {one}"),
        _ => format!(
            "Smeltr: {} raised, {} cleared",
            changes.raised.len(),
            changes.cleared.len()
        ),
    };
    (subject, lines.join("\n") + "\n")
}

/// Send one email through `mail` to `to`.
async fn send_mail(
    mail: &Mail,
    password: Option<String>,
    to: &[String],
    subject: String,
    body: String,
) -> Result<(), String> {
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::transport::smtp::client::{Certificate, CertificateStore, Tls, TlsParameters};
    use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

    let mut message = Message::builder()
        .from(
            mail.from
                .parse()
                .map_err(|e| format!("the From address {}: {e}", mail.from))?,
        )
        .subject(subject);
    for address in to {
        message = message.to(address.parse().map_err(|e| format!("the address {address}: {e}"))?);
    }
    let message = message
        .body(body)
        .map_err(|e| format!("could not write the email: {e}"))?;

    let mut tls = TlsParameters::builder(mail.host.clone()).certificate_store(CertificateStore::None);
    for root in crate::io::trust::roots(None).map_err(|e| e.to_string())? {
        tls = tls.add_root_certificate(Certificate::from_der(root.to_vec()).map_err(|e| e.to_string())?);
    }
    let tls = tls.build_rustls().map_err(|e| format!("the mail server's TLS: {e}"))?;
    let mut transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&mail.host)
        .port(mail.port)
        .timeout(Some(TIMEOUT))
        .tls(match mail.encryption {
            Encryption::StartTls => Tls::Required(tls),
            Encryption::Tls => Tls::Wrapper(tls),
        });
    if let Some(username) = &mail.username {
        transport = transport.credentials(Credentials::new(username.clone(), password.unwrap_or_default()));
    }
    transport
        .build()
        .send(message)
        .await
        .map_err(|e| format!("the mail server {}: {e}", mail.host))?;
    Ok(())
}

/// Tell `channel` of `changes`, one at a time, recording each as it is taken. Stops at the first that fails, so
/// what follows it is tried again on the next look, in order.
async fn tell(state: &AppState, channel: &Channel, mail: Option<&Mail>, changes: Changes) -> Result<(), String> {
    match &channel.kind {
        Kind::Webhook {
            url,
            format,
            secret_header,
        } => {
            let secret = match secret_header {
                Some(header) => {
                    let key = secret_key(&channel.id);
                    let value = run_store(state.store.clone(), move |s| s.opened_setting(&key))
                        .await
                        .map_err(|e| e.message)?
                        .ok_or_else(|| match format {
                            Format::PagerDuty => format!("{}: enter the routing key again", channel.name),
                            _ => format!("{}: enter the secret header value again", channel.name),
                        })?;
                    Some((header.clone(), value))
                }
                None => None,
            };
            let secret = secret.as_ref().map(|(n, v)| (n.as_str(), v.as_str()));
            for told in changes.raised {
                post(url, *format, secret, Status::Raised, &told).await?;
                record(state, vec![told], Vec::new()).await?;
            }
            for told in changes.cleared {
                post(url, *format, secret, Status::Cleared, &told).await?;
                record(state, Vec::new(), vec![(told.channel, told.warning)]).await?;
            }
            Ok(())
        }
        // Every change in one email, so a fleet that falls over at once fills nobody's inbox.
        Kind::Email { to } => {
            let mail = mail.ok_or("Turn the mail server on first")?;
            let password = match mail.username {
                Some(_) => run_store(state.store.clone(), |s| s.opened_setting(MAIL_PASSWORD_KEY))
                    .await
                    .map_err(|e| e.message)?,
                None => None,
            };
            let (subject, body) = email(&changes);
            send_mail(mail, password, to, subject, body).await?;
            let cleared = changes.cleared.into_iter().map(|t| (t.channel, t.warning)).collect();
            record(state, changes.raised, cleared).await
        }
    }
}

async fn record(state: &AppState, told: Vec<AlertTold>, cleared: Vec<(String, String)>) -> Result<(), String> {
    run_store(state.store.clone(), move |s| s.record_alerts_told(&told, &cleared))
        .await
        .map_err(|e| format!("could not record what was sent: {}", e.message))
}

/// One look: tell every channel what has changed since it was last told.
pub async fn look(state: &AppState) -> Result<(), StoreError> {
    let now = super::warnings::now_millis();
    let (channels, mail, told) =
        run_store(state.store.clone(), |s| Ok((channels(s)?, mail(s)?, s.alerts_told()?))).await?;
    let ids: Vec<String> = channels.iter().map(|c| c.id.clone()).collect();
    state.alerts.keep(&ids);
    if told.iter().any(|t| !ids.contains(&t.channel)) {
        run_store(state.store.clone(), move |s| s.forget_alert_channels(&ids)).await?;
    }
    if channels.is_empty() {
        return Ok(());
    }
    let warnings = super::warnings_snapshot(state).await?;
    for channel in &channels {
        // What an email channel would be told waits for the mail server, which the page says is off.
        if matches!(channel.kind, Kind::Email { .. }) && mail.is_none() {
            continue;
        }
        let mut changes = changes(channel, now, &warnings, &told);
        if !changes.dropped.is_empty() {
            let dropped: Vec<(String, String)> = std::mem::take(&mut changes.dropped)
                .into_iter()
                .map(|t| (t.channel, t.warning))
                .collect();
            run_store(state.store.clone(), move |s| s.record_alerts_told(&[], &dropped)).await?;
        }
        if changes.raised.is_empty() && changes.cleared.is_empty() {
            continue;
        }
        match tell(state, channel, mail.as_ref(), changes).await {
            Ok(()) => state.alerts.took(&channel.id),
            Err(why) => {
                tracing::warn!(channel = %channel.name, "could not send to the alert channel: {why}");
                state.alerts.failed(channel, why, now);
            }
        }
    }
    Ok(())
}

/// Look on the timer for as long as the leader runs, and at once when the channels change.
pub async fn run(state: AppState) {
    loop {
        if let Err(e) = look(&state).await {
            tracing::warn!("could not look for alerts to send: {}", e.message);
        }
        tokio::select! {
            _ = tokio::time::sleep(EVERY) => {}
            _ = state.alerts.settings_changed.notified() => {}
        }
    }
}

// -- settings ----------------------------------------------------------------

/// The mail server as the page reads it. The password itself is never sent back.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MailDto {
    #[serde(flatten)]
    mail: Mail,
    password_set: bool,
}

/// A channel as the page reads it: its secret only as whether one is stored, and its last failure while it fails.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDto {
    #[serde(flatten)]
    channel: Channel,
    secret_set: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    failing: Option<Failing>,
}

#[derive(Debug, Default, Serialize)]
pub struct AlertSettings {
    settings: Option<AlertSettingsDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertSettingsDto {
    /// `None` until a mail server is saved.
    mail: Option<MailDto>,
    channels: Vec<ChannelDto>,
    /// What a channel can take, in the catalog's order: every entry but those only ever raised as info, which no
    /// channel is sent.
    conditions: Vec<Condition>,
}

/// The catalog entries raised only as info.
const ONLY_INFO: &[Condition] = &[Condition::WorkerOnAnotherRelease, Condition::DestinationMemoryBound];

/// What `GET /api/settings/alerts` answers, read from the store and the failures held.
fn read_settings(store: &dyn Store, alerts: &Alerts) -> Result<AlertSettingsDto, StoreError> {
    let mail = match store.setting(MAIL_KEY)? {
        None => None,
        Some(text) => Some(MailDto {
            mail: serde_json::from_str(&text).map_err(|e| StoreError::failed(format!("{MAIL_KEY}: {e}")))?,
            password_set: store.setting(MAIL_PASSWORD_KEY)?.is_some_and(|p| !p.is_empty()),
        }),
    };
    let failing = alerts.failing();
    let channels = channels(store)?
        .into_iter()
        .map(|channel| {
            Ok(ChannelDto {
                secret_set: store.setting(&secret_key(&channel.id))?.is_some_and(|s| !s.is_empty()),
                failing: failing.get(&channel.id).cloned(),
                channel,
            })
        })
        .collect::<Result<_, StoreError>>()?;
    let conditions = Condition::ALL
        .iter()
        .copied()
        .filter(|c| !ONLY_INFO.contains(c))
        .collect();
    Ok(AlertSettingsDto {
        mail,
        channels,
        conditions,
    })
}

fn answer(read: Result<AlertSettingsDto, Refusal>) -> Envelope<AlertSettings> {
    match read {
        Ok(settings) => Envelope::ok(AlertSettings {
            settings: Some(settings),
        }),
        Err(refusal) => Envelope::refused(refusal),
    }
}

/// `GET /api/settings/alerts`.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<AlertSettings> {
    let alerts = state.alerts.clone();
    answer(
        run_store(state.store.clone(), move |s| read_settings(s, &alerts))
            .await
            .map_err(Refusal::from),
    )
}

/// `PUT /api/settings/alerts/mail`'s body: the mail server, and its password when it changes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutMail {
    #[serde(flatten)]
    mail: Mail,
    /// Absent keeps the stored one, empty forgets it.
    #[serde(default)]
    password: Option<String>,
}

/// Why `mail` cannot be saved, if it cannot. Off, it is kept as it is, so turning it on again finds it.
fn check_mail(mail: &Mail) -> Result<(), String> {
    if !mail.enabled {
        return Ok(());
    }
    if mail.host.trim().is_empty() {
        return Err("give the mail server's host".into());
    }
    if mail.port == 0 {
        return Err("give the mail server's port".into());
    }
    mail.from
        .parse::<lettre::Address>()
        .map_err(|e| format!("the From address {:?}: {e}", mail.from))?;
    Ok(())
}

/// `PUT /api/settings/alerts/mail`. Recorded with who set it, in the transaction that sets it. The password is
/// sealed, and is for the server it was saved with: another host forgets it unless a new one is given.
pub async fn put_mail(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<PutMail>,
) -> Envelope<AlertSettings> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let mut mail = request.mail;
    mail.host = mail.host.trim().to_string();
    mail.from = mail.from.trim().to_string();
    mail.username = mail.username.map(|u| u.trim().to_string()).filter(|u| !u.is_empty());
    if let Err(e) = check_mail(&mail) {
        return answer(Err(Refusal::invalid(e)));
    }
    let event = super::accounts::event_for(&actor, "alerts-mail-change", &mail.host, "ok");
    let alerts = state.alerts.clone();
    let written = run_store(state.store.clone(), move |s| {
        let was = s.setting(MAIL_KEY)?.and_then(|t| serde_json::from_str::<Mail>(&t).ok());
        let text = serde_json::to_string(&mail).map_err(|e| StoreError::failed(e.to_string()))?;
        let mut entries = vec![(MAIL_KEY, text)];
        let password = match request.password {
            None if was.is_none_or(|w| w.host != mail.host) => Some(String::new()),
            password => password,
        };
        if let Some(password) = password {
            let stored = match password.is_empty() {
                true => password,
                false => s.sealed_setting(MAIL_PASSWORD_KEY, &password)?,
            };
            entries.push((MAIL_PASSWORD_KEY, stored));
        }
        s.put_settings(&entries, Some(&event))?;
        read_settings(s, &alerts)
    })
    .await
    .map_err(Refusal::from);
    if written.is_ok() {
        state.alerts.settings_changed.notify_one();
    }
    answer(written)
}

/// One channel as the page saves it: the channel, and its secret when that changes.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutChannel {
    #[serde(flatten)]
    channel: Channel,
    /// A webhook's secret header value, or a PagerDuty channel's routing key. Absent keeps the stored one, empty
    /// forgets it.
    #[serde(default)]
    secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PutChannels {
    channels: Vec<PutChannel>,
}

/// Why `channel` cannot be saved, if it cannot.
fn check_channel(channel: &Channel) -> Result<(), String> {
    let name = &channel.name;
    if name.is_empty() {
        return Err("give each channel a name".into());
    }
    if channel.groups.as_ref().is_some_and(Vec::is_empty) {
        return Err(format!("{name}: choose at least one group, or all groups"));
    }
    match &channel.kind {
        Kind::Webhook { url, secret_header, .. } => {
            crate::io::discovery::check_url(url, true).map_err(|e| format!("{name}: {e}"))?;
            if let Some(header) = secret_header {
                reqwest::header::HeaderName::from_bytes(header.as_bytes())
                    .map_err(|_| format!("{name}: {header:?} is not a header name"))?;
            }
        }
        Kind::Email { to } => {
            if to.is_empty() {
                return Err(format!("{name}: give at least one address"));
            }
            for address in to {
                address
                    .parse::<lettre::Address>()
                    .map_err(|e| format!("{name}: the address {address:?}: {e}"))?;
            }
        }
    }
    Ok(())
}

/// The channel as the page sent it, made tidy: names and addresses trimmed, a new channel given its id.
fn tidy(mut channel: Channel) -> Channel {
    channel.name = channel.name.trim().to_string();
    if channel.id.is_empty() {
        channel.id = super::auth::generate_secret().split_at(16).0.to_string();
    }
    match &mut channel.kind {
        Kind::Webhook { url, secret_header, .. } => {
            *url = url.trim().to_string();
            *secret_header = secret_header
                .take()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty());
        }
        Kind::Email { to } => {
            *to = to
                .iter()
                .map(|a| a.trim().to_string())
                .filter(|a| !a.is_empty())
                .collect();
        }
    }
    channel
}

/// `PUT /api/settings/alerts/channels`: every channel, as one save, since the table that says what each takes spans
/// them all. Recorded with who saved it, in the transaction that saves it. A webhook's secret is sealed, and is for
/// the URL it was saved with: another URL forgets it unless a new one is given. A channel left out is removed, with
/// its secret and what it was told.
pub async fn put_channels(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<PutChannels>,
) -> Envelope<AlertSettings> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let mut sent: Vec<PutChannel> = request.channels;
    for put in &mut sent {
        put.channel = tidy(put.channel.clone());
        if let Err(e) = check_channel(&put.channel) {
            return answer(Err(Refusal::invalid(e)));
        }
    }
    let mut names: Vec<&str> = sent.iter().map(|p| p.channel.name.as_str()).collect();
    names.sort_unstable();
    if let Some(twice) = names.windows(2).find(|w| w[0] == w[1]) {
        return answer(Err(Refusal::invalid(format!(
            "Rename one of the two channels called {}",
            twice[0]
        ))));
    }
    let event = super::accounts::event_for(&actor, "alerts-channels-change", &names.join(", "), "ok");
    let alerts = state.alerts.clone();
    let written = run_store(state.store.clone(), move |s| {
        let was: HashMap<String, Channel> = channels(s)?.into_iter().map(|c| (c.id.clone(), c)).collect();
        let url = |c: &Channel| match &c.kind {
            Kind::Webhook { url, .. } => Some(url.clone()),
            Kind::Email { .. } => None,
        };
        let mut secrets: Vec<(String, String)> = Vec::new();
        for put in &sent {
            let key = secret_key(&put.channel.id);
            let moved = was.get(&put.channel.id).is_none_or(|w| url(w) != url(&put.channel));
            match &put.secret {
                Some(secret) if !secret.is_empty() => secrets.push((key.clone(), s.sealed_setting(&key, secret)?)),
                Some(_) => secrets.push((key, String::new())),
                None if moved => secrets.push((key, String::new())),
                None => {}
            }
        }
        for id in was.keys().filter(|id| !sent.iter().any(|p| &p.channel.id == *id)) {
            secrets.push((secret_key(id), String::new()));
        }
        let kept: Vec<Channel> = sent.iter().map(|p| p.channel.clone()).collect();
        let text = serde_json::to_string(&kept).map_err(|e| StoreError::failed(e.to_string()))?;
        let mut entries: Vec<(&str, String)> = vec![(CHANNELS_KEY, text)];
        entries.extend(secrets.iter().map(|(k, v)| (k.as_str(), v.clone())));
        s.put_settings(&entries, Some(&event))?;
        s.forget_alert_channels(&kept.iter().map(|c| c.id.clone()).collect::<Vec<_>>())?;
        read_settings(s, &alerts)
    })
    .await
    .map_err(Refusal::from);
    if written.is_ok() {
        state.alerts.settings_changed.notify_one();
    }
    answer(written)
}

/// What a test send found.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tested {
    /// The host that took it.
    host: String,
    /// What a webhook answered. Absent for email, whose server answers in its own terms.
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
}

/// `POST /api/settings/alerts/test`: send one test to the channel as the page holds it, saved or not. A secret not
/// sent is the one stored for the channel. PagerDuty is sent a test incident and its resolution at once.
pub async fn post_test(State(state): State<AppState>, Json(request): Json<PutChannel>) -> Envelope<Tested> {
    let channel = tidy(request.channel);
    if let Err(e) = check_channel(&channel) {
        return Envelope::refused(Refusal::invalid(e));
    }
    match test(&state, &channel, request.secret).await {
        Ok(tested) => Envelope::ok(tested),
        Err(why) => {
            tracing::warn!(channel = %channel.name, "the test send to the alert channel failed: {why}");
            Envelope::refused(Refusal::invalid(why))
        }
    }
}

async fn test(state: &AppState, channel: &Channel, secret: Option<String>) -> Result<Tested, String> {
    let told = AlertTold {
        channel: channel.id.clone(),
        warning: format!("smeltr-test-{}", channel.id),
        told_at: super::warnings::now_millis(),
        body: "{}".into(),
    };
    match &channel.kind {
        Kind::Webhook {
            url,
            format,
            secret_header,
        } => {
            let secret = match secret {
                Some(secret) => Some(secret).filter(|s| !s.is_empty()),
                None => {
                    let key = secret_key(&channel.id);
                    run_store(state.store.clone(), move |s| s.opened_setting(&key))
                        .await
                        .map_err(|e| e.message)?
                }
            };
            let name = match format {
                Format::PagerDuty => "",
                _ => secret_header.as_deref().unwrap_or_default(),
            };
            let secret = secret
                .as_deref()
                .filter(|_| *format == Format::PagerDuty || !name.is_empty());
            let secret = secret.map(|value| (name, value));
            let status = post(url, *format, secret, Status::Test, &told).await?;
            if *format == Format::PagerDuty {
                post(url, *format, secret, Status::Cleared, &told).await?;
            }
            let host = reqwest::Url::parse(url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .unwrap_or_default();
            Ok(Tested {
                host,
                status: Some(status),
            })
        }
        Kind::Email { to } => {
            let (mail, password) = run_store(state.store.clone(), |s| {
                Ok((mail(s)?, s.opened_setting(MAIL_PASSWORD_KEY)?))
            })
            .await
            .map_err(|e| e.message)?;
            let mail = mail.ok_or("Turn the mail server on first")?;
            let line = summary(Status::Test, &serde_json::Value::Null);
            send_mail(&mail, password, to, format!("Smeltr: {line}"), format!("{line}\n")).await?;
            Ok(Tested {
                host: mail.host,
                status: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader::warnings::{Severity, SubjectKind};

    fn warning(condition: Condition, subject: &str, group: Option<&str>) -> Warning {
        Warning {
            condition,
            subject_kind: SubjectKind::Source,
            subject_id: subject.into(),
            group: group.map(str::to_owned),
            severity: Severity::Failed,
            since_millis: Some(1_000),
            message: "orders: connection refused for row 42".into(),
            site: None,
            area: None,
        }
    }

    fn channel(groups: Option<&[&str]>) -> Channel {
        Channel {
            id: "c1".into(),
            name: "Paging".into(),
            kind: Kind::Webhook {
                url: "https://hooks.example.com/x".into(),
                format: Format::GenericJson,
                secret_header: None,
            },
            groups: groups.map(|g| g.iter().map(|s| s.to_string()).collect()),
            include_message: false,
            conditions: vec![Condition::SourceUnreachable],
            muted_until: None,
        }
    }

    #[test]
    fn a_channel_is_told_once_of_what_it_takes_and_again_when_it_clears() {
        let c = channel(None);
        let up = [warning(Condition::SourceUnreachable, "orders", Some("payments"))];
        let first = changes(&c, 5, &up, &[]);
        assert_eq!(first.raised.len(), 1);
        assert!(first.cleared.is_empty());

        let told = first.raised;
        assert_eq!(
            changes(&c, 6, &up, &told),
            Changes::default(),
            "a warning already told is not told again"
        );

        let gone = changes(&c, 7, &[], &told);
        assert!(gone.raised.is_empty());
        assert_eq!(gone.cleared, told);
    }

    #[test]
    fn a_channel_takes_only_its_conditions_and_groups() {
        let c = channel(Some(&["payments"]));
        let ws = [
            warning(Condition::SourceUnreachable, "orders", Some("payments")),
            warning(Condition::SourceUnreachable, "clicks", Some("search")),
            warning(Condition::SourceBlocked, "refunds", Some("payments")),
            warning(Condition::SourceUnreachable, "leader", None),
        ];
        let raised = changes(&c, 5, &ws, &[]).raised;
        assert_eq!(raised.len(), 1, "{raised:?}");
        assert_eq!(raised[0].warning, key(&ws[0]));

        let all = changes(&channel(None), 5, &ws, &[]).raised;
        assert_eq!(
            all.len(),
            3,
            "a channel for every group also takes the warnings that name none"
        );
    }

    #[test]
    fn a_muted_channel_is_told_nothing_until_its_mute_ends() {
        let mut c = channel(None);
        c.muted_until = Some(10);
        let up = [warning(Condition::SourceUnreachable, "orders", None)];
        let told = changes(&channel(None), 1, &up, &[]).raised;
        assert_eq!(changes(&c, 5, &[up[0].clone()], &[]), Changes::default());
        let meanwhile = changes(&c, 5, &[], &told);
        assert!(
            meanwhile.raised.is_empty() && meanwhile.cleared.is_empty(),
            "nor of what cleared meanwhile"
        );
        assert_eq!(meanwhile.dropped, told, "which is forgotten");
        let after = changes(&c, 10, &up, &[]);
        assert_eq!(after.raised.len(), 1, "once it ends, it is told of what still holds");
    }

    #[test]
    fn a_channel_is_told_the_message_only_when_it_takes_messages() {
        let mut c = channel(None);
        let up = [warning(Condition::SourceUnreachable, "orders", None)];
        let left_out = changes(&c, 5, &up, &[]).raised.remove(0);
        assert!(!left_out.body.contains("row 42"), "{}", left_out.body);
        c.include_message = true;
        let included = changes(&c, 5, &up, &[]).raised.remove(0);
        assert!(included.body.contains("row 42"), "{}", included.body);
    }

    #[test]
    fn a_generic_json_body_says_whether_the_warning_was_raised_or_cleared() {
        let c = channel(None);
        let told = changes(
            &c,
            5,
            &[warning(Condition::SourceUnreachable, "orders", Some("payments"))],
            &[],
        )
        .raised;
        let body = webhook_body(Format::GenericJson, Status::Cleared, &told[0], None);
        assert_eq!(body["status"], "cleared");
        assert_eq!(body["condition"], "sourceUnreachable");
        assert_eq!(body["subjectId"], "orders");
        assert_eq!(body["group"], "payments");
        assert_eq!(body["key"], told[0].warning);
        assert!(body.get("message").is_none());
    }

    #[test]
    fn slack_and_teams_are_sent_a_line_a_person_reads() {
        let mut c = channel(None);
        c.include_message = true;
        let told = changes(
            &c,
            5,
            &[warning(Condition::SourceUnreachable, "orders", Some("payments"))],
            &[],
        )
        .raised;
        let line = "Source unreachable: orders in payments. orders: connection refused for row 42";
        assert_eq!(
            webhook_body(Format::Slack, Status::Raised, &told[0], None),
            serde_json::json!({ "text": line })
        );
        let teams = webhook_body(Format::Teams, Status::Cleared, &told[0], None);
        assert_eq!(
            teams["attachments"][0]["content"]["body"][0]["text"],
            format!("Cleared: {line}")
        );
    }

    #[test]
    fn pagerduty_opens_an_incident_and_resolves_the_same_one() {
        let told = changes(
            &channel(None),
            5,
            &[warning(Condition::SourceUnreachable, "orders", None)],
            &[],
        )
        .raised;
        let open = webhook_body(Format::PagerDuty, Status::Raised, &told[0], Some("R0UT1NG"));
        let close = webhook_body(Format::PagerDuty, Status::Cleared, &told[0], Some("R0UT1NG"));
        assert_eq!(open["routing_key"], "R0UT1NG");
        assert_eq!(
            (open["event_action"].as_str(), close["event_action"].as_str()),
            (Some("trigger"), Some("resolve"))
        );
        assert_eq!(open["dedup_key"], close["dedup_key"]);
        assert_eq!(open["dedup_key"].as_str().unwrap().len(), 64);
        assert_eq!(open["payload"]["severity"], "critical");
        assert_eq!(open["payload"]["summary"], "Source unreachable: orders");
    }

    #[test]
    fn one_email_tells_of_every_change_and_one_change_is_its_subject() {
        let c = channel(None);
        let ws = [
            warning(Condition::SourceUnreachable, "orders", Some("payments")),
            warning(Condition::SourceUnreachable, "clicks", None),
        ];
        let both = changes(&c, 5, &ws, &[]);
        let (subject, body) = email(&both);
        assert_eq!(subject, "Smeltr: 2 raised, 0 cleared");
        assert_eq!(
            body,
            "Source unreachable: orders in payments\nSource unreachable: clicks\n"
        );
        let one = changes(&c, 6, &ws[..1], &both.raised);
        assert_eq!(email(&one).0, "Smeltr: Cleared: Source unreachable: clicks");
    }

    #[test]
    fn a_channel_fails_from_its_first_failed_send_until_one_is_taken() {
        let alerts = Alerts::default();
        let c = channel(None);
        alerts.failed(&c, "the webhook answered 500".into(), 5);
        alerts.failed(&c, "the webhook answered 503".into(), 9);
        let failing = alerts.failing().remove("c1").unwrap();
        assert_eq!(
            (failing.since, failing.last, failing.message.as_str()),
            (5, 9, "the webhook answered 503")
        );
        let w = crate::leader::warnings::alert_channel_failing(&failing.channel, &failing);
        assert_eq!(
            (w.condition, w.group, w.since_millis),
            (Condition::AlertChannelFailing, None, Some(5))
        );
        assert!(
            w.message.contains("Paging") && w.message.contains("503"),
            "{}",
            w.message
        );
        alerts.took("c1");
        assert!(alerts.failing().is_empty());
    }

    #[test]
    fn stored_channels_read_back_as_written() {
        let c = Channel {
            kind: Kind::Email {
                to: vec!["oncall@example.com".into()],
            },
            groups: Some(vec!["payments".into()]),
            include_message: true,
            muted_until: Some(99),
            ..channel(None)
        };
        let text = serde_json::to_string(&vec![c.clone()]).unwrap();
        assert_eq!(serde_json::from_str::<Vec<Channel>>(&text).unwrap(), vec![c]);
    }
}
