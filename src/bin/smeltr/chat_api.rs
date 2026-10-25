//! A person's conversations with their own AI agent (`leader::agent`), and the turns that run in them.
//!
//! A message starts a turn in the background and answers `202`; the GUI reads the conversation until the turn
//! ends. Each call the agent makes is a request into the leader's own router, carrying the person's session and
//! [`ByAgent`], so the gate judges it as the person's, narrowed to what they let their agent use. The session's
//! token is held in memory for the turn and never stored.
//!
//! A conversation is its person's alone: anyone else's reads as not there. No agent reaches these routes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use smeltr::leader::access::ByAgent;
use smeltr::leader::agent::{self, Answer, ApiCall, Ended, Host};
use smeltr::leader::blocking;
use smeltr::leader::envelope::Envelope;
use smeltr::leader::model::{self, Effort, Fresh, Said, Settings, Turn};
use smeltr::leader::store::{Actor, ConversationRecord, StoreError};
use smeltr::leader::warnings::now_millis;

use crate::agent_catalog::CATALOG;
use crate::agents_api::refused_while_off;
use crate::gate::{Auth, CSRF_HEADER, presented_token, refuse};
use crate::users_api::{no_store, store_call};

/// The most of a request's answer read back from the router. Far beyond any JSON answer the API gives; the model
/// reads only [`agent::MAX_RESULT`] of it.
const MAX_ANSWER: usize = 4 * 1024 * 1024;

/// The most of the first message a conversation's title takes.
const TITLE_CHARS: usize = 80;

/// How often conversations nobody has added to are looked for.
const SWEEP_EVERY: std::time::Duration = std::time::Duration::from_secs(60 * 60);

const NO_CONVERSATION: &str = "there is no such conversation";

/// The turns running now, and the router their calls go through.
#[derive(Default)]
pub struct Chats {
    router: OnceLock<Router>,
    /// Held around every reach into a conversation's turns as well as its run, so the two are only ever seen and
    /// changed together. The store's own lock is taken inside it, never around it.
    runs: Mutex<HashMap<String, Run>>,
}

impl Chats {
    /// The composed router, gate and all, that a turn's calls go through. Set once it is built: the chat routes are
    /// part of it.
    pub fn serve(&self, router: Router) {
        let _ = self.router.set(router);
    }

    fn runs(&self) -> std::sync::MutexGuard<'_, HashMap<String, Run>> {
        self.runs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A conversation's turn: the one running, or how the last one this leader ran ended.
#[derive(Clone)]
struct Run {
    stop: Arc<AtomicBool>,
    running: bool,
    ended: Option<Ended>,
    error: Option<String>,
    /// The answer the model is writing, so far; empty between answers.
    writing: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    pub message: String,
    /// The model and effort to answer with; without them, what made the conversation's last answer, or the
    /// administrator's model and the provider's effort.
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<Effort>,
}

/// `POST /api/agent/conversations/:id/context`: whether the model's reading begins again from a summary of the
/// conversation, or from nothing.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Restart {
    pub summarize: bool,
}

/// What a run does.
enum Job {
    /// Answer the person's message.
    Message(String),
    /// Begin the model's reading of the conversation again, from a summary of it or from nothing.
    Fresh { summarize: bool },
}

/// What a turn is answered with, as the person picked it.
#[derive(Clone, Default)]
struct Picked {
    model: Option<String>,
    effort: Option<Effort>,
}

impl Message {
    fn picked(&self) -> Picked {
        Picked {
            model: self
                .model
                .as_deref()
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .map(str::to_string),
            effort: self.effort,
        }
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationSummary {
    id: String,
    title: String,
    created_at: i64,
    updated_at: i64,
    running: bool,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationDto {
    #[serde(flatten)]
    summary: ConversationSummary,
    /// How the last turn this leader ran ended: `answered`, `stopped` or `rounds`.
    ended: Option<&'static str>,
    /// Why the last turn failed, when it did.
    error: Option<String>,
    items: Vec<Value>,
    /// What made the last answer, which the next message starts from.
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    effort: Option<Effort>,
    /// How much of its model's context the last request filled.
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<Value>,
    /// While a turn runs, the answer its model is writing, so far.
    #[serde(skip_serializing_if = "Option::is_none")]
    writing: Option<String>,
}

/// What every chat route refuses first: an agent, since a conversation is its person's, and anyone while AI agents
/// are off.
async fn refused(auth: &Auth, by_agent: &Option<Extension<ByAgent>>) -> Option<Response> {
    if by_agent.is_some() {
        return Some(refuse(
            StatusCode::FORBIDDEN,
            "an agent cannot use its person's conversations",
        ));
    }
    refused_while_off(auth).await
}

/// What `request` says, or the refusal of a message that says nothing.
fn said(request: &Message) -> Result<String, Box<Response>> {
    let message = request.message.trim().to_string();
    match message.is_empty() {
        true => Err(Box::new(refuse(StatusCode::BAD_REQUEST, "a message says something"))),
        false => Ok(message),
    }
}

/// The caller's conversation `id`, or the refusal to answer with.
async fn owned(auth: &Auth, actor: &Actor, id: &str) -> Result<ConversationRecord, Box<Response>> {
    let store = auth.store.clone();
    let id = id.to_string();
    match store_call(move || store.conversation(&id)).await {
        Ok(Some(c)) if c.user_id == actor.id => Ok(c),
        Ok(_) => Err(Box::new(refuse(StatusCode::NOT_FOUND, NO_CONVERSATION))),
        Err(response) => Err(response),
    }
}

fn summary(c: ConversationRecord, running: bool) -> ConversationSummary {
    ConversationSummary {
        id: c.id,
        title: c.title,
        created_at: c.created_at,
        updated_at: c.updated_at,
        running,
    }
}

/// `POST /api/agent/conversations`: start a conversation with its first message.
pub async fn start(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    headers: HeaderMap,
    Json(request): Json<Message>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    let message = match said(&request) {
        Ok(message) => message,
        Err(refused) => return *refused,
    };
    let at = now_millis();
    let conversation = ConversationRecord {
        id: format!("c_{}", smeltr::leader::auth::generate_secret().split_at(24).0),
        user_id: actor.id.clone(),
        title: message.chars().take(TITLE_CHARS).collect(),
        created_at: at,
        updated_at: at,
    };
    let store = auth.store.clone();
    let kept = conversation.clone();
    if let Err(response) = store_call(move || store.create_conversation(&kept)).await {
        return *response;
    }
    let job = Job::Message(message);
    if let Err(refused) = begin(&auth, &actor, &headers, &conversation.id, job, request.picked(), any).await {
        return refused;
    }
    (
        StatusCode::ACCEPTED,
        no_store(Envelope::ok(json!({"id": conversation.id}))),
    )
        .into_response()
}

/// `POST /api/agent/conversations/:id/messages`: say something more, once the turn before has ended.
pub async fn add_message(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Message>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    let message = match said(&request) {
        Ok(message) => message,
        Err(refused) => return *refused,
    };
    if let Err(refused) = owned(&auth, &actor, &id).await {
        return *refused;
    }
    let job = Job::Message(message);
    if let Err(refused) = begin(&auth, &actor, &headers, &id, job, request.picked(), any).await {
        return refused;
    }
    (StatusCode::ACCEPTED, no_store(Envelope::done())).into_response()
}

/// `POST /api/agent/conversations/:id/context`: begin the model's reading of the conversation again, from a summary
/// of it or from nothing. The conversation is still shown whole; a summary is written in the background, like a turn.
pub async fn context(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<Restart>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    if let Err(refused) = owned(&auth, &actor, &id).await {
        return *refused;
    }
    let job = Job::Fresh {
        summarize: request.summarize,
    };
    let said_nothing = |turns: &[Turn]| {
        matches!(turns.last(), None | Some(Turn::Fresh(_)))
            .then_some("nothing has been said since the conversation began again")
    };
    if let Err(refused) = begin(&auth, &actor, &headers, &id, job, Picked::default(), said_nothing).await {
        return refused;
    }
    (StatusCode::ACCEPTED, no_store(Envelope::done())).into_response()
}

/// No refusal: any conversation can take a message.
fn any(_: &[Turn]) -> Option<&'static str> {
    None
}

/// Start a turn of `id` in the background from its turns as they stand, unless one runs or `refusal` says why
/// those turns cannot start one.
async fn begin(
    auth: &Auth,
    actor: &Actor,
    headers: &HeaderMap,
    id: &str,
    job: Job,
    picked: Picked,
    refusal: fn(&[Turn]) -> Option<&'static str>,
) -> Result<(), Response> {
    let stop = Arc::new(AtomicBool::new(false));
    let (chats, store, read, flag) = (auth.chats.clone(), auth.store.clone(), id.to_string(), stop.clone());
    let begun = store_call(move || {
        let mut runs = chats.runs();
        if runs.get(&read).is_some_and(|r| r.running) {
            return Ok(Err(refuse(StatusCode::CONFLICT, "the agent is still answering")));
        }
        let turns = store.turns_of(&read)?;
        let turns: Result<Vec<Turn>, _> = turns.iter().map(|t| serde_json::from_str(t)).collect();
        let turns = match turns {
            Ok(turns) => turns,
            Err(e) => {
                let why = format!("a turn of this conversation cannot be read: {e}");
                return Ok(Err(refuse(StatusCode::INTERNAL_SERVER_ERROR, &why)));
            }
        };
        if let Some(why) = refusal(&turns) {
            return Ok(Err(refuse(StatusCode::CONFLICT, why)));
        }
        let run = Run {
            stop: flag,
            running: true,
            ended: None,
            error: None,
            writing: String::new(),
        };
        runs.insert(read, run);
        Ok(Ok(turns))
    })
    .await;
    let turns = match begun {
        Ok(Ok(turns)) => turns,
        Ok(Err(refused)) => return Err(refused),
        Err(response) => return Err(*response),
    };
    let host = Turned {
        auth: auth.clone(),
        conversation: id.to_string(),
        // The gate let this request through on it, so it is there.
        token: presented_token(headers, auth.origin.cookie_name()).unwrap_or_default(),
        stop,
        person: smeltr::leader::auth::hash_secret(&actor.id),
        picked,
    };
    let auth = auth.clone();
    let id = id.to_string();
    tokio::spawn(async move {
        let outcome = match job {
            Job::Message(message) => turn(&host, turns, message).await,
            Job::Fresh { summarize } => fresh(&host, turns, summarize).await,
        };
        let mut runs = auth.chats.runs();
        if let Some(run) = runs.get_mut(&id) {
            run.running = false;
            run.writing.clear();
            match outcome {
                Ok(ended) => run.ended = Some(ended),
                Err(e) => run.error = Some(e),
            }
        }
    });
    Ok(())
}

async fn turn(host: &Turned, turns: Vec<Turn>, message: String) -> Result<Ended, String> {
    let (model, asking) = client(host, &turns).await?;
    let system = agent::system_prompt(CATALOG);
    agent::run(
        model.as_ref(),
        host,
        CATALOG,
        &system,
        turns,
        vec![Said::Text(message)],
        &asking,
    )
    .await
}

/// A fresh start kept after `turns`: a summary of them the model writes, or nothing.
async fn fresh(host: &Turned, turns: Vec<Turn>, summarize: bool) -> Result<Ended, String> {
    let fresh = match summarize {
        true => {
            let (model, asking) = client(host, &turns).await?;
            agent::summarize(model.as_ref(), &agent::system_prompt(CATALOG), turns, &asking).await?
        }
        false => Fresh {
            summary: None,
            tokens: 0,
        },
    };
    host.keep(&Turn::Fresh(fresh)).await?;
    Ok(Ended::Answered)
}

/// The model a run of the conversation `turns` asks, and how.
async fn client(host: &Turned, turns: &[Turn]) -> Result<(Box<dyn model::Model>, agent::Asking), String> {
    let store = host.auth.store.clone();
    let mut endpoint = blocking(move || Settings::read(store.as_ref()))
        .await
        .map_err(|e| e.to_string())?
        .endpoint()
        .ok_or("AI agents are not turned on")?;
    // What was picked, else what made the last answer, else the administrator's model and the provider's effort.
    let last = last_answer(turns);
    let effort = host.picked.effort.or(last.and_then(|r| r.effort));
    if let Some(picked) = host.picked.model.clone().or(last.and_then(|r| r.model.clone()))
        && picked != endpoint.model
    {
        endpoint.model = picked;
        // Anthropic says how much each model holds; for another provider the setting stands for them all.
        if let Some(context) = model::context_of(&endpoint).await {
            endpoint.context = context;
        }
    }
    let asking = agent::Asking {
        effort,
        cache_key: Some(host.conversation.clone()),
        person: Some(host.person.clone()),
    };
    Ok((model::model(endpoint)?, asking))
}

/// How much of its model's context the conversation fills: what the last request read and wrote, or since a fresh
/// start, what it holds of the conversation before.
fn filled(turns: &[Turn]) -> Option<Value> {
    let window = last_answer(turns)?.context?;
    let from = turns.iter().rposition(|t| matches!(t, Turn::Fresh(_))).unwrap_or(0);
    let used = match (last_answer(&turns[from..]), &turns[from]) {
        (Some(r), _) => r.usage.input + r.usage.output,
        (None, Turn::Fresh(fresh)) => fresh.tokens,
        (None, _) => 0,
    };
    Some(json!({"used": used, "window": window}))
}

/// The conversation's last answer.
fn last_answer(turns: &[Turn]) -> Option<&model::Reply> {
    turns.iter().rev().find_map(|t| match t {
        Turn::Answered(reply) => Some(reply),
        Turn::Asked(_) | Turn::Fresh(_) => None,
    })
}

/// A running turn's way into the leader: its conversation, and the person's session its calls carry.
struct Turned {
    auth: Auth,
    conversation: String,
    token: String,
    stop: Arc<AtomicBool>,
    /// The person's account id, hashed.
    person: String,
    picked: Picked,
}

#[async_trait::async_trait]
impl Host for Turned {
    async fn live(&self) -> bool {
        if self.stop.load(Ordering::SeqCst) {
            return false;
        }
        let store = self.auth.store.clone();
        blocking(move || model::on(store.as_ref())).await.unwrap_or(false)
    }

    async fn keep(&self, turn: &Turn) -> Result<(), String> {
        let text = serde_json::to_string(turn).map_err(|e| e.to_string())?;
        let (chats, store, id) = (
            self.auth.chats.clone(),
            self.auth.store.clone(),
            self.conversation.clone(),
        );
        let kept = blocking(move || {
            let mut runs = chats.runs();
            let kept = store.append_turn(&id, &text, now_millis())?;
            // What was being written is now among the turns.
            if let Some(run) = runs.get_mut(&id) {
                run.writing.clear();
            }
            Ok::<_, StoreError>(kept)
        })
        .await
        .map_err(|e| e.to_string())?;
        if !kept {
            self.stop.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn heard(&self, text: &str) {
        if let Some(run) = self.auth.chats.runs().get_mut(&self.conversation) {
            run.writing.push_str(text);
        }
    }

    async fn request(&self, call: &ApiCall) -> Answer {
        let failed = |why: String| Answer { status: 500, body: why };
        let Some(router) = self.auth.chats.router.get() else {
            return failed("the leader is not serving yet".to_string());
        };
        let mut uri = call.path.clone();
        if !call.query.is_empty() {
            let query: Vec<String> = call
                .query
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        smeltr::io::http::percent_encode(k),
                        smeltr::io::http::percent_encode(v)
                    )
                })
                .collect();
            uri = format!("{uri}?{}", query.join("&"));
        }
        // An expired session sends no CSRF value, and the gate refuses it as it would the GUI's.
        let csrf = self
            .auth
            .sessions
            .resolve(&self.token, Instant::now())
            .map(|s| s.csrf)
            .unwrap_or_default();
        let body = match &call.body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        };
        let request = axum::http::Request::builder()
            .method(call.method.as_str())
            .uri(uri)
            .header(
                header::COOKIE,
                format!("{}={}", self.auth.origin.cookie_name(), self.token),
            )
            .header(header::ORIGIN, &self.auth.origin.value)
            .header(CSRF_HEADER, csrf)
            .header(header::CONTENT_TYPE, "application/json")
            .extension(ByAgent)
            .body(body);
        let request = match request {
            Ok(request) => request,
            Err(e) => return failed(format!("the request could not be made: {e}")),
        };
        let mut router = router.clone();
        let response = match std::future::poll_fn(|cx| {
            tower_service::Service::<axum::http::Request<Body>>::poll_ready(&mut router, cx)
        })
        .await
        {
            Ok(()) => tower_service::Service::call(&mut router, request).await,
            Err(e) => match e {},
        };
        let response = match response {
            Ok(response) => response,
            Err(e) => match e {},
        };
        let status = response.status().as_u16();
        match axum::body::to_bytes(response.into_body(), MAX_ANSWER).await {
            Ok(bytes) => Answer {
                status,
                body: String::from_utf8_lossy(&bytes).into_owned(),
            },
            Err(_) => Answer {
                status,
                body: format!("[the answer was over {} bytes and was not read]", MAX_ANSWER),
            },
        }
    }
}

/// `GET /api/agent/conversations`: the caller's conversations, the one added to last first.
pub async fn list(State(auth): State<Auth>, actor: Extension<Actor>, by_agent: Option<Extension<ByAgent>>) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    let store = auth.store.clone();
    let user_id = actor.id.clone();
    match store_call(move || store.conversations_of(&user_id)).await {
        Ok(conversations) => {
            let runs = auth.chats.runs();
            let running = |id: &str| runs.get(id).is_some_and(|r| r.running);
            let conversations: Vec<ConversationSummary> = conversations
                .into_iter()
                .map(|c| {
                    let running = running(&c.id);
                    summary(c, running)
                })
                .collect();
            no_store(Envelope::ok(json!({"conversations": conversations})))
        }
        Err(response) => *response,
    }
}

/// `GET /api/agent/models`: the models the stored key reaches, and the administrator's, which a new conversation
/// starts from. Asked of the provider each time, so a model it adds shows without a restart.
pub async fn models(State(auth): State<Auth>, by_agent: Option<Extension<ByAgent>>) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    let store = auth.store.clone();
    let endpoint = match store_call(move || Settings::read(store.as_ref()).map(|s| s.endpoint())).await {
        Ok(Some(endpoint)) => endpoint,
        Ok(None) => return refuse(StatusCode::CONFLICT, "AI agents are not turned on"),
        Err(response) => return *response,
    };
    match model::models(&endpoint).await {
        Ok(models) => no_store(Envelope::ok(json!({"models": models, "default": endpoint.model}))),
        Err(why) => refuse(StatusCode::BAD_GATEWAY, &why),
    }
}

/// `GET /api/agent/conversations/:id`: one conversation, as what was said, asked and answered in order.
pub async fn read(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Path(id): Path<String>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    let conversation = match owned(&auth, &actor, &id).await {
        Ok(c) => c,
        Err(refused) => return *refused,
    };
    let (chats, store) = (auth.chats.clone(), auth.store.clone());
    let read = store_call(move || {
        let runs = chats.runs();
        Ok((store.turns_of(&id)?, runs.get(&id).cloned()))
    });
    let (turns, run) = match read.await {
        Ok(read) => read,
        Err(response) => return *response,
    };
    let running = run.as_ref().is_some_and(|r| r.running);
    let parsed: Vec<Turn> = turns.iter().filter_map(|t| serde_json::from_str(t).ok()).collect();
    let last = last_answer(&parsed);
    let body = ConversationDto {
        summary: summary(conversation, running),
        ended: run.as_ref().and_then(|r| r.ended).map(Ended::as_str),
        writing: run
            .as_ref()
            .filter(|r| r.running && !r.writing.is_empty())
            .map(|r| r.writing.clone()),
        error: run.and_then(|r| r.error),
        items: turns.iter().flat_map(|t| items(t)).collect(),
        model: last.and_then(|r| r.model.clone()),
        effort: last.and_then(|r| r.effort),
        context: filled(&parsed),
    };
    no_store(Envelope::ok(json!({"conversation": body})))
}

/// One stored turn as what the GUI shows: the person's messages, the agent's answers, its calls and their results.
fn items(stored: &str) -> Vec<Value> {
    match serde_json::from_str::<Turn>(stored) {
        Ok(Turn::Asked(said)) => said
            .into_iter()
            .map(|s| match s {
                Said::Text(text) => json!({"kind": "message", "text": text}),
                Said::Result {
                    call_id,
                    output,
                    is_error,
                } => json!({"kind": "result", "callId": call_id, "output": output, "isError": is_error}),
            })
            .collect(),
        Ok(Turn::Fresh(fresh)) => vec![json!({"kind": "fresh", "summary": fresh.summary})],
        Ok(Turn::Answered(reply)) => {
            let mut items = Vec::new();
            if !reply.text.is_empty() {
                items.push(json!({"kind": "answer", "text": reply.text}));
            }
            items.extend(
                reply
                    .calls
                    .into_iter()
                    .map(|c| json!({"kind": "call", "id": c.id, "input": c.input})),
            );
            items
        }
        Err(e) => vec![json!({"kind": "unreadable", "error": e.to_string()})],
    }
}

/// `POST /api/agent/conversations/:id/stop`: stop the running turn before its next step.
pub async fn stop(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Path(id): Path<String>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    if let Err(refused) = owned(&auth, &actor, &id).await {
        return *refused;
    }
    if let Some(run) = auth.chats.runs().get(&id) {
        run.stop.store(true, Ordering::SeqCst);
    }
    no_store(Envelope::done())
}

/// `DELETE /api/agent/conversations/:id`: delete a conversation and its turns, stopping one that runs.
pub async fn delete(
    State(auth): State<Auth>,
    actor: Extension<Actor>,
    by_agent: Option<Extension<ByAgent>>,
    Path(id): Path<String>,
) -> Response {
    if let Some(refused) = refused(&auth, &by_agent).await {
        return refused;
    }
    if let Err(refused) = owned(&auth, &actor, &id).await {
        return *refused;
    }
    let (chats, store) = (auth.chats.clone(), auth.store.clone());
    let deleted = store_call(move || {
        let mut runs = chats.runs();
        if let Some(run) = runs.remove(&id) {
            run.stop.store(true, Ordering::SeqCst);
        }
        store.delete_conversation(&id)
    });
    match deleted.await {
        Ok(_) => no_store(Envelope::done()),
        Err(response) => *response,
    }
}

/// Delete conversations nobody has added to for the days the settings keep them, on a schedule.
pub async fn sweep(auth: Auth) {
    let mut every = tokio::time::interval(SWEEP_EVERY);
    loop {
        every.tick().await;
        let store = auth.store.clone();
        let swept = blocking(move || {
            let days = Settings::read(store.as_ref())?.keep_days;
            store.delete_conversations_untouched_since(now_millis() - i64::from(days) * 24 * 60 * 60 * 1000)
        })
        .await;
        match swept {
            Ok(0) => {}
            Ok(n) => tracing::info!(
                target: "leader.conversations_swept",
                conversations = n,
                "conversations nobody added to were deleted"
            ),
            Err(e) => tracing::warn!(
                target: "leader.conversations_unswept",
                error = %e,
                "could not delete the conversations nobody added to"
            ),
        }
    }
}
