//! The model endpoint the leader's AI agent thinks with: the settings that name it, and one client over the
//! providers it can be.
//!
//! ```mermaid
//! flowchart LR
//!   A[agent loop] -->|Request| M[Model]
//!   M --> AN["Anthropic: /v1/messages"]
//!   M --> OA["OpenAI: /v1/responses"]
//!   M --> OC["OpenAI-compatible: /chat/completions"]
//!   AN & OA & OC -->|Reply| A
//! ```
//!
//! A conversation is kept in the client's own terms ([`Turn`]), and each provider translates it both ways. What a
//! model answered is also kept as the provider wrote it, and sent back so in the next request: thinking and
//! reasoning travel with it, and a provider refuses a conversation that drops them between tool calls.
//!
//! What a request carries stays under the model's context. Anthropic and OpenAI summarize the older turns
//! themselves once a request passes three quarters of the model's context ([`compact_at`]), and their summary comes back in the answer like thinking does;
//! for an OpenAI-compatible endpoint the leader asks the model for that summary and keeps it on the answer
//! ([`Reply`]). Either way the conversation itself is kept whole.
//!
//! The settings' model is where a conversation starts. Each request may name another model the key reaches
//! ([`models`]) and an effort, and its [`Reply`] keeps both, so the conversation carries on with them.
//!
//! A request that says where its text goes ([`Heard`]) is asked for as a stream, and each piece of the answer's
//! text goes there as it comes. The stream is folded back into the answer the provider would have sent whole, so
//! the [`Reply`] and what is kept are the same either way; a stream that ends early is an error, as a failed
//! request is.
//!
//! The endpoint is untrusted input like any other far end: the answer is read to a bound, and nothing in it is
//! followed. The API key is a sealed leader setting and goes nowhere but the endpoint it was issued by.

use std::time::Duration;

use serde_json::{Value, json};

use super::outbound;
use super::store::{Store, StoreError};
use crate::io::discovery::{check_url, said};

pub const ENABLED_KEY: &str = "agents.enabled";
pub const PROVIDER_KEY: &str = "agents.provider";
pub const ENDPOINT_KEY: &str = "agents.endpoint";
pub const MODEL_KEY: &str = "agents.model";
pub const API_KEY_KEY: &str = "agents.api_key";
pub const KEEP_DAYS_KEY: &str = "agents.keep_days";
pub const CONTEXT_KEY: &str = "agents.context_tokens";

/// How many days a conversation nobody adds to is kept, unless an administrator sets otherwise.
pub const DEFAULT_KEEP_DAYS: u32 = 90;

/// How long one answer may take. A model writing a long answer whole, not streamed, takes minutes.
const TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// The most an answer may hold. Far beyond the longest answer a model writes.
const MAX_ANSWER: usize = 4 * 1024 * 1024;

/// How long the check made when AI agents are turned on may take. A short answer comes back in seconds.
const CHECK_TIMEOUT: Duration = Duration::from_secs(30);

/// What the Anthropic Messages API is asked to speak.
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The beta Anthropic's compaction at a token threshold is behind.
const ANTHROPIC_COMPACTION: &str = "compact-2026-01-12";

/// The tokens a model's context holds, unless an administrator sets otherwise or Anthropic says: what every
/// current model holds at least.
pub const DEFAULT_CONTEXT: u64 = 200_000;

/// The fewest and most tokens a context may be set to.
pub const CONTEXT_RANGE: std::ops::RangeInclusive<u64> = 4_096..=10_000_000;

/// The fewest input tokens Anthropic's compaction may be set to start at.
const ANTHROPIC_LEAST_COMPACTION: u64 = 50_000;

/// The input tokens at which a conversation's older turns are summarized: three quarters of the model's context,
/// leaving room for the answer and the round after.
fn compact_at(context: u64) -> u64 {
    context / 4 * 3
}

/// Which API an endpoint speaks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Provider {
    /// The Anthropic Messages API.
    #[default]
    #[serde(rename = "anthropic")]
    Anthropic,
    /// OpenAI's Responses API.
    #[serde(rename = "openai")]
    OpenAi,
    /// Any service speaking OpenAI's Chat Completions API. Its URL is the base the service documents, which the
    /// request path follows.
    #[serde(rename = "openai-compatible")]
    OpenAiCompatible,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Provider::Anthropic => "anthropic",
            Provider::OpenAi => "openai",
            Provider::OpenAiCompatible => "openai-compatible",
        }
    }

    pub fn parse(text: &str) -> Option<Provider> {
        [Provider::Anthropic, Provider::OpenAi, Provider::OpenAiCompatible]
            .into_iter()
            .find(|p| p.as_str() == text)
    }
}

/// What an administrator set under Admin › System › AI agents.
#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    pub enabled: bool,
    pub provider: Provider,
    pub endpoint: String,
    pub model: String,
    pub api_key: Option<String>,
    /// How many days a conversation nobody adds to is kept.
    pub keep_days: u32,
    /// The tokens the model's context holds.
    pub context: u64,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("enabled", &self.enabled)
            .field("provider", &self.provider)
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("api_key_set", &self.api_key.is_some())
            .field("keep_days", &self.keep_days)
            .field("context", &self.context)
            .finish()
    }
}

impl Settings {
    /// A provider this build does not know reads as Anthropic, turned off: what a newer build stored is not
    /// guessed at.
    pub fn read(store: &dyn Store) -> Result<Settings, StoreError> {
        let provider = store.setting(PROVIDER_KEY)?;
        let known = provider.as_deref().is_none_or(|p| Provider::parse(p).is_some());
        Ok(Settings {
            enabled: known && store.setting(ENABLED_KEY)?.as_deref() == Some("true"),
            provider: provider.as_deref().and_then(Provider::parse).unwrap_or_default(),
            endpoint: store.setting(ENDPOINT_KEY)?.unwrap_or_default(),
            model: store.setting(MODEL_KEY)?.unwrap_or_default(),
            api_key: store.opened_setting(API_KEY_KEY)?.filter(|k| !k.is_empty()),
            keep_days: store
                .setting(KEEP_DAYS_KEY)?
                .and_then(|d| d.parse().ok())
                .filter(|d| *d > 0)
                .unwrap_or(DEFAULT_KEEP_DAYS),
            context: store
                .setting(CONTEXT_KEY)?
                .and_then(|t| t.parse().ok())
                .filter(|t| CONTEXT_RANGE.contains(t))
                .unwrap_or(DEFAULT_CONTEXT),
        })
    }

    /// The endpoint to ask, when AI agents are turned on and it is set.
    pub fn endpoint(&self) -> Option<Endpoint> {
        let key = self.api_key.clone()?;
        (self.enabled && !self.endpoint.is_empty() && !self.model.is_empty()).then(|| Endpoint {
            provider: self.provider,
            url: self.endpoint.clone(),
            model: self.model.clone(),
            api_key: key,
            context: self.context,
        })
    }
}

/// Whether AI agents are turned on and their endpoint set: what the agent's screens and APIs wait on.
pub fn on(store: &dyn Store) -> Result<bool, StoreError> {
    Ok(Settings::read(store)?.endpoint().is_some())
}

/// One model at one endpoint, and the key it is asked with.
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub provider: Provider,
    pub url: String,
    pub model: String,
    pub api_key: String,
    /// The tokens the model's context holds.
    pub context: u64,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("provider", &self.provider)
            .field("url", &self.url)
            .field("model", &self.model)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

/// A tool the model may call.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    /// JSON Schema for the tool's input object.
    pub input_schema: Value,
}

/// A tool call the model made.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Call {
    /// What the result is sent back under.
    pub id: String,
    pub name: String,
    pub input: Value,
}

/// What is said to the model.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Said {
    Text(String),
    /// What a tool call came back with.
    Result {
        call_id: String,
        output: String,
        is_error: bool,
    },
}

/// One turn of a conversation.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Turn {
    Asked(Vec<Said>),
    Answered(Reply),
    /// Where the model's reading of the conversation begins again.
    Fresh(Fresh),
}

/// A fresh start: the person summarized the conversation or cleared it, and a request reads nothing before it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Fresh {
    /// What the model wrote of the conversation before, which stands in for it. None where it was cleared.
    pub summary: Option<String>,
    /// The tokens the summary took to write: what the context holds until the next answer says.
    pub tokens: u64,
}

/// The turns a request reads, from the newest [`Turn::Fresh`] on, its summary said first. Borrowed where there is
/// none.
fn readable(turns: &[Turn]) -> std::borrow::Cow<'_, [Turn]> {
    let Some(at) = turns.iter().rposition(|t| matches!(t, Turn::Fresh(_))) else {
        return std::borrow::Cow::Borrowed(turns);
    };
    let Turn::Fresh(fresh) = &turns[at] else { unreachable!() };
    let mut after = turns[at + 1..].to_vec();
    if let Some(summary) = &fresh.summary {
        let said = Said::Text(format!("Summary of the conversation before this:\n{summary}"));
        match after.first_mut() {
            Some(Turn::Asked(first)) => first.insert(0, said),
            _ => after.insert(0, Turn::Asked(vec![said])),
        }
    }
    std::borrow::Cow::Owned(after)
}

/// Why the model stopped.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Stop {
    /// It said what it had to say.
    Done,
    /// It waits on the results of its calls.
    Calls,
    /// It ran out of tokens before it finished.
    Tokens,
    /// It declined to answer.
    Refused,
    /// What the provider said, when none of the above.
    Other(String),
}

/// Tokens a request spent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Usage {
    /// Everything read, cached or not.
    pub input: u64,
    pub output: u64,
}

/// How hard a model works at an answer: how long it thinks before it answers. Unset, the provider's default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl Effort {
    pub const ALL: [Effort; 5] = [Effort::Low, Effort::Medium, Effort::High, Effort::Xhigh, Effort::Max];

    pub fn as_str(self) -> &'static str {
        match self {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Xhigh => "xhigh",
            Effort::Max => "max",
        }
    }
}

/// What the model answered.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Reply {
    pub text: String,
    pub calls: Vec<Call>,
    pub stop: Stop,
    pub usage: Usage,
    /// The answer as the provider wrote it, sent back unchanged in a later [`Turn::Answered`].
    native: Value,
    /// For an endpoint with no compaction of its own, what the leader had the model write of the conversation
    /// before the person's latest message: it stood in for those turns in this answer's request, and does in every
    /// later one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    /// The model that answered, the effort it was asked for, and the tokens its context holds: what the person's
    /// next message starts from, and how full the conversation is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<Effort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<u64>,
}

/// What the model is asked.
#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    pub system: &'a str,
    pub turns: &'a [Turn],
    pub tools: &'a [Tool],
    /// The most the answer may spend, thinking included.
    pub max_tokens: u32,
    /// Whether the model may think before it answers, where the provider lets it be asked.
    pub think: bool,
    /// Whether the older turns may be summarized once a request passes three quarters of the model's context. A
    /// model that refuses to is asked again without.
    pub compact: bool,
    pub effort: Option<Effort>,
    /// What OpenAI keeps one conversation's requests together in its cache by: the conversation's id.
    pub cache_key: Option<&'a str>,
    /// Who is asking, as a hash OpenAI tells people apart by for abuse without learning who they are.
    pub person: Option<&'a str>,
    /// Where the answer's text goes as the model writes it. The provider is asked for a stream when there is one.
    pub heard: Option<Heard<'a>>,
}

/// What hears an answer's text as it is written, a piece at a time.
#[derive(Clone, Copy)]
pub struct Heard<'a>(pub &'a (dyn Fn(&str) + Send + Sync));

impl std::fmt::Debug for Heard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Heard")
    }
}

/// A model endpoint. Each request is sent and answered whole.
#[async_trait::async_trait]
pub trait Model: Send + Sync {
    async fn ask(&self, request: Request<'_>) -> Result<Reply, String>;
}

/// The client for `endpoint`, which speaks its provider's API. A redirect is never followed: the key rides in a
/// header any host it went to would read.
pub fn model(endpoint: Endpoint) -> Result<Box<dyn Model>, String> {
    let client = outbound::client(
        &endpoint.url,
        "the endpoint",
        TIMEOUT,
        reqwest::redirect::Policy::none(),
    )?;
    let base = endpoint.url.trim_end_matches('/').to_string();
    Ok(Box::new(Client { client, base, endpoint }))
}

struct Client {
    client: reqwest::Client,
    /// The endpoint's URL, without a trailing slash, for the request path to follow.
    base: String,
    endpoint: Endpoint,
}

#[async_trait::async_trait]
impl Model for Client {
    async fn ask(&self, request: Request<'_>) -> Result<Reply, String> {
        let turns = readable(request.turns);
        let request = Request {
            turns: &turns,
            ..request
        };
        if let Some((summary, at)) = self.summary(request).await {
            let reply = self.ask_summarized(request, summary, at).await?;
            return Ok(self.made(reply, request));
        }
        // A model too old for compaction, or a provider's endpoint that does not offer it, is asked again without.
        let answer = match self.send(request).await {
            Err(why) if request.compact && refused_compaction(&why) => {
                self.send(Request {
                    compact: false,
                    ..request
                })
                .await
            }
            answered => answered,
        }?;
        let reply = match self.endpoint.provider {
            Provider::Anthropic => anthropic_reply(answer),
            Provider::OpenAi => openai_reply(answer),
            Provider::OpenAiCompatible => compatible_reply(answer),
        }?;
        Ok(self.made(reply, request))
    }
}

/// The ask for a summary of the conversation, to carry on from in place of it.
pub const SUMMARIZE: &str = "Summarize the conversation so far for yourself, to carry on from in place of it: what the \
                         person asked for, what you found and changed, on which branch, what is left open, and the ids, \
                         names and values you will need. Write the summary only, and call no tool.";

/// The most a summary may spend.
pub const SUMMARY_TOKENS: u32 = 8_000;

/// Where the person's latest message is in `turns`, when there is anything before it to summarize.
fn latest_message(turns: &[Turn]) -> Option<usize> {
    turns
        .iter()
        .rposition(|t| matches!(t, Turn::Asked(said) if said.iter().any(|s| matches!(s, Said::Text(_)))))
        .filter(|at| *at > 0)
}

/// The summary that stands in for the turns before the person's message it was written ahead of, and the turns
/// from that message on.
fn summarized(turns: &[Turn]) -> (Option<&str>, &[Turn]) {
    let Some(answered) = turns
        .iter()
        .rposition(|t| matches!(t, Turn::Answered(Reply { summary: Some(_), .. })))
    else {
        return (None, turns);
    };
    let Turn::Answered(Reply {
        summary: Some(summary), ..
    }) = &turns[answered]
    else {
        unreachable!()
    };
    let from = latest_message(&turns[..answered]).unwrap_or(0);
    (Some(summary.as_str()), &turns[from..])
}

impl Client {
    /// `reply`, saying what made it.
    fn made(&self, reply: Reply, request: Request<'_>) -> Reply {
        Reply {
            model: Some(self.endpoint.model.clone()),
            effort: request.effort,
            context: Some(self.endpoint.context),
            ..reply
        }
    }

    /// For an OpenAI-compatible endpoint, which has no compaction of its own: once the last answer's request and
    /// what it wrote pass [`compact_at`] the context, a summary of the turns before the person's latest message, for the next
    /// answer to carry, and where that message is. `None` when none is due or the endpoint wrote none.
    async fn summary(&self, request: Request<'_>) -> Option<(String, usize)> {
        let last = request.turns.iter().rev().find_map(|t| match t {
            Turn::Answered(reply) => Some(reply.usage.input + reply.usage.output),
            Turn::Asked(_) | Turn::Fresh(_) => None,
        })?;
        let due = last >= compact_at(self.endpoint.context);
        if self.endpoint.provider != Provider::OpenAiCompatible || !request.compact || !due {
            return None;
        }
        let (_, rest) = summarized(request.turns);
        let at = latest_message(rest)? + request.turns.len() - rest.len();
        let mut turns = request.turns[..at].to_vec();
        // The calls the turns end on are answered at the person's message; the summary must see them answered.
        let mut ask: Vec<Said> = match &request.turns[at] {
            Turn::Asked(said) => said
                .iter()
                .filter(|s| matches!(s, Said::Result { .. }))
                .cloned()
                .collect(),
            Turn::Answered(_) | Turn::Fresh(_) => Vec::new(),
        };
        ask.push(Said::Text(SUMMARIZE.to_string()));
        turns.push(Turn::Asked(ask));
        let asked = Request {
            turns: &turns,
            tools: &[],
            max_tokens: SUMMARY_TOKENS,
            think: false,
            compact: false,
            effort: None,
            heard: None,
            ..request
        };
        let reply = compatible_reply(self.send(asked).await.ok()?).ok()?;
        (reply.stop == Stop::Done && !reply.text.trim().is_empty()).then_some((reply.text, at))
    }

    /// `request` asked with `summary` standing in for the turns before the person's latest message, and the answer
    /// carrying it for the requests after.
    async fn ask_summarized(&self, request: Request<'_>, summary: String, at: usize) -> Result<Reply, String> {
        let url = format!("{}/chat/completions", self.base);
        let call = self.client.post(&url).bearer_auth(&self.endpoint.api_key);
        let body = compatible_body_from(&self.endpoint.model, request, Some(&summary), &request.turns[at..]);
        let mut reply = compatible_reply(self.answered(call, &url, body, request.heard).await?)?;
        reply.summary = Some(summary);
        Ok(reply)
    }

    /// `request` in the provider's terms, and what it answered.
    async fn send(&self, request: Request<'_>) -> Result<Value, String> {
        let Endpoint {
            provider,
            model,
            api_key,
            context,
            ..
        } = &self.endpoint;
        match provider {
            Provider::Anthropic => {
                let url = format!("{}/v1/messages", self.base);
                let mut call = self
                    .client
                    .post(&url)
                    .header("x-api-key", api_key)
                    .header("anthropic-version", ANTHROPIC_VERSION);
                // A summary already in the conversation is read only under the beta it was written under.
                let summarized = request
                    .turns
                    .iter()
                    .any(|t| matches!(t, Turn::Answered(r) if holds_compaction(&r.native)));
                if request.compact || summarized {
                    call = call.header("anthropic-beta", ANTHROPIC_COMPACTION);
                }
                self.answered(
                    call,
                    &url,
                    anthropic_body(model, request, compact_at(*context)),
                    request.heard,
                )
                .await
            }
            Provider::OpenAi => {
                let url = format!("{}/v1/responses", self.base);
                let call = self.client.post(&url).bearer_auth(api_key);
                self.answered(
                    call,
                    &url,
                    openai_body(model, request, compact_at(*context)),
                    request.heard,
                )
                .await
            }
            Provider::OpenAiCompatible => {
                let url = format!("{}/chat/completions", self.base);
                let call = self.client.post(&url).bearer_auth(api_key);
                self.answered(call, &url, compatible_body(model, request), request.heard)
                    .await
            }
        }
    }

    /// POST `body` to `url` and the answer, as the provider sends it whole. With `heard`, the provider is asked for
    /// a stream, its text goes to `heard` as it comes, and the stream is folded back into that same answer.
    async fn answered(
        &self,
        call: reqwest::RequestBuilder,
        url: &str,
        mut body: Value,
        heard: Option<Heard<'_>>,
    ) -> Result<Value, String> {
        let Some(heard) = heard else {
            return post(call, url, &body).await;
        };
        body["stream"] = json!(true);
        if self.endpoint.provider == Provider::OpenAiCompatible {
            body["stream_options"] = json!({"include_usage": true});
        }
        let response = sent(json_body(call, &body), url).await?;
        let mut folded = Folded::new(self.endpoint.provider);
        events(response, url, |data| folded.add(data, heard)).await?;
        folded.answer()
    }
}

/// Each event of the server-sent stream `response`, as the text of its data, read to [`MAX_ANSWER`] in all.
async fn events(
    mut response: reqwest::Response,
    url: &str,
    mut each: impl FnMut(&str) -> Result<(), String>,
) -> Result<(), String> {
    let (mut read, mut pending, mut data) = (0, Vec::new(), String::new());
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("{url}: could not read the answer: {e}"))?
    {
        read += chunk.len();
        if read > MAX_ANSWER {
            return Err(format!("{url}: the answer is longer than {} KiB", MAX_ANSWER / 1024));
        }
        pending.extend_from_slice(&chunk);
        // A line ends at a newline byte, which no character of a longer UTF-8 sequence holds.
        while let Some(end) = pending.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = pending.drain(..=end).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                if !data.is_empty() {
                    each(&data)?;
                    data.clear();
                }
            } else if let Some(field) = line.strip_prefix("data:") {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(field.strip_prefix(' ').unwrap_or(field));
            }
        }
    }
    match data.is_empty() {
        true => Ok(()),
        false => each(&data),
    }
}

/// A provider's stream folded back into the answer it sends whole, for its `*_reply` to read.
struct Folded {
    provider: Provider,
    answer: Value,
    /// Anthropic's tool input, as the pieces of JSON written so far, by block.
    input: std::collections::HashMap<usize, String>,
    done: bool,
}

impl Folded {
    fn new(provider: Provider) -> Folded {
        Folded {
            provider,
            answer: Value::Null,
            input: std::collections::HashMap::new(),
            done: false,
        }
    }

    fn add(&mut self, data: &str, heard: Heard<'_>) -> Result<(), String> {
        if self.provider == Provider::OpenAiCompatible && data.trim() == "[DONE]" {
            self.done = true;
            return Ok(());
        }
        let event: Value =
            serde_json::from_str(data).map_err(|e| format!("the endpoint streamed something that is not JSON: {e}"))?;
        match self.provider {
            Provider::Anthropic => self.anthropic(event, heard),
            Provider::OpenAi => self.openai(event, heard),
            Provider::OpenAiCompatible => self.compatible(event, heard),
        }
    }

    /// The answer, once the stream said it was whole.
    fn answer(self) -> Result<Value, String> {
        match self.done {
            true => Ok(self.answer),
            false => Err("the endpoint's stream ended before its answer did".to_string()),
        }
    }

    /// Anthropic sends no whole message at the end: each block is started, written to and stopped, and the stop
    /// reason and usage come last.
    fn anthropic(&mut self, event: Value, heard: Heard<'_>) -> Result<(), String> {
        let at = event["index"].as_u64().unwrap_or_default() as usize;
        match event["type"].as_str().unwrap_or_default() {
            "message_start" => self.answer = event["message"].clone(),
            "content_block_start" => {
                let blocks = self.answer["content"]
                    .as_array_mut()
                    .ok_or("the endpoint streamed a block before its message")?;
                if blocks.len() != at {
                    return Err("the endpoint streamed its blocks out of order".to_string());
                }
                blocks.push(event["content_block"].clone());
            }
            "content_block_delta" => {
                let delta = &event["delta"];
                if delta["type"] == "input_json_delta" {
                    let piece = delta["partial_json"].as_str().unwrap_or_default();
                    self.input.entry(at).or_default().push_str(piece);
                    return Ok(());
                }
                if delta["type"] == "text_delta" {
                    (heard.0)(delta["text"].as_str().unwrap_or_default());
                }
                let block = self.answer["content"]
                    .get_mut(at)
                    .ok_or("the endpoint streamed into a block it never started")?;
                // Text, thinking, a signature, a summary: each piece adds to the block's field of the same name.
                for (field, piece) in delta.as_object().into_iter().flatten().filter(|(k, _)| *k != "type") {
                    match piece.as_str() {
                        Some(piece) => append(&mut block[field], piece),
                        None => block[field] = piece.clone(),
                    }
                }
            }
            "content_block_stop" => {
                if let Some(input) = self.input.remove(&at) {
                    self.answer["content"][at]["input"] = match input.trim() {
                        "" => json!({}),
                        input => serde_json::from_str(input)
                            .map_err(|e| format!("the endpoint streamed a tool input that is not JSON: {e}"))?,
                    };
                }
            }
            "message_delta" => {
                for (field, value) in event["delta"].as_object().into_iter().flatten() {
                    self.answer[field] = value.clone();
                }
                for (field, value) in event["usage"].as_object().into_iter().flatten() {
                    self.answer["usage"][field] = value.clone();
                }
            }
            "message_stop" => self.done = true,
            "error" => return Err(streamed_error(&event["error"])),
            _ => {}
        }
        Ok(())
    }

    /// OpenAI's last event carries the whole response.
    fn openai(&mut self, event: Value, heard: Heard<'_>) -> Result<(), String> {
        match event["type"].as_str().unwrap_or_default() {
            "response.output_text.delta" => (heard.0)(event["delta"].as_str().unwrap_or_default()),
            "response.completed" | "response.incomplete" => {
                self.answer = event["response"].clone();
                self.done = true;
            }
            "response.failed" => return Err(streamed_error(&event["response"]["error"])),
            "error" => return Err(streamed_error(&event)),
            _ => {}
        }
        Ok(())
    }

    /// Chat Completions streams the message a piece at a time, each tool call's arguments by its index.
    fn compatible(&mut self, event: Value, heard: Heard<'_>) -> Result<(), String> {
        if event.get("error").is_some() {
            return Err(streamed_error(&event["error"]));
        }
        if self.answer.is_null() {
            self.answer = json!({"choices": [{"message": {"role": "assistant", "content": null}}]});
        }
        if event["usage"].is_object() {
            self.answer["usage"] = event["usage"].clone();
        }
        let Some(choice) = event["choices"].get(0) else {
            return Ok(());
        };
        let message = &mut self.answer["choices"][0]["message"];
        if let Some(text) = choice["delta"]["content"].as_str() {
            (heard.0)(text);
            append(&mut message["content"], text);
        }
        for call in choice["delta"]["tool_calls"].as_array().into_iter().flatten() {
            let at = call["index"].as_u64().unwrap_or_default() as usize;
            if !message["tool_calls"].is_array() {
                message["tool_calls"] = json!([]);
            }
            let calls = message["tool_calls"].as_array_mut().expect("set just above");
            while calls.len() <= at {
                calls.push(json!({"id": "", "type": "function", "function": {"name": "", "arguments": ""}}));
            }
            let kept = &mut calls[at];
            if let Some(id) = call["id"].as_str() {
                kept["id"] = json!(id);
            }
            if let Some(name) = call["function"]["name"].as_str() {
                kept["function"]["name"] = json!(name);
            }
            if let Some(piece) = call["function"]["arguments"].as_str() {
                append(&mut kept["function"]["arguments"], piece);
            }
        }
        if let Some(reason) = choice["finish_reason"].as_str() {
            self.answer["choices"][0]["finish_reason"] = json!(reason);
            // A service that ends without `[DONE]` has still finished its answer.
            self.done = true;
        }
        Ok(())
    }
}

/// Add `piece` to the end of the string `slot`, which a null or other value is taken to be empty.
fn append(slot: &mut Value, piece: &str) {
    match slot {
        Value::String(so_far) => so_far.push_str(piece),
        _ => *slot = json!(piece),
    }
}

/// What a provider's error, sent in its stream, says.
fn streamed_error(error: &Value) -> String {
    match error["message"].as_str() {
        Some(message) => format!("the endpoint failed part way: {}", said(message.as_bytes())),
        None => "the endpoint failed part way through its answer".to_string(),
    }
}

/// Whether `why`, what [`post`] said, is an endpoint refusing the request for its compaction.
fn refused_compaction(why: &str) -> bool {
    let why = why.to_ascii_lowercase();
    why.contains(" answered 400") && (why.contains("compact") || why.contains("context_management"))
}

/// Ask `endpoint` for a short answer, so a setting whose key, model or URL cannot work is found when it is saved.
pub async fn check(endpoint: Endpoint) -> Result<(), String> {
    let model = model(endpoint)?;
    let turns = [Turn::Asked(vec![Said::Text("Answer with the word ok.".to_string())])];
    let asked = model.ask(Request {
        system: "",
        turns: &turns,
        tools: &[],
        max_tokens: 64,
        think: false,
        compact: false,
        effort: None,
        cache_key: None,
        person: None,
        heard: None,
    });
    match tokio::time::timeout(CHECK_TIMEOUT, asked).await {
        Ok(answered) => answered.map(drop),
        Err(_) => Err(format!(
            "the endpoint did not answer within {} seconds",
            CHECK_TIMEOUT.as_secs()
        )),
    }
}

/// The tokens Anthropic says `endpoint`'s model holds, from its Models API. `None` for another provider, or when it
/// does not say.
pub async fn context_of(endpoint: &Endpoint) -> Option<u64> {
    if endpoint.provider != Provider::Anthropic {
        return None;
    }
    let model = models_get(endpoint, &format!("/v1/models/{}", endpoint.model))
        .await
        .ok()?;
    listed(&model).context_tokens
}

/// A model `endpoint`'s key reaches. Only Anthropic says how much it holds and which efforts it takes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Listed {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub efforts: Option<Vec<Effort>>,
}

/// The most pages of Anthropic's model list read: far more models than it has.
const MODEL_PAGES: usize = 10;

/// The models `endpoint`'s key reaches, in the order the provider lists them, which for Anthropic is newest first.
pub async fn models(endpoint: &Endpoint) -> Result<Vec<Listed>, String> {
    let mut models = Vec::new();
    match endpoint.provider {
        Provider::Anthropic => {
            let mut after = None::<String>;
            for _ in 0..MODEL_PAGES {
                let page = match &after {
                    None => "/v1/models?limit=1000".to_string(),
                    Some(id) => format!("/v1/models?limit=1000&after_id={id}"),
                };
                let page = models_get(endpoint, &page).await?;
                models.extend(page["data"].as_array().into_iter().flatten().map(listed));
                match (page["has_more"].as_bool(), page["last_id"].as_str()) {
                    (Some(true), Some(last)) => after = Some(last.to_string()),
                    _ => break,
                }
            }
        }
        Provider::OpenAi => {
            let list = models_get(endpoint, "/v1/models").await?;
            models.extend(list["data"].as_array().into_iter().flatten().map(listed));
        }
        // The service's documented base already carries its version, as `/chat/completions` does.
        Provider::OpenAiCompatible => {
            let list = models_get(endpoint, "/models").await?;
            models.extend(list["data"].as_array().into_iter().flatten().map(listed));
        }
    }
    models.retain(|m| !m.id.is_empty());
    Ok(models)
}

/// One model as a provider's list writes it: Anthropic's `display_name`, `max_input_tokens` and
/// `capabilities.effort`, or OpenAI's bare `id`.
fn listed(model: &Value) -> Listed {
    let id = model["id"].as_str().unwrap_or_default().to_string();
    let effort = &model["capabilities"]["effort"];
    Listed {
        name: model["display_name"].as_str().unwrap_or(&id).to_string(),
        context_tokens: model["max_input_tokens"].as_u64().filter(|t| CONTEXT_RANGE.contains(t)),
        efforts: (effort["supported"] == true).then(|| {
            Effort::ALL
                .into_iter()
                .filter(|e| effort[e.as_str()]["supported"] == true)
                .collect()
        }),
        id,
    }
}

/// GET `path` under `endpoint`'s URL with its key.
async fn models_get(endpoint: &Endpoint, path: &str) -> Result<Value, String> {
    let url = format!("{}{path}", endpoint.url.trim_end_matches('/'));
    let client = outbound::client(
        &endpoint.url,
        "the endpoint",
        CHECK_TIMEOUT,
        reqwest::redirect::Policy::none(),
    )?;
    let call = match endpoint.provider {
        Provider::Anthropic => client
            .get(&url)
            .header("x-api-key", &endpoint.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION),
        Provider::OpenAi | Provider::OpenAiCompatible => client.get(&url).bearer_auth(&endpoint.api_key),
    };
    send_json(call, &url).await
}

/// Whether `url` may name a model endpoint: the rule an OIDC issuer follows, https unless it is on this host.
pub fn check_endpoint(url: &str) -> Result<(), String> {
    check_url(url, false).map(drop)
}

/// POST `body` to `url` and read the answer, to [`MAX_ANSWER`].
async fn post(request: reqwest::RequestBuilder, url: &str, body: &Value) -> Result<Value, String> {
    send_json(json_body(request, body), url).await
}

fn json_body(request: reqwest::RequestBuilder, body: &Value) -> reqwest::RequestBuilder {
    request
        .header("content-type", "application/json")
        .body(body.to_string())
}

/// Send `request` to `url` and read the answer, to [`MAX_ANSWER`].
async fn send_json(request: reqwest::RequestBuilder, url: &str) -> Result<Value, String> {
    let answer = outbound::read(sent(request, url).await?, MAX_ANSWER)
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    serde_json::from_slice(&answer).map_err(|e| format!("{url} answered something that is not JSON: {e}"))
}

/// Send `request` to `url`, and its answer unread when it succeeded, or what its refusal says.
async fn sent(request: reqwest::RequestBuilder, url: &str) -> Result<reqwest::Response, String> {
    let response = request
        .send()
        .await
        .map_err(|e| format!("could not reach {url}: {e}"))?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let answer = outbound::read(response, MAX_ANSWER)
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    Err(refusal(url, status, &answer))
}

/// What `url` refusing with `status` and `answer` says. The provider's own sentence says what is wrong: a bad key,
/// an unknown model.
fn refusal(url: &str, status: reqwest::StatusCode, answer: &[u8]) -> String {
    let why = serde_json::from_slice::<Value>(answer)
        .ok()
        .and_then(|v| v["error"]["message"].as_str().map(|m| said(m.as_bytes())))
        .unwrap_or_else(|| said(answer));
    match why.is_empty() {
        true => format!("{url} answered {}", status.as_u16()),
        false => format!("{url} answered {}: {why}", status.as_u16()),
    }
}

fn tokens(value: &Value) -> u64 {
    value.as_u64().unwrap_or(0)
}

/// A tool call's input, which OpenAI's APIs send as JSON text. Text that is not JSON is passed on as text, so the
/// tool refuses it rather than the conversation.
fn arguments(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

// -- Anthropic ------------------------------------------------------------------------------------------------------

fn anthropic_body(model: &str, request: Request<'_>, compact_at: u64) -> Value {
    // A fresh start is never among the turns: requests read from after it (`readable`).
    let messages: Vec<Value> = since_compaction(request.turns)
        .iter()
        .filter_map(|turn| match turn {
            Turn::Asked(said) => Some(json!({
                "role": "user",
                "content": said.iter().map(|s| match s {
                    Said::Text(text) => json!({"type": "text", "text": text}),
                    Said::Result { call_id, output, is_error } => json!({
                        "type": "tool_result", "tool_use_id": call_id, "content": output, "is_error": is_error,
                    }),
                }).collect::<Vec<_>>(),
            })),
            Turn::Answered(reply) => Some(json!({"role": "assistant", "content": reply.native})),
            Turn::Fresh(_) => None,
        })
        .collect();
    // Caches the conversation so far, so a turn's later rounds read the system prompt and tool catalog cheaply.
    let mut body = json!({
        "model": model, "max_tokens": request.max_tokens, "messages": messages,
        "cache_control": {"type": "ephemeral"},
    });
    if !request.system.is_empty() {
        // Its own breakpoint keeps the tools and the system prompt cached once a summary replaces the turns.
        body["system"] = json!([{"type": "text", "text": request.system, "cache_control": {"type": "ephemeral"}}]);
    }
    if !request.tools.is_empty() {
        body["tools"] = request
            .tools
            .iter()
            .map(|t| json!({"name": t.name, "description": t.description, "input_schema": t.input_schema}))
            .collect();
    }
    if request.think {
        body["thinking"] = json!({"type": "adaptive"});
    }
    if let Some(effort) = request.effort {
        body["output_config"] = json!({"effort": effort.as_str()});
    }
    if request.compact && compact_at >= ANTHROPIC_LEAST_COMPACTION {
        body["context_management"] = json!({"edits": [{
            "type": "compact_20260112",
            "trigger": {"type": "input_tokens", "value": compact_at},
        }]});
    }
    body
}

/// The turns from the last answer holding a summary of those before it: the provider reads nothing before that,
/// so nothing is sent.
fn since_compaction(turns: &[Turn]) -> &[Turn] {
    let from = turns
        .iter()
        .rposition(|turn| matches!(turn, Turn::Answered(reply) if holds_compaction(&reply.native)))
        .unwrap_or(0);
    &turns[from..]
}

/// Whether an answer, as the provider wrote it, holds a summary of the conversation before it: Anthropic's block
/// and OpenAI's item are both of type `compaction`.
fn holds_compaction(native: &Value) -> bool {
    native
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|b| b["type"] == "compaction"))
}

fn anthropic_reply(answer: Value) -> Result<Reply, String> {
    let content = answer["content"]
        .as_array()
        .ok_or("the endpoint answered with no content")?;
    let text = content
        .iter()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("");
    let calls = content
        .iter()
        .filter(|b| b["type"] == "tool_use")
        .map(|b| Call {
            id: b["id"].as_str().unwrap_or_default().to_string(),
            name: b["name"].as_str().unwrap_or_default().to_string(),
            input: b["input"].clone(),
        })
        .collect();
    let stop = match answer["stop_reason"].as_str().unwrap_or_default() {
        "end_turn" | "stop_sequence" => Stop::Done,
        "tool_use" => Stop::Calls,
        "max_tokens" => Stop::Tokens,
        "refusal" => Stop::Refused,
        other => Stop::Other(other.to_string()),
    };
    let usage = &answer["usage"];
    Ok(Reply {
        text,
        calls,
        stop,
        usage: Usage {
            input: tokens(&usage["input_tokens"])
                + tokens(&usage["cache_creation_input_tokens"])
                + tokens(&usage["cache_read_input_tokens"]),
            output: tokens(&usage["output_tokens"]),
        },
        native: Value::Array(content.clone()),
        summary: None,
        model: None,
        effort: None,
        context: None,
    })
}

// -- OpenAI ---------------------------------------------------------------------------------------------------------

/// Nothing is stored at OpenAI: each request carries the whole conversation, its reasoning included, encrypted.
fn openai_body(model: &str, request: Request<'_>, compact_at: u64) -> Value {
    let mut input = Vec::new();
    for turn in since_compaction(request.turns) {
        match turn {
            Turn::Asked(said) => {
                for s in said {
                    input.push(match s {
                        Said::Text(text) => json!({"role": "user", "content": text}),
                        Said::Result { call_id, output, .. } => {
                            json!({"type": "function_call_output", "call_id": call_id, "output": output})
                        }
                    });
                }
            }
            Turn::Answered(reply) => input.extend(reply.native.as_array().cloned().unwrap_or_default()),
            Turn::Fresh(_) => {}
        }
    }
    let mut body = json!({
        "model": model,
        "input": input,
        "max_output_tokens": request.max_tokens,
        "store": false,
        "include": ["reasoning.encrypted_content"],
    });
    if !request.system.is_empty() {
        body["instructions"] = json!(request.system);
    }
    if request.compact {
        body["context_management"] = json!([{"type": "compaction", "compact_threshold": compact_at}]);
    }
    if let Some(effort) = request.effort {
        body["reasoning"] = json!({"effort": effort.as_str()});
    }
    if let Some(key) = request.cache_key {
        body["prompt_cache_key"] = json!(key);
    }
    if let Some(person) = request.person {
        body["safety_identifier"] = json!(person);
    }
    if !request.tools.is_empty() {
        body["tools"] = request
            .tools
            .iter()
            .map(|t| {
                json!({"type": "function", "name": t.name, "description": t.description, "parameters": t.input_schema})
            })
            .collect();
    }
    body
}

fn openai_reply(answer: Value) -> Result<Reply, String> {
    let output = answer["output"]
        .as_array()
        .ok_or("the endpoint answered with no output")?;
    let text = output
        .iter()
        .filter(|item| item["type"] == "message")
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .filter(|part| part["type"] == "output_text")
        .filter_map(|part| part["text"].as_str().map(str::to_string))
        .collect::<Vec<_>>()
        .join("");
    let calls: Vec<Call> = output
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| Call {
            id: item["call_id"].as_str().unwrap_or_default().to_string(),
            name: item["name"].as_str().unwrap_or_default().to_string(),
            input: arguments(item["arguments"].as_str().unwrap_or("{}")),
        })
        .collect();
    let refused = output
        .iter()
        .filter(|item| item["type"] == "message")
        .flat_map(|item| item["content"].as_array().cloned().unwrap_or_default())
        .any(|part| part["type"] == "refusal");
    let stop = match answer["status"].as_str().unwrap_or_default() {
        _ if refused => Stop::Refused,
        "completed" if !calls.is_empty() => Stop::Calls,
        "completed" => Stop::Done,
        "incomplete" if answer["incomplete_details"]["reason"] == "max_output_tokens" => Stop::Tokens,
        other => Stop::Other(other.to_string()),
    };
    let usage = &answer["usage"];
    Ok(Reply {
        text,
        calls,
        stop,
        usage: Usage {
            input: tokens(&usage["input_tokens"]),
            output: tokens(&usage["output_tokens"]),
        },
        native: Value::Array(output.clone()),
        summary: None,
        model: None,
        effort: None,
        context: None,
    })
}

// -- OpenAI-compatible ----------------------------------------------------------------------------------------------

fn compatible_body(model: &str, request: Request<'_>) -> Value {
    let (summary, turns) = summarized(request.turns);
    compatible_body_from(model, request, summary, turns)
}

/// `request`'s body with `summary`, when there is one, standing in for everything before `turns`.
fn compatible_body_from(model: &str, request: Request<'_>, summary: Option<&str>, turns: &[Turn]) -> Value {
    let mut messages = Vec::new();
    if !request.system.is_empty() {
        messages.push(json!({"role": "system", "content": request.system}));
    }
    for (at, turn) in turns.iter().enumerate() {
        match turn {
            Turn::Asked(said) => {
                // Results answer the calls of the turn before, so they come first, each its own message. Those of
                // turns a summary stands in for went with them.
                if at > 0 || summary.is_none() {
                    for s in said {
                        if let Said::Result { call_id, output, .. } = s {
                            messages.push(json!({"role": "tool", "tool_call_id": call_id, "content": output}));
                        }
                    }
                }
                let summary = summary
                    .filter(|_| at == 0)
                    .map(|s| format!("Summary of the conversation before this:\n{s}"));
                let text: Vec<&str> = summary
                    .as_deref()
                    .into_iter()
                    .chain(said.iter().filter_map(|s| match s {
                        Said::Text(text) => Some(text.as_str()),
                        Said::Result { .. } => None,
                    }))
                    .collect();
                if !text.is_empty() {
                    messages.push(json!({"role": "user", "content": text.join("\n\n")}));
                }
            }
            Turn::Answered(reply) => messages.push(reply.native.clone()),
            Turn::Fresh(_) => {}
        }
    }
    let mut body = json!({"model": model, "messages": messages, "max_tokens": request.max_tokens});
    if let Some(effort) = request.effort {
        body["reasoning_effort"] = json!(effort.as_str());
    }
    if !request.tools.is_empty() {
        body["tools"] = request
            .tools
            .iter()
            .map(|t| {
                json!({"type": "function", "function": {
                    "name": t.name, "description": t.description, "parameters": t.input_schema,
                }})
            })
            .collect();
    }
    body
}

fn compatible_reply(answer: Value) -> Result<Reply, String> {
    let choice = &answer["choices"][0];
    let message = choice["message"]
        .as_object()
        .ok_or("the endpoint answered with no message")?;
    let calls: Vec<Call> = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|c| Call {
            id: c["id"].as_str().unwrap_or_default().to_string(),
            name: c["function"]["name"].as_str().unwrap_or_default().to_string(),
            input: arguments(c["function"]["arguments"].as_str().unwrap_or("{}")),
        })
        .collect();
    let stop = match choice["finish_reason"].as_str().unwrap_or_default() {
        "stop" => Stop::Done,
        "tool_calls" => Stop::Calls,
        "length" => Stop::Tokens,
        "content_filter" => Stop::Refused,
        _ if !calls.is_empty() => Stop::Calls,
        other => Stop::Other(other.to_string()),
    };
    // Sent back as an assistant message, with only what the API takes in one.
    let mut native = json!({"role": "assistant", "content": message.get("content").cloned().unwrap_or(Value::Null)});
    if let Some(tool_calls) = message.get("tool_calls") {
        native["tool_calls"] = tool_calls.clone();
    }
    let usage = &answer["usage"];
    Ok(Reply {
        text: message
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        calls,
        stop,
        usage: Usage {
            input: tokens(&usage["prompt_tokens"]),
            output: tokens(&usage["completion_tokens"]),
        },
        native,
        summary: None,
        model: None,
        effort: None,
        context: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A fake endpoint on loopback answering `answer` at `path`, keeping each request's headers and body.
    async fn serve(
        path: &'static str,
        answer: Vec<Value>,
    ) -> (String, Arc<Mutex<Vec<(axum::http::HeaderMap, Value)>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let answers = Arc::new(Mutex::new(answer.into_iter()));
        let kept = seen.clone();
        let app = axum::Router::new().route(
            path,
            axum::routing::post(
                move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                    let (kept, answers) = (kept.clone(), answers.clone());
                    async move {
                        kept.lock().unwrap().push((headers, body));
                        let next = answers.lock().unwrap().next().expect("an answer for each request");
                        match next.get("status_code").and_then(Value::as_u64) {
                            Some(code) => (
                                axum::http::StatusCode::from_u16(code as u16).unwrap(),
                                axum::Json(next["body"].clone()),
                            ),
                            None => (axum::http::StatusCode::OK, axum::Json(next)),
                        }
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, seen)
    }

    fn endpoint(provider: Provider, url: &str) -> Endpoint {
        Endpoint {
            provider,
            url: url.to_string(),
            model: "m-1".to_string(),
            api_key: "k-1".to_string(),
            context: DEFAULT_CONTEXT,
        }
    }

    fn tools() -> Vec<Tool> {
        vec![Tool {
            name: "list_pipelines".to_string(),
            description: "The pipelines in an area.".to_string(),
            input_schema: json!({"type": "object", "properties": {"area": {"type": "string"}}}),
        }]
    }

    /// Asks, takes the call the model made, and sends its result back with the answer it came in.
    async fn round_trip(model: &dyn Model) -> (Reply, Reply) {
        let tools = tools();
        let mut turns = vec![Turn::Asked(vec![Said::Text("What runs in shared?".to_string())])];
        let first = model
            .ask(Request {
                system: "Be brief.",
                turns: &turns,
                tools: &tools,
                max_tokens: 1000,
                think: true,
                compact: true,
                effort: None,
                cache_key: None,
                person: None,
                heard: None,
            })
            .await
            .unwrap();
        assert_eq!(first.stop, Stop::Calls);
        let call = first.calls[0].clone();
        assert_eq!(call.name, "list_pipelines");
        assert_eq!(call.input, json!({"area": "shared"}));
        turns.push(Turn::Answered(first.clone()));
        turns.push(Turn::Asked(vec![Said::Result {
            call_id: call.id,
            output: "orders".to_string(),
            is_error: false,
        }]));
        let second = model
            .ask(Request {
                system: "Be brief.",
                turns: &turns,
                tools: &tools,
                max_tokens: 1000,
                think: true,
                compact: true,
                effort: None,
                cache_key: None,
                person: None,
                heard: None,
            })
            .await
            .unwrap();
        (first, second)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_calls_a_tool_and_keeps_its_thinking() {
        let thinking = json!({"type": "thinking", "thinking": "", "signature": "sig-1"});
        let (base, seen) = serve(
            "/v1/messages",
            vec![
                json!({
                    "content": [thinking, {"type": "tool_use", "id": "tu_1", "name": "list_pipelines", "input": {"area": "shared"}}],
                    "stop_reason": "tool_use",
                    "usage": {"input_tokens": 10, "cache_read_input_tokens": 5, "output_tokens": 7},
                }),
                json!({
                    "content": [{"type": "text", "text": "orders runs in shared."}],
                    "stop_reason": "end_turn",
                    "usage": {"input_tokens": 20, "output_tokens": 4},
                }),
            ],
        )
        .await;
        let model = model(endpoint(Provider::Anthropic, &format!("{base}/"))).unwrap();
        let (first, second) = round_trip(model.as_ref()).await;
        assert_eq!(first.usage, Usage { input: 15, output: 7 });
        assert_eq!(second.text, "orders runs in shared.");
        assert_eq!(second.stop, Stop::Done);

        let seen = seen.lock().unwrap();
        let (headers, body) = &seen[1];
        assert_eq!(headers["x-api-key"], "k-1");
        assert_eq!(headers["anthropic-version"], ANTHROPIC_VERSION);
        assert_eq!(body["model"], "m-1");
        assert_eq!(headers["anthropic-beta"], ANTHROPIC_COMPACTION);
        assert_eq!(body["system"][0]["text"], "Be brief.");
        assert_eq!(body["system"][0]["cache_control"], json!({"type": "ephemeral"}));
        assert_eq!(body["thinking"], json!({"type": "adaptive"}));
        assert_eq!(body["context_management"]["edits"][0]["type"], "compact_20260112");
        assert_eq!(body["context_management"]["edits"][0]["trigger"]["value"], 150_000);
        assert_eq!(body["cache_control"], json!({"type": "ephemeral"}));
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(body["messages"][1]["content"][0], thinking);
        assert_eq!(body["messages"][2]["content"][0]["tool_use_id"], "tu_1");
        assert_eq!(body["messages"][2]["content"][0]["content"], "orders");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_sends_nothing_before_a_summary() {
        let summary = json!({"type": "compaction", "content": "They asked what runs in shared."});
        let (base, seen) = serve(
            "/v1/messages",
            vec![json!({
                "content": [{"type": "text", "text": "Nothing else."}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 20, "output_tokens": 4},
            })],
        )
        .await;
        let summarized = Reply {
            text: "orders runs in shared.".to_string(),
            calls: Vec::new(),
            stop: Stop::Done,
            usage: Usage::default(),
            native: json!([summary, {"type": "text", "text": "orders runs in shared."}]),
            summary: None,
            model: None,
            effort: None,
            context: None,
        };
        let turns = vec![
            Turn::Asked(vec![Said::Text("What runs in shared?".to_string())]),
            Turn::Answered(summarized),
            Turn::Asked(vec![Said::Text("And else?".to_string())]),
        ];
        let model = model(endpoint(Provider::Anthropic, &base)).unwrap();
        model
            .ask(Request {
                system: "Be brief.",
                turns: &turns,
                tools: &tools(),
                max_tokens: 1000,
                think: true,
                compact: true,
                effort: None,
                cache_key: None,
                person: None,
                heard: None,
            })
            .await
            .unwrap();
        let messages = seen.lock().unwrap()[0].1["messages"].clone();
        assert_eq!(messages.as_array().unwrap().len(), 2);
        assert_eq!(messages[0]["content"][0], summary);
    }

    fn short(turns: &[Turn]) -> Request<'_> {
        Request {
            system: "Be brief.",
            turns,
            tools: &[],
            max_tokens: 1000,
            think: false,
            compact: true,
            effort: None,
            cache_key: None,
            person: None,
            heard: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_compatible_endpoint_carries_a_summary_once_the_conversation_is_long() {
        let answer = |text: &str| {
            json!({
                "choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            })
        };
        let (base, seen) = serve(
            "/v1/chat/completions",
            vec![answer("They asked about shared."), answer("Second."), answer("Third.")],
        )
        .await;
        let model = model(endpoint(Provider::OpenAiCompatible, &format!("{base}/v1"))).unwrap();
        let long = Reply {
            usage: Usage {
                input: compact_at(DEFAULT_CONTEXT),
                output: 10,
            },
            ..compatible_reply(answer("First.")).unwrap()
        };
        let mut turns = vec![
            Turn::Asked(vec![Said::Text("first".to_string())]),
            Turn::Answered(long),
            Turn::Asked(vec![Said::Text("second".to_string())]),
        ];
        let second = model.ask(short(&turns)).await.unwrap();
        assert_eq!(second.text, "Second.");
        assert_eq!(second.summary.as_deref(), Some("They asked about shared."));
        turns.push(Turn::Answered(second));
        turns.push(Turn::Asked(vec![Said::Text("third".to_string())]));
        model.ask(short(&turns)).await.unwrap();

        let seen = seen.lock().unwrap();
        let summarize = &seen[0].1["messages"];
        assert_eq!(summarize.as_array().unwrap().len(), 4);
        assert_eq!(summarize[3]["content"], SUMMARIZE);
        let expected = json!({"role": "user", "content": "Summary of the conversation before this:\nThey asked about shared.\n\nsecond"});
        assert_eq!(seen[1].1["messages"][1], expected);
        assert_eq!(seen[1].1["messages"].as_array().unwrap().len(), 2);
        let third = seen[2].1["messages"].as_array().unwrap();
        assert_eq!(third.len(), 4);
        assert_eq!(third[1], expected);
        assert_eq!(third[3]["content"], "third");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_sends_nothing_before_a_summary() {
        let summary = json!({"type": "compaction", "id": "cmp_1", "encrypted_content": "enc"});
        let (base, seen) = serve(
            "/v1/responses",
            vec![json!({
                "status": "completed",
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Nothing else."}]}],
                "usage": {"input_tokens": 20, "output_tokens": 4},
            })],
        )
        .await;
        let summarized = Reply {
            text: String::new(),
            calls: Vec::new(),
            stop: Stop::Done,
            usage: Usage::default(),
            native: json!([summary]),
            summary: None,
            model: None,
            effort: None,
            context: None,
        };
        let turns = vec![
            Turn::Asked(vec![Said::Text("What runs in shared?".to_string())]),
            Turn::Answered(summarized),
            Turn::Asked(vec![Said::Text("And else?".to_string())]),
        ];
        let model = model(endpoint(Provider::OpenAi, &base)).unwrap();
        model.ask(short(&turns)).await.unwrap();
        let input = seen.lock().unwrap()[0].1["input"].clone();
        assert_eq!(input, json!([summary, {"role": "user", "content": "And else?"}]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_says_how_much_its_model_holds() {
        let app = axum::Router::new().route(
            "/v1/models/:id",
            axum::routing::get(|axum::extract::Path(id): axum::extract::Path<String>| async move {
                axum::Json(json!({"type": "model", "id": id, "max_input_tokens": 1_000_000}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert_eq!(context_of(&endpoint(Provider::Anthropic, &base)).await, Some(1_000_000));
        assert_eq!(context_of(&endpoint(Provider::OpenAi, &base)).await, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_lists_its_models_page_by_page_with_what_each_holds_and_takes() {
        let app = axum::Router::new().route(
            "/v1/models",
            axum::routing::get(
                |axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                    axum::Json(match q.get("after_id").map(String::as_str) {
                        None => json!({
                            "data": [{
                                "id": "claude-opus-5-5", "display_name": "Claude Opus 5.5", "max_input_tokens": 1_000_000,
                                "capabilities": {"effort": {
                                    "supported": true, "low": {"supported": true}, "medium": {"supported": true},
                                    "high": {"supported": true}, "xhigh": {"supported": false}, "max": {"supported": true},
                                }},
                            }],
                            "has_more": true, "last_id": "claude-opus-5-5",
                        }),
                        Some(_) => json!({
                            "data": [{"id": "claude-haiku-4-5", "display_name": "Claude Haiku 4.5",
                                      "capabilities": {"effort": {"supported": false}}}],
                            "has_more": false,
                        }),
                    })
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let listed = models(&endpoint(Provider::Anthropic, &base)).await.unwrap();
        assert_eq!(
            listed,
            vec![
                Listed {
                    id: "claude-opus-5-5".to_string(),
                    name: "Claude Opus 5.5".to_string(),
                    context_tokens: Some(1_000_000),
                    efforts: Some(vec![Effort::Low, Effort::Medium, Effort::High, Effort::Max]),
                },
                Listed {
                    id: "claude-haiku-4-5".to_string(),
                    name: "Claude Haiku 4.5".to_string(),
                    context_tokens: None,
                    efforts: None,
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_lists_its_models_by_id_and_a_compatible_service_under_its_base() {
        let list = || async {
            axum::Json(json!({"object": "list", "data": [{"id": "gpt-5.5", "object": "model", "owned_by": "openai"}]}))
        };
        let app = axum::Router::new()
            .route("/v1/models", axum::routing::get(list))
            .route("/compat/v1/models", axum::routing::get(list));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let gpt = vec![Listed {
            id: "gpt-5.5".to_string(),
            name: "gpt-5.5".to_string(),
            context_tokens: None,
            efforts: None,
        }];
        assert_eq!(models(&endpoint(Provider::OpenAi, &base)).await.unwrap(), gpt);
        let compatible = endpoint(Provider::OpenAiCompatible, &format!("{base}/compat/v1"));
        assert_eq!(models(&compatible).await.unwrap(), gpt);
    }

    /// A fake endpoint on loopback answering each request at `path` with the next server-sent stream of `events`,
    /// keeping each request's body.
    async fn serve_events(path: &'static str, streams: Vec<Vec<Value>>) -> (String, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let streams = Arc::new(Mutex::new(streams.into_iter()));
        let kept = seen.clone();
        let app = axum::Router::new().route(
            path,
            axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
                let (kept, streams) = (kept.clone(), streams.clone());
                async move {
                    kept.lock().unwrap().push(body);
                    let events = streams.lock().unwrap().next().expect("a stream for each request");
                    let text: String = events
                        .iter()
                        .map(|e| match e.as_str() {
                            Some(raw) => format!("data: {raw}\n\n"),
                            None => format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap_or("message")),
                        })
                        .collect();
                    ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], text)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, seen)
    }

    /// `model` asked "Hi" with a tool, the answer it gave, and the text it was heard writing.
    async fn streamed(model: &dyn Model) -> (Result<Reply, String>, String) {
        let heard = Mutex::new(String::new());
        let ear = |text: &str| heard.lock().unwrap().push_str(text);
        let turns = [Turn::Asked(vec![Said::Text("Hi".to_string())])];
        let reply = model
            .ask(Request {
                system: "",
                turns: &turns,
                tools: &tools(),
                max_tokens: 100,
                think: true,
                compact: false,
                effort: None,
                cache_key: None,
                person: None,
                heard: Some(Heard(&ear)),
            })
            .await;
        let heard = heard.lock().unwrap().clone();
        (reply, heard)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn anthropic_streams_its_answer_and_it_is_kept_as_if_sent_whole() {
        let thinking = json!({"type": "thinking", "thinking": "Look first.", "signature": "sig-1"});
        let text = json!({"type": "text", "text": "Let me look."});
        let call = json!({"type": "tool_use", "id": "tu_1", "name": "list_pipelines", "input": {"area": "shared"}});
        let whole = json!({
            "id": "msg_1", "type": "message", "role": "assistant", "model": "m-1",
            "content": [thinking, text, call],
            "stop_reason": "tool_use", "stop_sequence": null,
            "usage": {"input_tokens": 12, "cache_read_input_tokens": 3, "output_tokens": 9},
        });
        let started =
            |block: Value| json!({"type": "content_block_start", "index": block["i"], "content_block": block["b"]});
        let delta = |i: u64, d: Value| json!({"type": "content_block_delta", "index": i, "delta": d});
        let stop = |i: u64| json!({"type": "content_block_stop", "index": i});
        let stream = vec![
            json!({"type": "message_start", "message": {
                "id": "msg_1", "type": "message", "role": "assistant", "model": "m-1", "content": [],
                "stop_reason": null, "stop_sequence": null,
                "usage": {"input_tokens": 12, "cache_read_input_tokens": 3, "output_tokens": 1},
            }}),
            json!({"type": "ping"}),
            started(json!({"i": 0, "b": {"type": "thinking", "thinking": "", "signature": ""}})),
            delta(0, json!({"type": "thinking_delta", "thinking": "Look "})),
            delta(0, json!({"type": "thinking_delta", "thinking": "first."})),
            delta(0, json!({"type": "signature_delta", "signature": "sig-1"})),
            stop(0),
            started(json!({"i": 1, "b": {"type": "text", "text": ""}})),
            delta(1, json!({"type": "text_delta", "text": "Let me "})),
            delta(1, json!({"type": "text_delta", "text": "look."})),
            stop(1),
            started(json!({"i": 2, "b": {"type": "tool_use", "id": "tu_1", "name": "list_pipelines", "input": {}}})),
            delta(2, json!({"type": "input_json_delta", "partial_json": "{\"area\": "})),
            delta(2, json!({"type": "input_json_delta", "partial_json": "\"shared\"}"})),
            stop(2),
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": null}, "usage": {"output_tokens": 9}}),
            json!({"type": "message_stop"}),
        ];
        let (base, seen) = serve_events("/v1/messages", vec![stream.clone(), stream[..8].to_vec()]).await;
        let model = model(endpoint(Provider::Anthropic, &base)).unwrap();
        let (reply, heard) = streamed(model.as_ref()).await;
        let expected = Reply {
            model: Some("m-1".to_string()),
            context: Some(DEFAULT_CONTEXT),
            ..anthropic_reply(whole).unwrap()
        };
        assert_eq!(reply.unwrap(), expected);
        assert_eq!(heard, "Let me look.");
        assert_eq!(seen.lock().unwrap()[0]["stream"], true);
        // A stream cut off part way is no answer.
        let (cut, _) = streamed(model.as_ref()).await;
        assert!(cut.unwrap_err().contains("ended before its answer did"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_streams_its_answer_and_keeps_the_response_it_ends_with() {
        let whole = json!({
            "status": "completed",
            "output": [
                {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "enc"},
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Hello there."}]},
            ],
            "usage": {"input_tokens": 8, "output_tokens": 4},
        });
        let stream = vec![
            json!({"type": "response.created", "response": {"status": "in_progress", "output": []}}),
            json!({"type": "response.output_text.delta", "delta": "Hello "}),
            json!({"type": "response.output_text.delta", "delta": "there."}),
            json!({"type": "response.completed", "response": whole}),
        ];
        let failed = vec![json!({"type": "response.failed", "response": {"error": {"message": "overloaded"}}})];
        let (base, _) = serve_events("/v1/responses", vec![stream, failed]).await;
        let model = model(endpoint(Provider::OpenAi, &base)).unwrap();
        let (reply, heard) = streamed(model.as_ref()).await;
        let expected = Reply {
            model: Some("m-1".to_string()),
            context: Some(DEFAULT_CONTEXT),
            ..openai_reply(whole).unwrap()
        };
        assert_eq!((reply.unwrap(), heard.as_str()), (expected, "Hello there."));
        let (failed, _) = streamed(model.as_ref()).await;
        assert_eq!(failed.unwrap_err(), "the endpoint failed part way: overloaded");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_compatible_service_streams_its_message_and_calls_by_index() {
        let whole = json!({
            "choices": [{"message": {"role": "assistant", "content": "Looking.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "list_pipelines", "arguments": "{\"area\":\"shared\"}"}},
            ]}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 7, "completion_tokens": 5},
        });
        let chunk =
            |delta: Value, finish: Value| json!({"choices": [{"index": 0, "delta": delta, "finish_reason": finish}]});
        let stream = vec![
            chunk(json!({"role": "assistant", "content": "Look"}), Value::Null),
            chunk(json!({"content": "ing."}), Value::Null),
            chunk(
                json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "list_pipelines", "arguments": "{\"area\":"}}]}),
                Value::Null,
            ),
            chunk(
                json!({"tool_calls": [{"index": 0, "function": {"arguments": "\"shared\"}"}}]}),
                json!("tool_calls"),
            ),
            json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 5}}),
            json!("[DONE]"),
        ];
        let (base, seen) = serve_events("/compat/v1/chat/completions", vec![stream]).await;
        let model = model(endpoint(Provider::OpenAiCompatible, &format!("{base}/compat/v1"))).unwrap();
        let (reply, heard) = streamed(model.as_ref()).await;
        let expected = Reply {
            model: Some("m-1".to_string()),
            context: Some(DEFAULT_CONTEXT),
            ..compatible_reply(whole).unwrap()
        };
        assert_eq!((reply.unwrap(), heard.as_str()), (expected, "Looking."));
        assert_eq!(
            seen.lock().unwrap()[0]["stream_options"],
            json!({"include_usage": true})
        );
    }

    #[test]
    fn each_provider_is_asked_for_the_effort_picked_and_openai_for_its_cache_key_and_who_asks() {
        let turns = [Turn::Asked(vec![Said::Text("Hi".to_string())])];
        let request = Request {
            system: "",
            turns: &turns,
            tools: &[],
            max_tokens: 100,
            think: true,
            compact: false,
            effort: Some(Effort::Xhigh),
            cache_key: Some("c_1"),
            person: Some("h_1"),
            heard: None,
        };
        let anthropic = anthropic_body("m", request, compact_at(DEFAULT_CONTEXT));
        assert_eq!(anthropic["output_config"], json!({"effort": "xhigh"}));
        assert!(anthropic.get("prompt_cache_key").is_none());
        let openai = openai_body("m", request, compact_at(DEFAULT_CONTEXT));
        assert_eq!(openai["reasoning"], json!({"effort": "xhigh"}));
        assert_eq!(openai["prompt_cache_key"], "c_1");
        assert_eq!(openai["safety_identifier"], "h_1");
        assert_eq!(compatible_body("m", request)["reasoning_effort"], "xhigh");
        let unpicked = Request {
            effort: None,
            cache_key: None,
            person: None,
            ..request
        };
        assert!(
            anthropic_body("m", unpicked, compact_at(DEFAULT_CONTEXT))
                .get("output_config")
                .is_none()
        );
        let openai = openai_body("m", unpicked, compact_at(DEFAULT_CONTEXT));
        assert!(openai.get("reasoning").is_none() && openai.get("prompt_cache_key").is_none());
        assert!(compatible_body("m", unpicked).get("reasoning_effort").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_request_reads_from_the_newest_fresh_start_its_summary_said_first() {
        let done = json!({
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 3, "output_tokens": 1},
        });
        let (base, seen) = serve("/v1/messages", vec![done.clone(), done.clone()]).await;
        let model = model(endpoint(Provider::Anthropic, &base)).unwrap();
        let said = |text: &str| Turn::Asked(vec![Said::Text(text.to_string())]);
        let fresh = |summary: Option<&str>| {
            Turn::Fresh(Fresh {
                summary: summary.map(str::to_string),
                tokens: 5,
            })
        };
        let before = vec![
            said("Hi"),
            Turn::Answered(anthropic_reply(done.clone()).unwrap()),
            fresh(Some("old")),
            said("Then"),
            Turn::Answered(anthropic_reply(done).unwrap()),
        ];
        for turns in [
            [before.clone(), vec![fresh(Some("S")), said("Next")]].concat(),
            [before, vec![fresh(None), said("Next")]].concat(),
        ] {
            model
                .ask(Request {
                    system: "",
                    turns: &turns,
                    tools: &[],
                    max_tokens: 100,
                    think: false,
                    compact: false,
                    effort: None,
                    cache_key: None,
                    person: None,
                    heard: None,
                })
                .await
                .unwrap();
        }
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen[0].1["messages"],
            json!([{"role": "user", "content": [
                {"type": "text", "text": "Summary of the conversation before this:\nS"},
                {"type": "text", "text": "Next"},
            ]}])
        );
        assert_eq!(
            seen[1].1["messages"],
            json!([{"role": "user", "content": [{"type": "text", "text": "Next"}]}])
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_model_that_refuses_compaction_is_asked_again_without() {
        let (base, seen) = serve(
            "/v1/messages",
            vec![
                json!({"status_code": 400, "body": {"type": "error", "error": {"message": "context_management: compact_20260112 is not supported on this model"}}}),
                json!({
                    "content": [{"type": "text", "text": "ok"}],
                    "stop_reason": "end_turn",
                    "usage": {"input_tokens": 3, "output_tokens": 1},
                }),
            ],
        )
        .await;
        let model = model(endpoint(Provider::Anthropic, &base)).unwrap();
        let turns = [Turn::Asked(vec![Said::Text("Hi".to_string())])];
        let reply = model
            .ask(Request {
                system: "",
                turns: &turns,
                tools: &[],
                max_tokens: 100,
                think: false,
                compact: true,
                effort: None,
                cache_key: None,
                person: None,
                heard: None,
            })
            .await
            .unwrap();
        assert_eq!(reply.text, "ok");
        // The answer says what made it, for the person's next message to start from.
        assert_eq!(
            (reply.model.as_deref(), reply.context),
            (Some("m-1"), Some(DEFAULT_CONTEXT))
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].1["context_management"]["edits"][0]["type"], "compact_20260112");
        assert!(seen[1].1.get("context_management").is_none());
        assert!(seen[1].0.get("anthropic-beta").is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn openai_calls_a_tool_and_keeps_its_reasoning() {
        let reasoning = json!({"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "enc"});
        let (base, seen) = serve(
            "/v1/responses",
            vec![
                json!({
                    "status": "completed",
                    "output": [reasoning, {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "list_pipelines", "arguments": "{\"area\":\"shared\"}"}],
                    "usage": {"input_tokens": 12, "output_tokens": 9},
                }),
                json!({
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "orders runs in shared."}]}],
                    "usage": {"input_tokens": 30, "output_tokens": 5},
                }),
            ],
        )
        .await;
        let model = model(endpoint(Provider::OpenAi, &base)).unwrap();
        let (first, second) = round_trip(model.as_ref()).await;
        assert_eq!(first.usage, Usage { input: 12, output: 9 });
        assert_eq!(second.text, "orders runs in shared.");
        assert_eq!(second.stop, Stop::Done);

        let seen = seen.lock().unwrap();
        let (headers, body) = &seen[1];
        assert_eq!(headers["authorization"], "Bearer k-1");
        assert_eq!(body["store"], false);
        assert_eq!(body["instructions"], "Be brief.");
        assert_eq!(body["context_management"][0]["type"], "compaction");
        assert_eq!(body["tools"][0]["parameters"]["type"], "object");
        assert_eq!(body["input"][1], reasoning);
        assert_eq!(
            body["input"][3],
            json!({"type": "function_call_output", "call_id": "call_1", "output": "orders"})
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_compatible_endpoint_calls_a_tool() {
        let (base, seen) = serve(
            "/v1/chat/completions",
            vec![
                json!({
                    "choices": [{"finish_reason": "tool_calls", "message": {"role": "assistant", "content": null, "tool_calls": [
                        {"id": "call_1", "type": "function", "function": {"name": "list_pipelines", "arguments": "{\"area\":\"shared\"}"}},
                    ]}}],
                    "usage": {"prompt_tokens": 11, "completion_tokens": 6},
                }),
                json!({
                    "choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "orders runs in shared."}}],
                    "usage": {"prompt_tokens": 25, "completion_tokens": 5},
                }),
            ],
        )
        .await;
        let model = model(endpoint(Provider::OpenAiCompatible, &format!("{base}/v1"))).unwrap();
        let (first, second) = round_trip(model.as_ref()).await;
        assert_eq!(first.usage, Usage { input: 11, output: 6 });
        assert_eq!(second.text, "orders runs in shared.");

        let seen = seen.lock().unwrap();
        let (headers, body) = &seen[1];
        assert_eq!(headers["authorization"], "Bearer k-1");
        assert_eq!(body["messages"][0], json!({"role": "system", "content": "Be brief."}));
        assert_eq!(body["messages"][2]["tool_calls"][0]["id"], "call_1");
        assert_eq!(
            body["messages"][3],
            json!({"role": "tool", "tool_call_id": "call_1", "content": "orders"})
        );
        assert_eq!(body["tools"][0]["function"]["name"], "list_pipelines");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_key_says_what_the_provider_said() {
        let (base, _) = serve(
            "/v1/messages",
            vec![json!({"status_code": 401, "body": {"type": "error", "error": {"type": "authentication_error", "message": "invalid x-api-key"}}})],
        )
        .await;
        let error = check(endpoint(Provider::Anthropic, &base)).await.unwrap_err();
        assert!(error.ends_with("answered 401: invalid x-api-key"), "{error}");
    }

    #[test]
    fn an_endpoint_is_https_unless_it_is_on_this_host() {
        assert!(check_endpoint("https://api.anthropic.com").is_ok());
        assert!(check_endpoint("http://127.0.0.1:8080/v1").is_ok());
        assert!(check_endpoint("http://models.example.com").is_err());
        assert!(check_endpoint("https://api.openai.com?x=1").is_err());
    }

    #[test]
    fn only_known_providers_parse() {
        assert_eq!(Provider::parse("openai"), Some(Provider::OpenAi));
        assert_eq!(Provider::parse("gemini"), None);
    }
}
