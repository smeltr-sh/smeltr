//! Starting a capture, watching it fill, and saving what it caught.
//!
//! The leader opens no connection here. A capture is an invitation every worker picks up on its next heartbeat
//! (`leader::capture`), and this is the operator's side of it: ask, poll, then save the records as a sample like any
//! other (`config_file::sample`), which is where capture and paste meet.

use axum::Json;
use axum::extract::{Extension, Path as AxPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use smeltr::config_file;
use smeltr::leader::access::{Access, Level};
use smeltr::leader::capture::{CaptureRequest, CaptureStatus, Captures};
use smeltr::leader::envelope::Envelope;
use smeltr::leader::store::Actor;
use smeltr::leader::{AppState, blocking};
use smeltr::worker::capture::UNCLAIMED;

use crate::config_api::{self, BranchQuery};
use crate::gate::refuse;

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartedCapture {
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<String>,
    /// What the bounds actually became, which a caller who asked past a ceiling needs to see.
    #[serde(skip_serializing_if = "Option::is_none")]
    max_matches: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_secs: Option<u64>,
}

/// Open a capture of a source or at a destination. Answers immediately with its id: workers pick the invitation up
/// on their own heartbeats, so nothing here waits for one.
///
/// The source is validated exactly as saving one is, and a filter that will not compile is refused now rather than
/// by every worker separately -- the author is here to read the error, and the workers are not.
///
/// The gate let in anyone who edits somewhere; what is captured is the group's records, so it needs editor there.
pub async fn start_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Json(request): Json<CaptureRequest>,
) -> Response {
    if !access.allows(&request.group, Level::Editor) {
        return refuse(
            StatusCode::FORBIDDEN,
            &format!("this needs {} in {}", Level::Editor.as_str(), request.group),
        );
    }
    open_capture(state, request).await.into_response()
}

async fn open_capture(state: AppState, request: CaptureRequest) -> Json<Envelope<StartedCapture>> {
    if !takes_its_start(&request) {
        return Json(Envelope::failed_one(
            "only a Kafka or Event Hubs source's capture takes a start",
        ));
    }
    let source = match (&request.source, &request.destination) {
        (Some(source), None) => source.clone(),
        (None, Some(destination)) => {
            let destination = destination.clone();
            return Json(start_destination_capture(&state, request, &destination));
        }
        _ => return Json(Envelope::failed_one("a capture names one source or one destination")),
    };
    // The same check a save runs (`config_file::validate`), because this source travels to workers exactly as a
    // saved one does. Without it a credential written as a literal -- which `validate_kafka_props` refuses on the
    // save path -- would be handed to every worker in the group over the wire.
    let one_source = config_file::FileConfig {
        version: config_file::CURRENT_VERSION,
        sources: vec![source.clone()],
        destinations: Vec::new(),
        pipelines: Vec::new(),
        routes: Vec::new(),
        lookup_tables: Vec::new(),
        grok_patterns: Vec::new(),
        // Without it the source names an identity nothing declares, and the key that identity names is never
        // resolved for the workers to receive.
        identities: request.identity.clone().into_iter().collect(),
        on_exception: None,
        name: None,
    };
    let mut invalid = config_file::validation_errors(one_source.clone());
    // Run reads an SQL source's query before its author has declared fields or chosen a mode: the worker declares
    // every column the query returns and reads every poll (`worker::capture::run_sql_capture`).
    if source.kind.undeclared() {
        invalid.retain(|e| e.field != Some("fields"));
    }
    if !invalid.is_empty() {
        return Json(Envelope::failed_one(
            invalid.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "),
        ));
    }

    // A source whose record is known only once the worker describes its query has its filter compiled there; only
    // its syntax can be checked here.
    if let Some(filter) = request.filter.as_deref() {
        let compiled = if source.kind.undeclared() {
            smeltr::plan::plan::parse(filter, None)
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            smeltr::worker::capture::Filter::compile(Some(filter), source.kind.meta(), &source.id).map(|_| ())
        };
        if let Err(e) = compiled {
            return Json(Envelope::failed_one(e));
        }
    }

    let (expected, holding) = waits_for(&state, &request.group, Some(&source.id), None);
    // Named by content of its own: two captures opened in the same second must not share an id, and a worker keys
    // its sessions by this.
    let session = format!("{:016x}", rand_session());

    // The credentials this source itself names, and no others: a capture is entitled to what it asks for, where a
    // deployment is entitled to what its bundle names (design doc, Secrets boundary). Read once here rather than on
    // every heartbeat that hands the invitation on, and recorded, because this is the one way a value reaches a
    // worker without a commit on `main` naming it -- the trail the deploy path leaves for free.
    let wanted = leader_stored_refs(&one_source);
    let granted = if wanted.is_empty() {
        Vec::new()
    } else {
        let store = state.store.clone();
        let asked = wanted.clone();
        match blocking(move || store.secrets_for_delivery(&asked, &[])).await {
            Ok(v) => v,
            Err(e) => return Json(Envelope::failed_one(e)),
        }
    };
    if !granted.is_empty() {
        let named: Vec<String> = granted.iter().map(|d| format!("{} v{}", d.name, d.version)).collect();
        tracing::info!(
            target: "capture.secrets_granted",
            subject = %session,
            group = %request.group,
            secrets = %named.join(", "),
            "a capture carries credentials to its workers"
        );
    }
    let invitation = state
        .captures
        .open(session.clone(), request, expected, holding, granted);
    Json(Envelope::ok(StartedCapture {
        session: Some(session),
        max_matches: Some(invitation.max_matches),
        max_bytes: Some(invitation.max_bytes),
        deadline_secs: Some(invitation.deadline_secs),
    }))
}

/// Open a capture at a deployed destination. It carries no configuration and no credentials: a worker reads only
/// what its own deployment sends there, and one that sends nothing there says so. Its filter reads each row as its
/// pipeline finished it.
fn start_destination_capture(state: &AppState, request: CaptureRequest, destination: &str) -> Envelope<StartedCapture> {
    if destination.is_empty() {
        return Envelope::failed_one("a capture at a destination names it");
    }
    if request.identity.is_some() {
        return Envelope::failed_one("a capture at a destination reads what is sent there, under no identity");
    }
    // Only its syntax can be checked here: what it reads is each route's output, which the workers' deployment
    // compiles. A filter a route's output does not resolve ends the capture there, saying which route.
    if let Some(filter) = request.filter.as_deref()
        && let Err(e) = smeltr::plan::plan::parse(filter, None)
    {
        return Envelope::failed_one(e.to_string());
    }
    let (expected, holding) = waits_for(state, &request.group, None, Some(destination));
    let session = format!("{:016x}", rand_session());
    let invitation = state
        .captures
        .open(session.clone(), request, expected, holding, Vec::new());
    Envelope::ok(StartedCapture {
        session: Some(session),
        max_matches: Some(invitation.max_matches),
        max_bytes: Some(invitation.max_bytes),
        deadline_secs: Some(invitation.deadline_secs),
    })
}

/// Every leader-stored credential one source names, as the store addresses them. A worker-local one is a variable
/// on the worker's own host and is nobody's to deliver.
fn leader_stored_refs(one_source: &config_file::FileConfig) -> Vec<(String, i64)> {
    use smeltr::plan::topology::SecretRef;
    let Ok(json) = serde_json::to_string(one_source) else {
        return Vec::new();
    };
    let references: Vec<SecretRef> = match config_file::load_str(&json) {
        Ok(system) => system.topology.named_secrets.into_iter().map(|s| s.reference).collect(),
        // An SQL source that declares no fields yet does not load, though Run reads it, so its credentials are read
        // from its connection.
        Err(_) => match one_source
            .sources
            .first()
            .and_then(|s| config_file::sql_connection(s, one_source.identities.first()))
        {
            Some(database) => database
                .password()
                .cloned()
                .into_iter()
                .chain(database.identity().and_then(|i| i.certificate.clone()))
                .collect(),
            None => Vec::new(),
        },
    };
    let mut out: Vec<(String, i64)> = references
        .into_iter()
        .filter_map(|reference| match reference {
            SecretRef::LeaderStored { name, version } => Some((name, version)),
            SecretRef::WorkerLocal(_) => None,
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Enough entropy that two captures never collide, without a dependency: a hasher keyed by `RandomState`, which the
/// OS seeds, over the clock and a counter that never repeats within the process.
fn rand_session() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    let mut hasher = RandomState::new().build_hasher();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    NEXT.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    hasher.finish()
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Followed {
    #[serde(skip_serializing_if = "Option::is_none")]
    capture: Option<CaptureStatus>,
}

pub async fn capture_status(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Json<Envelope<Followed>> {
    Json(status_of(&state.captures, &access, &session))
}

fn status_of(captures: &Captures, access: &Access, session: &str) -> Envelope<Followed> {
    let capture = followed(captures, access, session).then(|| captures.status(session));
    Followed::found(capture.flatten())
}

/// Whether `access` may follow `session`: watch, stop, discard or save it. The gate let in anyone who edits
/// somewhere; a capture holds its group's records, so it needs editor there. A capture in a group the caller does
/// not edit answers as an unknown id does, so the ids of other areas' captures stay invisible.
fn followed(captures: &Captures, access: &Access, session: &str) -> bool {
    captures
        .group(session)
        .is_some_and(|group| access.allows(&group, Level::Editor))
}

impl Followed {
    fn found(capture: Option<CaptureStatus>) -> Envelope<Followed> {
        match capture {
            Some(capture) => Envelope::ok(Followed { capture: Some(capture) }),
            None => Envelope::failed_one("no capture by that id -- it may have been saved, or swept after finishing"),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveCaptureRequest {
    /// The sample's id, minted by the GUI so a rename keeps it. Absent from a client that predates it, which gets
    /// the name as the id.
    #[serde(default)]
    pub id: Option<String>,
    /// What to call the sample.
    pub name: String,
    /// The source these records stand for, so the sample can be route-previewed. A capture at a source names one; a
    /// capture at a destination holds what pipelines finished, which no route takes in, so it names none.
    #[serde(default)]
    pub source: Option<String>,
}

impl SaveCaptureRequest {
    fn sample_id(&self) -> String {
        self.id.clone().unwrap_or_else(|| self.name.clone())
    }
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedCapture {
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    records: Option<usize>,
}

/// How recently a worker must have heartbeated to be one a capture waits for. Several heartbeat intervals, so an
/// ordinary missed beat does not drop a live worker out of the set.
const EXPECTED_WORKER_SEEN_SECS: u64 = 15;

/// Who a capture waits for: the workers in `group` the leader has heard from, and those of them holding rows at its
/// source or destination, whose held rows come first. Fixed when it opens, so a capture cannot declare itself
/// finished before a worker that was always going to answer has even been asked.
fn waits_for(
    state: &AppState,
    group: &str,
    source: Option<&str>,
    destination: Option<&str>,
) -> (std::collections::HashSet<String>, std::collections::HashSet<String>) {
    let within = std::time::Duration::from_secs(EXPECTED_WORKER_SEEN_SECS);
    let expected = state.registry.workers_in(group, within);
    let holding = smeltr::leader::warnings::holding_for_capture(
        &state.registry.snapshot(),
        group,
        &expected,
        within,
        source,
        destination,
    );
    (expected, holding)
}

/// A batch's rows as a sample stores them: each column in the record by its path, `__source` and `__destination`
/// columns included, and the keys of an open record's unclaimed column (`UNCLAIMED`) as they were sent. A null is a
/// field the row did not have. A failed value is its original text, and the record lists its field under
/// `io::MISTYPED_FIELDS` (`io::take_mistyped`), so a preview of the sample fails it as the pipeline did.
fn sample_records(batch: &smeltr::heartbeat::CaptureBatch) -> impl Iterator<Item = smeltr::io::RecordObject> + '_ {
    batch.rows.iter().map(|row| {
        let mut record = smeltr::io::RecordObject::new();
        for (column, value) in batch.columns.iter().zip(&row.values) {
            if value.is_null() {
                continue;
            }
            match (column.name.as_str(), value) {
                (UNCLAIMED, serde_json::Value::Object(keys)) => record.extend(keys.clone()),
                _ => put_path(&mut record, &column.name, value.clone()),
            }
        }
        if !row.failed.is_empty() {
            let names = row
                .failed
                .iter()
                .map(|&c| batch.columns[c].name.clone().into())
                .collect();
            put_path(
                &mut record,
                smeltr::io::MISTYPED_FIELDS,
                serde_json::Value::Array(names),
            );
        }
        record
    })
}

/// The one set of columns a sample of `batches` holds: each column of every batch, in the order first seen, with
/// `UNCLAIMED` in place of each key its rows hold there, a variant, as its record holds it. Batches differ where
/// workers run different builds, or where a destination is fed by several routes; a name they type differently is a
/// variant, which holds either.
fn sample_columns(batches: &[smeltr::heartbeat::CaptureBatch]) -> Vec<smeltr::heartbeat::CaptureColumn> {
    let unclaimed = |batch: &smeltr::heartbeat::CaptureBatch, at: usize| {
        let mut keys: Vec<smeltr::heartbeat::CaptureColumn> = Vec::new();
        for row in &batch.rows {
            if let Some(serde_json::Value::Object(object)) = row.values.get(at) {
                for key in object.keys().map(|k| smeltr::plan::tree::spell(k).into_owned()) {
                    if !keys.iter().any(|c| c.name == key) {
                        keys.push(smeltr::heartbeat::CaptureColumn {
                            name: key,
                            ty: "variant".to_string(),
                        });
                    }
                }
            }
        }
        keys
    };
    let each = batches.iter().flat_map(|b| {
        b.columns
            .iter()
            .enumerate()
            .flat_map(move |(at, c)| match c.name == UNCLAIMED {
                true => unclaimed(b, at),
                false => vec![c.clone()],
            })
    });
    let mut columns: Vec<smeltr::heartbeat::CaptureColumn> = Vec::new();
    for column in each {
        match columns.iter_mut().find(|c| c.name == column.name) {
            None => columns.push(column),
            Some(seen) if seen.ty != column.ty => seen.ty = "variant".to_string(),
            Some(_) => {}
        }
    }
    columns
}

/// Set `value` at `path`, a record field's name, in `object`: under each of its keys in turn (`plan::tree::segments`),
/// as `io::value_at` reads it back.
fn put_path(object: &mut serde_json::Map<String, serde_json::Value>, path: &str, value: serde_json::Value) {
    let keys: Vec<_> = smeltr::plan::tree::segments(path).collect();
    let Some((last, parents)) = keys.split_last() else {
        return;
    };
    let mut at = object;
    for key in parents {
        let inner = at
            .entry(key.as_ref())
            .or_insert_with(|| serde_json::Value::Object(Default::default()));
        let serde_json::Value::Object(inner) = inner else {
            return;
        };
        at = inner;
    }
    at.insert(last.to_string(), value);
}

/// The capture `session` as a save would commit it, or why it cannot be saved.
fn savable(captures: &Captures, access: &Access, session: &str) -> Result<CaptureStatus, String> {
    let status = followed(captures, access, session)
        .then(|| captures.status(session))
        .flatten()
        .ok_or("no capture by that id -- it may already have been saved")?;
    if !status.done {
        let waiting = if status.waiting_on.is_empty() {
            String::new()
        } else {
            format!(" -- waiting on {}", status.waiting_on.join(", "))
        };
        return Err(format!(
            "this capture is still running{waiting}. Stop it first to save what it holds"
        ));
    }
    if status.batches.iter().all(|b| b.rows.is_empty()) {
        return Err(
            "this capture caught nothing, and a sample of no records would preview as an empty pipeline \
             rather than as the problem it is"
                .to_string(),
        );
    }
    Ok(status)
}

/// Save a finished capture as a sample, and forget the session.
///
/// This is the seam the whole design turns on: a capture ends at a sample file, exactly as a paste does, and nothing
/// downstream can tell which way the records arrived. Saving is a commit on a branch like any other save.
pub async fn save_capture(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
    Query(q): Query<BranchQuery>,
    headers: HeaderMap,
    Json(request): Json<SaveCaptureRequest>,
) -> Json<Envelope<SavedCapture>> {
    let status = match savable(&state.captures, &access, &session) {
        Ok(status) => status,
        Err(e) => return Json(Envelope::failed_one(e)),
    };

    let source = match (&status.destination, request.source.clone()) {
        (Some(_), _) => None,
        (None, Some(source)) => Some(source),
        (None, None) => {
            return Json(Envelope::failed_one(
                "name the source these records stand for, so the sample can be route-previewed",
            ));
        }
    };
    let columns = sample_columns(&status.batches);
    let records = status.batches.iter().flat_map(sample_records).collect();
    let sample = config_file::SampleDto {
        id: request.sample_id(),
        name: request.name,
        source,
        columns,
        records,
    };
    let records = sample.records.len();

    let (branch, base, scope) = (
        q.name().to_string(),
        config_api::base_commit(&headers),
        q.scope().to_string(),
    );
    let saved = blocking(move || {
        let path = config_file::sample_path(&sample.id).map_err(|e| e.to_string())?;
        let writing = config_api::Writing::open(&branch, base.as_deref(), &scope)?;
        let content = Some(config_file::sample_bytes(&sample));
        config_api::refuse_sample_collision(&mut writing.files(), &path, &sample.id, &content)?;
        writing.commit(
            vec![smeltr::leader::git::FileEdit { path, content }],
            &format!("save capture as sample {}", sample.name),
            &actor,
        )
    })
    .await;

    match saved {
        Ok(commit) => {
            // Only once it is durable: a session dropped before the commit landed would lose the records with no
            // way to ask for them again.
            state.captures.close(&session);
            Json(Envelope::ok(SavedCapture {
                commit: Some(commit),
                records: Some(records),
            }))
        }
        Err(e) => Json(Envelope::failed_one(e)),
    }
}

/// End a capture still running, keeping what it holds for a save or a discard. Records that land after are dropped,
/// and every worker still running it lets go once its next heartbeat's answer no longer lists it -- a supervisor's
/// own capture on that beat, a runtime's on the deployer's next turn. A capture already over is answered as it ended.
pub async fn stop_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Json<Envelope<Followed>> {
    Json(stop(&state.captures, &access, &session))
}

fn stop(captures: &Captures, access: &Access, session: &str) -> Envelope<Followed> {
    let capture = followed(captures, access, session).then(|| captures.stop(session));
    Followed::found(capture.flatten())
}

/// Give up on a capture without saving it.
pub async fn discard_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": discard(&state.captures, &access, &session) }))
}

/// Whether `session` was found and forgotten.
fn discard(captures: &Captures, access: &Access, session: &str) -> bool {
    followed(captures, access, session) && captures.close(session).is_some()
}

/// Whether whatever `request` captures has a position to start from, where it names a start. Anywhere else a start
/// would be read by nothing.
fn takes_its_start(request: &CaptureRequest) -> bool {
    request.start.is_none()
        || request.source.as_ref().is_some_and(|s| {
            matches!(
                s.kind,
                config_file::SourceKindDto::Kafka { .. } | config_file::SourceKindDto::EventHubs { .. }
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A start is a stream's: a capture of any other source, or at a destination, is refused one.
    #[test]
    fn only_a_stream_source_s_capture_takes_a_start() {
        let ask = |json: serde_json::Value| -> CaptureRequest { serde_json::from_value(json).unwrap() };
        let kafka = serde_json::json!({"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g"});
        let metrics = serde_json::json!({"id":"s","type":"metrics","bucketSeconds":5});
        let earliest = serde_json::json!({"type":"earliest"});

        assert!(takes_its_start(&ask(
            serde_json::json!({"group":"g","source":kafka,"start":earliest})
        )));
        assert!(!takes_its_start(&ask(
            serde_json::json!({"group":"g","source":metrics,"start":earliest})
        )));
        assert!(takes_its_start(&ask(serde_json::json!({"group":"g","source":metrics}))));
        assert!(!takes_its_start(&ask(
            serde_json::json!({"group":"g","destination":"es","start":earliest})
        )));
    }

    /// The mechanism a leaked allocation address stood in for: many calls in a tight loop must not repeat.
    #[test]
    fn rand_session_does_not_repeat_across_many_calls() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            assert!(seen.insert(rand_session()), "a session id repeated");
        }
    }

    /// The id the GUI minted names the sample, so a rename keeps it; a client that sends none gets the name.
    #[test]
    fn a_save_takes_the_id_it_is_sent_and_falls_back_to_the_name() {
        let minted: SaveCaptureRequest =
            serde_json::from_str(r#"{"id":"sample-4k2p9x1a","name":"orders, ordinary hour","source":"orders"}"#)
                .unwrap();
        assert_eq!(minted.sample_id(), "sample-4k2p9x1a");

        let old: SaveCaptureRequest =
            serde_json::from_str(r#"{"name":"orders, ordinary hour","source":"orders"}"#).unwrap();
        assert_eq!(old.sample_id(), "orders, ordinary hour");
    }

    /// A capture is entitled to what its own source names, where a deployment is entitled to what its bundle names.
    /// A worker-local reference is a variable on the worker's own host and is nobody's to resolve or deliver.
    #[test]
    fn a_capture_asks_only_for_the_credentials_its_own_source_names() {
        let one = |json: &str| -> config_file::FileConfig {
            config_file::FileConfig {
                version: config_file::CURRENT_VERSION,
                sources: vec![serde_json::from_str(json).unwrap()],
                destinations: Vec::new(),
                pipelines: Vec::new(),
                routes: Vec::new(),
                lookup_tables: Vec::new(),
                grok_patterns: Vec::new(),
                identities: Vec::new(),
                on_exception: None,
                name: None,
            }
        };

        let stored = one(
            r#"{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
                "properties":{"security.protocol":"SASL_SSL"},"credentials":{"sasl.password":{"secret":"kafka-cred","version":4}}}"#,
        );
        assert_eq!(
            leader_stored_refs(&stored),
            vec![("kafka-cred".to_string(), 4)],
            "the one it quotes, at the version it quotes"
        );

        let local = one(
            r#"{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
                "properties":{"security.protocol":"SASL_SSL"},"credentials":{"sasl.password":"KAFKA_PASSWORD"}}"#,
        );
        assert!(
            leader_stored_refs(&local).is_empty(),
            "a worker-local variable is the host's, and the leader has no value to send"
        );

        let none = one(r#"{"id":"s","type":"metrics","bucketSeconds":5}"#);
        assert!(leader_stored_refs(&none).is_empty());

        let fieldless = one(r#"{"id":"s","type":"postgres","host":"db","database":"d","user":"u",
                "password":{"secret":"pg-cred","version":2},"query":"SELECT 1","mode":{"type":"everyPoll"},"fields":[]}"#);
        assert_eq!(
            leader_stored_refs(&fieldless),
            vec![("pg-cred".to_string(), 2)],
            "Run reads an SQL source before it declares fields, with its own password"
        );
    }

    /// A source batch saves as the sample records a paste would make: its fields by path in the record, its
    /// `__source` columns under its `__source`, a field the row did not have left out, and a failed value as its
    /// original text, its field listed under `io::MISTYPED_FIELDS`.
    #[test]
    fn a_source_batch_saves_as_sample_records() {
        use smeltr::heartbeat::{CaptureBatch, CaptureColumn, CapturedRow};
        let column = |name: &str, ty: &str| CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let batch = CaptureBatch {
            columns: vec![
                column("user.name", "str"),
                column(r#"labels["a.b"]"#, "i64"),
                column("id", "i64"),
                column("__source.offset", "i64"),
            ],
            rows: vec![CapturedRow {
                values: vec![
                    serde_json::json!("ann"),
                    serde_json::Value::Null,
                    serde_json::json!("true"),
                    serde_json::json!(7),
                ],
                failed: vec![2],
                worker: Some("w1".to_string()),
            }],
        };
        let saved: Vec<_> = sample_records(&batch).collect();
        assert_eq!(
            serde_json::to_value(&saved).unwrap(),
            serde_json::json!([{"user": {"name": "ann"}, "id": "true", "__source": {"offset": 7},
                "smeltr": {"mistyped": ["id"]}}])
        );
        let mut one = batch.clone();
        one.rows[0].values[1] = serde_json::json!(2);
        let saved = sample_records(&one).next().unwrap();
        assert_eq!(
            saved["labels"],
            serde_json::json!({"a.b": 2}),
            "a quoted key is one key"
        );
    }

    /// A destination batch saves as one object per row: its fields, its `__source` and `__destination` columns, and an
    /// open record's unclaimed keys back in it, as it was sent.
    #[test]
    fn a_destination_batch_saves_its_placement_in_the_record() {
        use smeltr::heartbeat::{CaptureBatch, CaptureColumn, CapturedRow};
        let column = |name: &str, ty: &str| CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        };
        let batch = CaptureBatch {
            columns: vec![
                column("n", "i64"),
                column(UNCLAIMED, "{...}"),
                column("__source.topic", "str"),
                column("__destination.route", "str"),
            ],
            rows: vec![CapturedRow {
                values: vec![
                    serde_json::json!(2),
                    serde_json::json!({"extra": "x"}),
                    serde_json::json!("t"),
                    serde_json::json!("orders"),
                ],
                failed: Vec::new(),
                worker: None,
            }],
        };
        let saved: Vec<_> = sample_records(&batch).collect();
        assert_eq!(
            serde_json::to_value(&saved).unwrap(),
            serde_json::json!([{"n": 2, "extra": "x", "__source": {"topic": "t"},
                "__destination": {"route": "orders"}}])
        );
        assert_eq!(
            sample_columns(&[batch])
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["n", "extra", "__source.topic", "__destination.route"],
            "each unclaimed key is a column of its own"
        );
    }

    /// Batches of several routes save as one set of columns, unless they type one name two ways.
    #[test]
    fn a_sample_holds_every_batch_s_columns_once() {
        use smeltr::heartbeat::{CaptureBatch, CaptureColumn};
        let batch = |columns: &[(&str, &str)]| {
            CaptureBatch::new(
                columns
                    .iter()
                    .map(|(name, ty)| CaptureColumn {
                        name: name.to_string(),
                        ty: ty.to_string(),
                    })
                    .collect(),
            )
        };
        let columns = sample_columns(&[
            batch(&[("a", "i64"), ("b", "str")]),
            batch(&[("a", "i64"), ("c", "f64")]),
        ]);
        assert_eq!(
            columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        let typed_twice = sample_columns(&[batch(&[("a", "i64")]), batch(&[("a", "str")])]);
        assert_eq!(typed_twice[0].ty, "variant", "a variant holds either");
    }

    /// A capture in "payments", and someone who edits only "search".
    fn payments_capture() -> (Captures, Access, Access) {
        use std::collections::BTreeMap;
        let captures = Captures::default();
        let request: CaptureRequest = serde_json::from_value(serde_json::json!({
            "group": "payments",
            "source": {"id": "m", "type": "metrics", "bucketSeconds": 10}
        }))
        .unwrap();
        captures.open(
            "c1".to_string(),
            request,
            ["w1".to_string()].into(),
            Default::default(),
            Vec::new(),
        );
        let editor = |area: &str| Access::Areas(BTreeMap::from([(area.to_string(), Level::Editor)]));
        (captures, editor("payments"), editor("search"))
    }

    /// Following a capture needs editor in its group. Anyone else is answered as for an id that does not exist, so
    /// the ids of other areas' captures stay invisible.
    #[test]
    fn a_capture_in_a_group_the_caller_does_not_edit_answers_as_unknown() {
        let (captures, own, other) = payments_capture();
        let unknown = serde_json::to_value(status_of(&captures, &own, "no-such-id")).unwrap();

        assert_eq!(
            serde_json::to_value(status_of(&captures, &other, "c1")).unwrap(),
            unknown
        );
        assert_eq!(serde_json::to_value(stop(&captures, &other, "c1")).unwrap(), unknown);
        assert!(
            !captures.status("c1").unwrap().stopped,
            "not stopped by the refused call"
        );
        assert!(!discard(&captures, &other, "c1"));
        assert_eq!(
            savable(&captures, &other, "c1").unwrap_err(),
            savable(&captures, &own, "no-such-id").unwrap_err()
        );
        assert!(captures.status("c1").is_some(), "not discarded by the refused call");

        let viewer = Access::Areas([("payments".to_string(), Level::Viewer)].into());
        assert_eq!(
            serde_json::to_value(status_of(&captures, &viewer, "c1")).unwrap(),
            unknown
        );

        assert!(status_of(&captures, &own, "c1").ok);
        assert!(
            savable(&captures, &own, "c1").unwrap_err().contains("still running"),
            "its own group's editor reaches the save's own checks"
        );
        assert!(stop(&captures, &own, "c1").body.capture.unwrap().stopped);
        assert!(status_of(&captures, &Access::Administrator, "c1").ok);
        assert!(discard(&captures, &own, "c1"));
        assert!(captures.status("c1").is_none());
    }
}
