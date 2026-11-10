//! Starting a capture, watching it fill, and saving what it caught.
//!
//! The leader opens no connection here. A capture is an invitation every worker picks up on its next heartbeat
//! (`leader::capture`), and this is the operator's side of it: ask, poll, then save the records as a sample like any
//! other (`config_file::sample`), which is where capture and paste meet.

use axum::Json;
use axum::extract::{Extension, Path as AxPath, State};
use axum::response::{IntoResponse, Response};

use smeltr::config_file;
use smeltr::leader::access::{Access, Level};
use smeltr::leader::accounts;
use smeltr::leader::capture::{CaptureRequest, CaptureStatus, Captures, Opened};
use smeltr::leader::envelope::{Envelope, Refusal};
use smeltr::leader::store::Actor;
use smeltr::leader::{AppState, blocking};

use crate::config_api;
use crate::policy::Need;

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
/// The gate let in anyone who views somewhere; what is captured is the group's records, so it needs viewer there.
/// Its audit row says what the workers were asked to read ([`captured`]), so a capture of a source the group does
/// not run is on the record.
pub async fn start_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Extension(actor): Extension<Actor>,
    Json(request): Json<CaptureRequest>,
) -> Response {
    let need = Need::Area {
        area: request.group.clone(),
        level: Level::Viewer,
    };
    if !need.met_by(&access) {
        return Refusal::from(need).into_response();
    }
    // The commit the fleet ran when the capture opened, for a sample saved from it to record and for the audit to
    // compare against. A leader that cannot read its history still takes the capture, and records none.
    let (data_dir, request_read) = (state.data_dir.clone(), request.clone());
    let (config, what) = blocking(move || {
        let config = smeltr::leader::git::deployed(&smeltr::leader::git::repo_path(&data_dir));
        let what = captured(&data_dir, &request_read, config.as_deref());
        Ok::<_, String>((config, what))
    })
    .await
    .unwrap_or_else(|_| (None, captured(&state.data_dir, &request, None)));
    let store = state.store.clone();
    let response = open_capture(state, request, actor.id.clone(), config)
        .await
        .into_response();
    // A capture reads live records out of a group, so who opened one, and on what, is recorded.
    if response.status().is_success() {
        accounts::record_async(store, &actor, "capture-start", &what, "ok").await;
    }
    response
}

/// What a capture-start's audit row names: its group, then what its workers were asked to read. A destination, and a
/// source the group's deployment runs unchanged, are named by id. Any other source is written out whole, its
/// credentials by name as a bundle names them, so whoever reads the audit sees where it pointed.
fn captured(data_dir: &std::path::Path, request: &CaptureRequest, deployed: Option<&str>) -> String {
    let group = &request.group;
    match (&request.destination, &request.source) {
        (Some(destination), _) => format!("{group}: destination {destination}"),
        (None, Some(source)) if deployed.is_some_and(|commit| runs(data_dir, commit, group, source)) => {
            format!("{group}: source {} as deployed", source.id)
        }
        (None, Some(source)) => {
            let written = serde_json::to_string(source).unwrap_or_else(|e| format!("({e})"));
            format!("{group}: source {}, not as deployed: {written}", source.id)
        }
        (None, None) => group.clone(),
    }
}

/// Whether `group`'s bundle at `commit` holds `source` exactly as the request carries it, in the group's own subtree
/// or in `shared`.
fn runs(data_dir: &std::path::Path, commit: &str, group: &str, source: &config_file::SourceDto) -> bool {
    let sent = serde_json::to_value(source).ok();
    [group, config_file::SHARED_DIR].into_iter().any(|scope| {
        config_api::Reading::at_commit_in(data_dir, scope, commit).is_ok_and(|reading| {
            reading
                .raw()
                .file
                .sources
                .iter()
                .find(|s| s.id == source.id)
                .is_some_and(|s| serde_json::to_value(s).ok() == sent)
        })
    })
}

/// `owner` holds the capture in place of any other they held (`Captures::open`).
async fn open_capture(
    state: AppState,
    request: CaptureRequest,
    owner: String,
    config: Option<String>,
) -> Envelope<StartedCapture> {
    if !takes_its_start(&request) {
        return Envelope::refused(Refusal::invalid(
            "only a Kafka or Event Hubs source's capture takes a start",
        ));
    }
    if let Some(why) = not_as_sent(&request) {
        return Envelope::refused(Refusal::invalid(why));
    }
    let source = match (&request.source, &request.destination) {
        (Some(source), None) => source.clone(),
        (None, Some(destination)) => {
            let destination = destination.clone();
            return start_destination_capture(&state, request, owner, &destination, config);
        }
        _ => return Envelope::refused(Refusal::invalid("a capture names one source or one destination")),
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
        on_exception: None,
        name: None,
        schemas: Vec::new(),
    };
    let mut invalid = config_file::validation_errors(one_source.clone());
    // Run reads an SQL source's query before its author has declared fields or chosen a mode: the worker declares
    // every column the query returns and reads every poll (`worker::capture::run_sql_capture`).
    if source.kind.undeclared() {
        invalid.retain(|e| e.field != Some("fields"));
        // Nor does it refuse the query's own ORDER BY, which only orders the rows Run shows; a save still does.
        if let Some((kind, sql)) = source.kind.sql()
            && smeltr::io::sql::Query::parse_for_run(&sql.query, kind.dialect()).is_ok()
        {
            invalid.retain(|e| e.field != Some("query"));
        }
    }
    if !invalid.is_empty() {
        return Envelope::refused(Refusal::invalid(
            invalid.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "),
        ));
    }

    // A source whose record is known only once the worker describes its query has its filter compiled there; only
    // its syntax can be checked here.
    if let Some(filter) = request.filter.as_deref() {
        let compiled = if source.kind.undeclared() {
            smeltr::plan::compile::parse(filter, None)
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            smeltr::worker::capture::Filter::compile(Some(filter), source.kind.meta(), &source.id).map(|_| ())
        };
        if let Err(e) = compiled {
            return Envelope::refused(Refusal::invalid(e));
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
    let store = state.store.clone();
    let granted = match blocking(move || lent(&store, &one_source)).await {
        Ok(v) => v,
        Err(e) => return Envelope::refused(Refusal::internal(e)),
    };
    if !granted.is_empty() {
        let named: Vec<String> = granted.iter().map(|d| format!("{} v{}", d.name, d.version)).collect();
        tracing::info!(
            name: "secrets_granted", target: "captures",
            subject = %session,
            group = %request.group,
            secrets = %named.join(", "),
            "a capture carries credentials to its workers"
        );
    }
    let invitation = state.captures.open(
        session.clone(),
        request,
        expected,
        holding,
        granted,
        Opened { owner, config },
    );
    Envelope::ok(StartedCapture {
        session: Some(session),
        max_matches: Some(invitation.max_matches),
        max_bytes: Some(invitation.max_bytes),
        deadline_secs: Some(invitation.deadline_secs),
    })
}

/// Open a capture at a deployed destination. It carries no configuration and no credentials: a worker reads only
/// what its own deployment sends there, and one that sends nothing there says so. Its filter reads each row as its
/// pipeline finished it.
fn start_destination_capture(
    state: &AppState,
    request: CaptureRequest,
    owner: String,
    destination: &str,
    config: Option<String>,
) -> Envelope<StartedCapture> {
    if destination.is_empty() {
        return Envelope::refused(Refusal::invalid("a capture at a destination names it"));
    }
    // Only its syntax can be checked here: what it reads is each route's output, which the workers' deployment
    // compiles. A filter a route's output does not resolve ends the capture there, saying which route.
    if let Some(filter) = request.filter.as_deref()
        && let Err(e) = smeltr::plan::compile::parse(filter, None)
    {
        return Envelope::refused(Refusal::invalid(e.to_string()));
    }
    let (expected, holding) = waits_for(state, &request.group, None, Some(destination));
    let session = format!("{:016x}", rand_session());
    let invitation = state.captures.open(
        session.clone(),
        request,
        expected,
        holding,
        Vec::new(),
        Opened { owner, config },
    );
    Envelope::ok(StartedCapture {
        session: Some(session),
        max_matches: Some(invitation.max_matches),
        max_bytes: Some(invitation.max_bytes),
        deadline_secs: Some(invitation.deadline_secs),
    })
}

/// What a capture of `one_source` lends its workers: the secrets it names, and the certificates it names at the
/// list's newest version, as a deployment runs them. Nothing about it need be deployed.
fn lent(
    store: &smeltr::leader::store::Store,
    one_source: &config_file::FileConfig,
) -> Result<Vec<smeltr::heartbeat::SecretDelivery>, String> {
    let (wanted, certificates) = leader_stored_refs(one_source);
    if wanted.is_empty() && certificates.is_empty() {
        return Ok(Vec::new());
    }
    let newest = smeltr::leader::certificate_versions(store)?;
    let quoted = smeltr::leader::Quoted {
        secrets: wanted,
        certificates,
    };
    store
        .secrets_for_delivery(&smeltr::leader::at_newest(&quoted, &newest), &[])
        .map_err(|e| e.to_string())
}

/// Every leader-stored credential one source names, as the store addresses them. A worker-local one is a variable
/// on the worker's own host and is nobody's to deliver.
fn leader_stored_refs(one_source: &config_file::FileConfig) -> (Vec<(String, i64)>, Vec<String>) {
    use smeltr::plan::topology::SecretRef;
    let Ok(json) = serde_json::to_string(one_source) else {
        return (Vec::new(), Vec::new());
    };
    let (references, certificates): (Vec<SecretRef>, Vec<String>) = match config_file::load_str(&json) {
        Ok(system) => (
            system.topology.named_secrets.into_iter().map(|s| s.reference).collect(),
            system.topology.named_certificates.into_iter().map(|c| c.name).collect(),
        ),
        // An SQL source that declares no fields yet does not load, though Run reads it, so its credentials are read
        // from its connection.
        Err(_) => match one_source.sources.first().and_then(config_file::sql_connection) {
            Some(database) => (
                database.password().cloned().into_iter().collect(),
                database.tls().and_then(|t| t.certificate.clone()).into_iter().collect(),
            ),
            None => (Vec::new(), Vec::new()),
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
    (out, certificates)
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

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PasteRequest {
    /// The area whose previews run over the records; the caller must be able to view it.
    pub group: String,
    /// The source the records stand for, where the caller names one.
    #[serde(default)]
    pub source: Option<String>,
    pub records: Vec<smeltr::io::RecordObject>,
}

/// Hold records the caller pasted as the capture they hold, in place of any other, until they save or discard it or
/// it goes unused (`Captures::hold`). Answers it as a capture's status is answered.
pub async fn hold_paste(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Extension(actor): Extension<Actor>,
    Json(request): Json<PasteRequest>,
) -> Response {
    let need = Need::Area {
        area: request.group.clone(),
        level: Level::Viewer,
    };
    if !need.met_by(&access) {
        return Refusal::from(need).into_response();
    }
    let session = format!("{:016x}", rand_session());
    let held = state.captures.hold(
        session.clone(),
        actor.id.clone(),
        request.group,
        request.source,
        request.records,
    );
    match held {
        Ok(()) => status_of(&state.captures, &access, &session).into_response(),
        Err(e) => Envelope::<Followed>::refused(Refusal::invalid(e)).into_response(),
    }
}

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Followed {
    #[serde(skip_serializing_if = "Option::is_none")]
    capture: Option<CaptureStatus>,
}

/// The capture the caller holds (`Captures::open`), if the leader still holds it and they still view its group.
pub async fn my_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Extension(actor): Extension<Actor>,
) -> Envelope<Followed> {
    let capture = state
        .captures
        .held_by(&actor.id)
        .and_then(|session| followed_status(&state.captures, &access, &session));
    Envelope::ok(Followed { capture })
}

/// A finished capture as a preview runs it in place of a saved sample: the capture named by its session, standing for
/// the source it was taken from, or the reason it cannot run. Only the person who opened the capture previews over it,
/// and only while they view its group.
pub fn held_for_preview(
    captures: &Captures,
    access: &Access,
    actor: &Actor,
    session: &str,
) -> Result<config_file::SampleDto, String> {
    let gone = || {
        "these records are no longer kept: newer ones replaced them, or they went unused too long, or they were \
         dropped to make room. Capture or paste them again"
            .to_string()
    };
    if captures.owner(session).as_deref() != Some(actor.id.as_str()) {
        return Err(gone());
    }
    let status = followed_status(captures, access, session).ok_or_else(gone)?;
    if !status.done {
        return Err("this capture is still running. Stop it to use what it holds".to_string());
    }
    let (columns, records) = held_records(captures, session, &status);
    Ok(config_file::SampleDto {
        id: session.to_string(),
        name: session.to_string(),
        source: status.source,
        columns,
        records,
    })
}

pub async fn capture_status(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Envelope<Followed> {
    status_of(&state.captures, &access, &session)
}

fn status_of(captures: &Captures, access: &Access, session: &str) -> Envelope<Followed> {
    Followed::found(followed_status(captures, access, session))
}

/// What a capture holds, read apart from its status, which only counts it.
#[derive(Default, serde::Serialize)]
pub struct HeldRecords {
    batches: Vec<smeltr::heartbeat::CaptureBatch>,
}

/// What `session` holds so far, running or over: one batch per distinct column list, each row naming its worker.
/// Worth reading again only once its status's `revision` moves.
pub async fn capture_records(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Envelope<HeldRecords> {
    records_of(&state.captures, &access, &session)
}

fn records_of(captures: &Captures, access: &Access, session: &str) -> Envelope<HeldRecords> {
    match followed_status(captures, access, session) {
        Some(status) => Envelope::ok(HeldRecords {
            batches: status.batches,
        }),
        None => Envelope::refused(Refusal::not_found(NO_CAPTURE)),
    }
}

/// Whether `access` may follow `session`: watch, stop, discard, download or save it. The gate let in anyone who views
/// somewhere; a capture holds its group's records, so it needs viewer there, and saving it needs editor where the
/// sample is saved. A capture in a group the caller does not view answers as an unknown id does, so the ids of
/// other areas' captures stay invisible.
fn followed(captures: &Captures, access: &Access, session: &str) -> bool {
    captures
        .group(session)
        .is_some_and(|group| access.allows(&group, Level::Viewer))
}

/// The status of `session`, if `access` may follow it (see [`followed`]).
fn followed_status(captures: &Captures, access: &Access, session: &str) -> Option<CaptureStatus> {
    followed(captures, access, session)
        .then(|| captures.status(session))
        .flatten()
}

impl Followed {
    fn found(capture: Option<CaptureStatus>) -> Envelope<Followed> {
        match capture {
            Some(capture) => Envelope::ok(Followed { capture: Some(capture) }),
            None => Envelope::refused(Refusal::not_found(NO_CAPTURE)),
        }
    }
}

/// What a capture the caller cannot follow is answered with, whether it never was or is not theirs.
const NO_CAPTURE: &str = "no capture by that id -- it may have been saved, or swept after finishing";

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveCaptureRequest {
    /// The capture whose records the sample holds.
    pub capture: String,
    /// The sample's id, minted by the GUI so a rename keeps it. Absent from a client that predates it, which gets
    /// the name as the id.
    #[serde(default)]
    pub id: Option<String>,
    /// What to call the sample.
    pub name: String,
    /// The source these records stand for, so the sample can be route-previewed. A capture at a source names one; a
    /// capture at a destination names none, and a route preview of it is told which source it stands for.
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

/// The capture `session` as a save would commit it, or why it cannot be saved.
fn savable(captures: &Captures, access: &Access, session: &str) -> Result<CaptureStatus, Refusal> {
    let status = followed_status(captures, access, session).ok_or_else(|| Refusal::not_found(NO_CAPTURE))?;
    if !status.done {
        let waiting = if status.waiting_on.is_empty() {
            String::new()
        } else {
            format!(" -- waiting on {}", status.waiting_on.join(", "))
        };
        return Err(Refusal::conflict(format!(
            "this capture is still running{waiting}. Stop it first to save what it holds"
        )));
    }
    if status.batches.iter().all(|b| b.rows.is_empty()) {
        return Err(Refusal::conflict(
            "this capture caught nothing, and a sample of no records would preview as an empty pipeline \
             rather than as the problem it is",
        ));
    }
    Ok(status)
}

/// What `status`, the capture `session`, holds, as a sample of it holds it: its columns, and its records with how it
/// was taken on each, as its `__source` fields are.
fn held_records(
    captures: &Captures,
    session: &str,
    status: &CaptureStatus,
) -> (Vec<smeltr::heartbeat::CaptureColumn>, Vec<smeltr::io::RecordObject>) {
    let mut records: Vec<smeltr::io::RecordObject> =
        status.batches.iter().flat_map(config_file::sample_records).collect();
    if status.pasted {
        return (Vec::new(), records);
    }
    let mut columns = config_file::sample_columns(&status.batches);
    for (column, value) in captures.capture_fields(session) {
        for record in &mut records {
            config_file::put_path(record, &column.name, value.clone());
        }
        columns.push(column);
    }
    (columns, records)
}

/// A finished capture's records as a file to keep or send on: one JSON record a line, each as a sample of it holds
/// it. The session stays, to be saved or discarded after.
pub async fn download_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    AxPath(session): AxPath<String>,
) -> Response {
    let status = match savable(&state.captures, &access, &session) {
        Ok(status) => status,
        Err(e) => return Envelope::<()>::refused(e).into_response(),
    };
    let (_, records) = held_records(&state.captures, &session, &status);
    let mut body = Vec::new();
    for record in &records {
        serde_json::to_writer(&mut body, record).expect("a record object serializes");
        body.push(b'\n');
    }
    let disposition = format!("attachment; filename=\"capture-{session}.ndjson\"");
    (
        [
            (axum::http::header::CONTENT_TYPE, "application/x-ndjson".to_string()),
            (axum::http::header::CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response()
}

/// Save a finished capture as a sample, and forget the session.
///
/// This is the seam the whole design turns on: a capture ends at a sample file, exactly as a paste does, and nothing
/// downstream can tell which way the records arrived. Saving is a commit on a branch like any other save.
pub async fn save_capture(
    State(state): State<AppState>,
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<Access>,
    at: config_api::Addressed,
    Json(request): Json<SaveCaptureRequest>,
) -> Envelope<SavedCapture> {
    let session = request.capture.clone();
    let status = match savable(&state.captures, &access, &session) {
        Ok(status) => status,
        Err(e) => return Envelope::refused(e),
    };

    let source = match (&status.destination, request.source.clone()) {
        (Some(_), _) => None,
        (None, None) if !status.pasted => {
            return Envelope::refused(Refusal::invalid(
                "name the source these records stand for, so the sample can be route-previewed",
            ));
        }
        (None, source) => source,
    };
    let (columns, records) = held_records(&state.captures, &session, &status);
    let sample = config_file::SampleDto {
        id: request.sample_id(),
        name: request.name,
        source,
        columns,
        records,
    };
    let records = sample.records.len();

    let saved = blocking(move || {
        let path = config_file::sample_path(&sample.id).map_err(|e| Refusal::invalid(e.to_string()))?;
        let writing = at.open()?;
        let content = Some(config_file::sample_bytes(&sample));
        config_api::refuse_sample_collision(&mut writing.files(), &path, &sample.id, &content)
            .map_err(Refusal::conflict)?;
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
            Envelope::ok(SavedCapture {
                commit: Some(commit),
                records: Some(records),
            })
        }
        Err(e) => Envelope::refused(e),
    }
}

/// End a capture still running, keeping what it holds for a save or a discard. Records that land after are dropped,
/// and every worker still running it lets go once its next heartbeat's answer no longer lists it -- a supervisor's
/// own capture on that beat, a runtime's on the deployer's next turn. A capture already over is answered as it ended.
pub async fn stop_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Extension(actor): Extension<Actor>,
    AxPath(session): AxPath<String>,
) -> Envelope<Followed> {
    let found = followed(&state.captures, &access, &session);
    let stopped = stop(&state.captures, &access, &session);
    if found {
        accounts::record_async(state.store.clone(), &actor, "capture-stop", &session, "ok").await;
    }
    stopped
}

fn stop(captures: &Captures, access: &Access, session: &str) -> Envelope<Followed> {
    let capture = followed(captures, access, session).then(|| captures.stop(session));
    Followed::found(capture.flatten())
}

/// Give up on a capture without saving it.
pub async fn discard_capture(
    State(state): State<AppState>,
    Extension(access): Extension<Access>,
    Extension(actor): Extension<Actor>,
    AxPath(session): AxPath<String>,
) -> Envelope {
    match discard(&state.captures, &access, &session) {
        true => {
            accounts::record_async(state.store.clone(), &actor, "capture-discard", &session, "ok").await;
            Envelope::done()
        }
        false => Envelope::refused(Refusal::not_found(NO_CAPTURE)),
    }
}

/// Whether `session` was found and forgotten.
fn discard(captures: &Captures, access: &Access, session: &str) -> bool {
    followed(captures, access, session) && captures.close(session).is_some()
}

/// Why `request` cannot be captured as sent, where it asks to be.
fn not_as_sent(request: &CaptureRequest) -> Option<&'static str> {
    if !request.raw {
        return None;
    }
    if !request
        .source
        .as_ref()
        .is_some_and(|s| s.kind.receiver_port().is_some())
    {
        return Some("only a receiver is captured as sent");
    }
    // What it keeps arrives before there is a record for a filter to read.
    request.filter.is_some().then_some("a capture as sent takes no filter")
}

/// Whether whatever `request` captures has a position to start from, where it names a start. Anywhere else a start
/// would be read by nothing.
fn takes_its_start(request: &CaptureRequest) -> bool {
    request.start.is_none()
        || request.source.as_ref().is_some_and(|s| {
            matches!(
                s.kind,
                config_file::SourceKindDto::Kafka(_) | config_file::SourceKindDto::EventHubs(_)
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

    /// Only a receiver has bytes before records, and a filter reads a record, so a capture as sent is of a receiver and
    /// takes no filter.
    #[test]
    fn only_a_receiver_is_captured_as_sent_and_without_a_filter() {
        let ask = |json: serde_json::Value| -> CaptureRequest { serde_json::from_value(json).unwrap() };
        let syslog = serde_json::json!({"id":"s","type":"syslogReceiver","port":1514,"allowFrom":["10.0.0.0/8"]});
        let metrics = serde_json::json!({"id":"s","type":"metrics","bucketSeconds":5});

        assert_eq!(
            not_as_sent(&ask(serde_json::json!({"group":"g","source":syslog,"raw":true}))),
            None
        );
        assert_eq!(
            not_as_sent(&ask(serde_json::json!({"group":"g","source":metrics}))),
            None
        );
        assert_eq!(
            not_as_sent(&ask(serde_json::json!({"group":"g","source":metrics,"raw":true}))),
            Some("only a receiver is captured as sent")
        );
        assert_eq!(
            not_as_sent(&ask(serde_json::json!({"group":"g","destination":"es","raw":true}))),
            Some("only a receiver is captured as sent")
        );
        assert_eq!(
            not_as_sent(&ask(
                serde_json::json!({"group":"g","source":syslog,"raw":true,"filter":"true"})
            )),
            Some("a capture as sent takes no filter")
        );
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
        let minted: SaveCaptureRequest = serde_json::from_str(
            r#"{"capture":"c1","id":"sample-4k2p9x1a","name":"orders, ordinary hour","source":"orders"}"#,
        )
        .unwrap();
        assert_eq!(minted.sample_id(), "sample-4k2p9x1a");

        let old: SaveCaptureRequest =
            serde_json::from_str(r#"{"capture":"c1","name":"orders, ordinary hour","source":"orders"}"#).unwrap();
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
                on_exception: None,
                name: None,
                schemas: Vec::new(),
            }
        };

        let stored = one(
            r#"{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
                "properties":{"security.protocol":"SASL_SSL"},"credentials":{"sasl.password":{"secret":"kafka-cred","version":4}}}"#,
        );
        assert_eq!(
            leader_stored_refs(&stored),
            (vec![("kafka-cred".to_string(), 4)], Vec::new()),
            "the one it quotes, at the version it quotes"
        );

        let local = one(
            r#"{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
                "properties":{"security.protocol":"SASL_SSL"},"credentials":{"sasl.password":"KAFKA_PASSWORD"}}"#,
        );
        assert!(
            leader_stored_refs(&local).0.is_empty(),
            "a worker-local variable is the host's, and the leader has no value to send"
        );

        let none = one(r#"{"id":"s","type":"metrics","bucketSeconds":5}"#);
        assert_eq!(leader_stored_refs(&none), (Vec::new(), Vec::new()));

        let fieldless = one(r#"{"id":"s","type":"postgres","host":"db","database":"d","user":"u",
                "password":{"secret":"pg-cred","version":2},"query":"SELECT 1","mode":{"type":"everyPoll"},"fields":[]}"#);
        assert_eq!(
            leader_stored_refs(&fieldless),
            (vec![("pg-cred".to_string(), 2)], Vec::new()),
            "Run reads an SQL source before it declares fields, with its own password"
        );
    }

    /// A source never deployed is captured with its certificate lent for the session, at the list's newest version,
    /// chain beside key. A name the list does not hold is left out, for the worker to refuse.
    #[test]
    fn a_capture_lends_its_certificate_at_the_newest_version() {
        use smeltr::leader::store::Store;
        let key = std::env::temp_dir().join(format!("smeltr-capture-lent-{}.key", std::process::id()));
        let _ = std::fs::remove_file(&key);
        let store = Store::open(std::path::Path::new(":memory:"), &key).unwrap();
        store.create_certificate("cli", "", "chain-1", "key-1", None).unwrap();
        store.add_certificate_version("cli", "chain-2", "key-2", None).unwrap();
        let source = |certificate: &str| config_file::FileConfig {
            version: config_file::CURRENT_VERSION,
            sources: vec![
                serde_json::from_str(&format!(
                    r#"{{"id":"s","type":"kafka","brokers":"b:9092","topics":["t"],"groupId":"g",
                        "properties":{{"security.protocol":"SSL"}},"certificate":"{certificate}"}}"#
                ))
                .unwrap(),
            ],
            destinations: Vec::new(),
            pipelines: Vec::new(),
            routes: Vec::new(),
            lookup_tables: Vec::new(),
            grok_patterns: Vec::new(),
            on_exception: None,
            name: None,
            schemas: Vec::new(),
        };

        let lent_now = lent(&store, &source("cli")).unwrap();
        let named: Vec<_> = lent_now
            .iter()
            .map(|d| (d.name.as_str(), d.version, d.value.as_str(), d.chain.as_deref()))
            .collect();
        assert_eq!(named, vec![("cli", 2, "key-2", Some("chain-2"))]);

        assert!(lent(&store, &source("unlisted")).unwrap().is_empty());
        let _ = std::fs::remove_file(&key);
    }

    /// A capture is previewed over only by whoever opened it, once it is over, and only while they view its group.
    #[test]
    fn only_its_owner_previews_over_a_finished_capture() {
        let (captures, own, other) = payments_capture();
        let person = |id: &str| Actor {
            id: id.to_string(),
            ..Actor::system()
        };
        let gone = held_for_preview(&captures, &own, &person("alice"), "no-such-id").unwrap_err();
        assert_eq!(
            held_for_preview(&captures, &own, &person("bob"), "c1").unwrap_err(),
            gone
        );
        assert_eq!(
            held_for_preview(&captures, &other, &person("alice"), "c1").unwrap_err(),
            gone
        );
        assert!(
            held_for_preview(&captures, &own, &person("alice"), "c1")
                .unwrap_err()
                .contains("still running")
        );
        captures.stop("c1");
        assert!(held_for_preview(&captures, &own, &person("alice"), "c1").is_ok());
    }

    /// A paste is held in place of its owner's capture and reads back as written: no columns of its own, no
    /// `__capture` fields, and the source it was given.
    #[test]
    fn a_paste_replaces_its_owner_s_capture_and_runs_as_written() {
        let (captures, own, _) = payments_capture();
        let alice = Actor {
            id: "alice".to_string(),
            ..Actor::system()
        };
        let record = |raw: &str| serde_json::json!({"_raw": raw}).as_object().unwrap().clone();
        captures
            .hold(
                "p1".into(),
                "alice".into(),
                "payments".into(),
                Some("orders".into()),
                vec![record("a"), record("b")],
            )
            .unwrap();
        assert_eq!(
            captures.held_by("alice").as_deref(),
            Some("p1"),
            "the paste replaced the capture"
        );
        let sample = held_for_preview(&captures, &own, &alice, "p1").unwrap();
        assert!(sample.columns.is_empty(), "{:?}", sample.columns);
        assert_eq!(sample.records, vec![record("a"), record("b")]);
        assert_eq!(sample.source.as_deref(), Some("orders"));

        let too_many = vec![record("x"); config_file::MAX_SAMPLE_RECORDS + 1];
        assert!(
            captures
                .hold("p2".into(), "alice".into(), "payments".into(), None, too_many)
                .is_err()
        );
        assert!(
            captures
                .hold("p3".into(), "alice".into(), "payments".into(), None, Vec::new())
                .is_err()
        );
        assert_eq!(
            captures.held_by("alice").as_deref(),
            Some("p1"),
            "a refused paste keeps what was held"
        );
    }

    /// A capture in "payments", someone who views "payments", and someone who views only "search".
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
            Opened {
                owner: "alice".to_string(),
                config: None,
            },
        );
        let viewer = |area: &str| Access::Areas(BTreeMap::from([(area.to_string(), Level::Viewer)]));
        (captures, viewer("payments"), viewer("search"))
    }

    /// Following a capture needs viewer in its group. Anyone else is answered as for an id that does not exist, so
    /// the ids of other areas' captures stay invisible.
    #[test]
    fn a_capture_in_a_group_the_caller_does_not_view_answers_as_unknown() {
        let (captures, own, other) = payments_capture();
        let unknown = status_of(&captures, &own, "no-such-id").to_json();

        assert_eq!(status_of(&captures, &other, "c1").to_json(), unknown);
        assert_eq!(
            records_of(&captures, &other, "c1").to_json(),
            records_of(&captures, &own, "no-such-id").to_json()
        );
        assert!(records_of(&captures, &own, "c1").succeeded());
        assert_eq!(stop(&captures, &other, "c1").to_json(), unknown);
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

        assert!(status_of(&captures, &own, "c1").succeeded());
        assert!(
            savable(&captures, &own, "c1")
                .unwrap_err()
                .message()
                .contains("still running"),
            "its own group's viewer reaches the save's own checks"
        );
        assert!(stop(&captures, &own, "c1").body.capture.unwrap().stopped);
        assert!(status_of(&captures, &Access::Administrator, "c1").succeeded());
        assert!(discard(&captures, &own, "c1"));
        assert!(captures.status("c1").is_none());
    }

    fn metrics_source(bucket: u64) -> config_file::SourceDto {
        serde_json::from_value(serde_json::json!({"id": "m", "type": "metrics", "bucketSeconds": bucket})).unwrap()
    }

    fn request_for(source: Option<config_file::SourceDto>, destination: Option<&str>) -> CaptureRequest {
        CaptureRequest {
            group: "payments".to_string(),
            source,
            destination: destination.map(str::to_string),
            filter: None,
            start: None,
            raw: false,
            max_matches: None,
            max_bytes: None,
            deadline_secs: None,
        }
    }

    /// A scratch leader whose `work` branch holds `source` in "payments", and the commit that holds it.
    fn a_repo_holding(source: &config_file::SourceDto) -> (std::path::PathBuf, String) {
        use smeltr::leader::git;
        let data = std::env::temp_dir().join(format!("smeltr-capture-api-{}-{}", std::process::id(), rand_session()));
        let repo = git::repo_path(&data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();
        let commit = config_api::Writing::open_in(&data, "work", None, "payments")
            .unwrap()
            .commit(
                vec![git::FileEdit {
                    path: "sources.json".to_string(),
                    content: Some(serde_json::to_vec(&[source]).unwrap()),
                }],
                "seed",
                &Actor::system(),
            )
            .expect("seeded");
        (data, commit)
    }

    /// The audit names a capture of the deployed source by its id, and writes out whole one that differs from it, so
    /// a capture pointed somewhere new is on the record.
    #[test]
    fn the_audit_says_what_a_capture_read() {
        let deployed = metrics_source(10);
        let (data, commit) = a_repo_holding(&deployed);

        let as_deployed = request_for(Some(deployed), None);
        assert_eq!(
            captured(&data, &as_deployed, Some(&commit)),
            "payments: source m as deployed"
        );

        let changed = request_for(Some(metrics_source(60)), None);
        let written = captured(&data, &changed, Some(&commit));
        assert!(
            written.starts_with("payments: source m, not as deployed: {"),
            "{written}"
        );
        assert!(written.contains(r#""bucketSeconds":60"#), "{written}");
        assert_eq!(
            captured(&data, &request_for(Some(metrics_source(10)), None), None),
            written.replace("60", "10"),
            "with nothing deployed, a source is written out whole"
        );

        let at = request_for(None, Some("search"));
        assert_eq!(captured(&data, &at, Some(&commit)), "payments: destination search");
        std::fs::remove_dir_all(&data).ok();
    }
}
