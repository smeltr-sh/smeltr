//! `smeltr`, the product's only binary (`smeltr-demo` is the website demo's, and ships in no release). Both roles
//! are compiled into it and each is enabled by its own flag: `[leader] enabled` and `[worker] enabled`. A process
//! with neither refuses to start; one with both runs a leader and a worker that reports to it.
//!
//! The leader serves everything on one port: the admin and worker API (`smeltr::leader`), the GUI and its config API,
//! the preview and capture endpoints, and the Cluster page (`cluster.rs`).

use std::collections::HashMap;

use axum::Router;
use axum::extract::{Json, Multipart};
use axum::routing::{delete, get, patch, post, put};

mod acme_api;
mod agent_catalog;
mod agents_api;
mod assets;
mod auth_api;
mod branch_api;
mod capture_api;
mod certificates_api;
mod chat_api;
mod cluster;
mod config_api;
mod docs_api;
mod gate;
mod oidc_api;
mod policy;
mod roles_api;
mod service;
mod sql_api;
mod test_api;
mod users_api;
mod worker_groups_api;

use smeltr::config_file;
use smeltr::leader::envelope::{ApiError, Envelope, Refusal};
use smeltr::leader::preview::{
    FormatRequest, Formatted, GrokCaptures, GrokCapturesRequest, LineBreakerRequest, Preview, PreviewRequest,
    RoutePreview, RoutePreviewRequest, captures_of, format_code, line_breaker_verdict, run_preview, run_route_preview,
    single_origin_declared_at,
};
use smeltr::leader::sessions;
use smeltr::leader::{blocking, blocking_answer};

async fn route_preview(
    axum::extract::Path(area): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
    Json(req): Json<RoutePreviewRequest>,
) -> Envelope<RoutePreview> {
    let branch = q.name().to_string();
    blocking_answer(move || route_preview_on(&smeltr::startup_config::leader_data_dir(), &branch, &area, req)).await
}

/// The route preview of `req.config` on `branch`, its sample read from that branch. A group's routes run with
/// `shared/` merged in, as the group runs once deployed: shared's routes route the sample too, and a group route may
/// read a shared source, call a shared pipeline or read a shared lookup table. `data_dir` explicit, as for
/// `Reading::open_in`.
fn route_preview_on(
    data_dir: &std::path::Path,
    branch: &str,
    scope: &str,
    req: RoutePreviewRequest,
) -> Envelope<RoutePreview> {
    let reading = match config_api::Reading::open_in(data_dir, branch, scope) {
        Ok(reading) => reading,
        Err(e) => return Envelope::refused(e),
    };
    match config_file::read_sample(&mut reading.files(), &req.sample) {
        Ok(Some(sample)) => {
            let merged = if scope == config_file::SHARED_DIR {
                None
            } else {
                merged_with_shared(&req.config, &reading)
            };
            match merged {
                Some(merged) => run_route_preview(&merged.config, sample, merged.declared_at, req.at_micros),
                None => {
                    let declared_at = single_origin_declared_at(&req.config, &reading.at());
                    run_route_preview(&req.config, sample, declared_at, req.at_micros)
                }
            }
        }
        Ok(None) => Envelope::refused(Refusal::not_found(format!(
            "there is no sample {:?} on this branch",
            req.sample
        ))),
        Err(e) => Envelope::refused(Refusal::internal(e.to_string())),
    }
}

// -- topology map ---------------------------------------------------------
//
// The whole fleet's compiled topology for a branch, as nodes and edges banded by worker group (design doc "The
// map"; `smeltr::leader::map` builds it). Unlike preview and route preview (`leader::preview`), this reads the branch's own
// stored bundle rather than a client-supplied draft: the map is a view of what is committed, not of an editor's
// in-progress buffer, so it needs only a branch name -- never an area, which names the bundle an *editor* is
// working in and has nothing to say about what the fleet runs (`docs/user-stories.md` G2: "a group is a filter on
// that view rather than a mode that hides the rest"). The gate let in anyone viewing somewhere, so the groups are
// the ones the caller holds (`map::visible_groups`).

async fn get_map(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    axum::Extension(access): axum::Extension<smeltr::leader::access::Access>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
) -> Envelope<smeltr::leader::map::Map> {
    let branch = q.name().to_string();
    let read = move || -> Result<
        (
            config_file::RawBundle,
            config_file::BundleAt,
            Vec<smeltr::leader::map::GroupBundle>,
            String,
        ),
        Refusal,
    > {
        let shared = config_api::Reading::open(&branch, config_file::SHARED_DIR)?;
        let groups = group_bundles(&shared, visible_groups(&shared, &access)?)?;
        Ok((shared.raw(), shared.at(), groups, shared.commit.clone()))
    };
    let (shared_raw, shared_at, groups, commit) = match blocking(read).await {
        Ok(read) => read,
        Err(e) => return Envelope::refused(e),
    };
    let warnings = match smeltr::leader::warnings_snapshot(&state).await {
        Ok(w) => w,
        Err(e) => return Envelope::refused(e),
    };
    // Compiling is the expensive part -- `run_route_preview` above pays the same cost the same way -- so it runs
    // off the async executor too.
    let build = move || {
        Ok::<_, Refusal>(smeltr::leader::map::build(
            shared_raw,
            &shared_at,
            groups,
            &warnings,
            state.registry.series(),
        ))
    };
    blocking(build)
        .await
        .map(|map| smeltr::leader::map::Map {
            commit: Some(commit),
            ..map
        })
        .into()
}

// -- map measurements ---------------------------------------------------------
//
// What the map's nodes report now, polled on its own so a refresh never recompiles the fleet
// (`leader::measurements`). The branch is read only for its commit: that is how the page learns
// the topology it drew is out of date.

async fn get_map_measurements(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    axum::Extension(access): axum::Extension<smeltr::leader::access::Access>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
) -> Envelope<smeltr::leader::measurements::Measurements> {
    use smeltr::leader::measurements::Measurements;
    let branch = q.name().to_string();
    let warnings = match smeltr::leader::warnings_snapshot(&state).await {
        Ok(w) => w,
        Err(e) => return Envelope::refused(e),
    };
    let readings = smeltr::leader::throughput::readings(
        &state,
        smeltr::leader::warnings::now_millis(),
        smeltr::startup_config::worker_heartbeat_interval(),
        true,
    )
    .await;
    let answer = move || -> Result<Measurements, Refusal> {
        let commit = config_api::Reading::open(&branch, config_file::SHARED_DIR)?.commit;
        let held = state.store.held_sources().unwrap_or_else(|e| {
            tracing::warn!(
                target: "map.holds_unreadable",
                error = %e,
                "could not read who holds the single-worker sources, so the map names no holder"
            );
            Vec::new()
        });
        let reading_now = smeltr::leader::measurements::reading_now(&held, |h| {
            state.registry.holds.seen(
                &h.holder,
                &h.group,
                &h.key,
                smeltr::leader::holds::Clock::at(state.registry.up_for()),
            )
        });
        let snapshot = state.registry.snapshot();
        let live = smeltr::leader::stats::Live {
            snapshot: &snapshot,
            heartbeat_interval: smeltr::startup_config::worker_heartbeat_interval(),
            band_share: smeltr::leader::throughput::DEFAULT_BAND_PERCENT / 100.0,
        };
        Ok(Measurements {
            commit: Some(commit),
            ..smeltr::leader::measurements::build(
                &warnings,
                state.registry.series(),
                smeltr::leader::measurements::Holding {
                    held: &held,
                    reading_now: &reading_now,
                },
                live,
                smeltr::leader::warnings::now_millis(),
            )
            .with_expected(&readings)
            .visible_to(&access)
        })
    };
    blocking(answer).await.into()
}

// -- object stats ------------------------------------------------------------
//
// One node's counts, for the map's focus view (`smeltr::leader::stats`). Reads one group's committed bundle rather
// than every group's, which `/api/map` needs and this does not: the question is about one object, and the group it
// runs in is part of naming it.

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatsQuery {
    /// Branch only, the same as the map's: stats are about what the fleet runs, never about an editor's buffer.
    #[serde(flatten)]
    branch: config_api::BranchQuery,
    kind: smeltr::leader::map::NodeKind,
    id: String,
    #[serde(default = "default_stats_window_minutes")]
    window_minutes: i64,
}

fn default_stats_window_minutes() -> i64 {
    10
}

async fn get_stats(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    axum::extract::Path(group): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<StatsQuery>,
) -> Envelope<smeltr::leader::stats::Stats> {
    let branch = q.branch.name().to_string();
    let (kind, id) = (q.kind, q.id.clone());
    let window =
        std::time::Duration::from_secs((q.window_minutes.clamp(1, smeltr::leader::MAX_WINDOW_MINUTES) * 60) as u64);

    // Reading the repository, compiling, and reading the series are all blocking, and the last two are the expensive
    // ones -- the same split `get_map` makes for the same reason, which means carrying `state` in rather than
    // touching the registry back on the executor.
    let answer = {
        let (group, id) = (group.clone(), id.clone());
        move || -> Result<Envelope<smeltr::leader::stats::Stats>, Refusal> {
            let loaded = config_api::Reading::open(&branch, &group).and_then(|reading| {
                let repo = smeltr::leader::git::repo_path(&smeltr::startup_config::leader_data_dir());
                smeltr::leader::tree::load_group_at(&repo, &reading.commit, &group)
                    .map_err(|e| Refusal::internal(e.to_string()))
            });
            Ok(match loaded {
                Err(e) => smeltr::leader::stats::Stats::failed(&group, kind, &id, e),
                Ok(system) => {
                    // A queue's fill is a level, not a total, so it comes off the freshest heartbeat rather than
                    // the series (`stats::Live`).
                    let snapshot = state.registry.snapshot();
                    let live = smeltr::leader::stats::Live {
                        snapshot: &snapshot,
                        heartbeat_interval: smeltr::startup_config::worker_heartbeat_interval(),
                        band_share: smeltr::leader::throughput::band_percent(&*state.store)
                            .unwrap_or(smeltr::leader::throughput::DEFAULT_BAND_PERCENT)
                            / 100.0,
                    };
                    smeltr::leader::stats::build(&group, &system, kind, &id, window, state.registry.series(), live)
                }
            })
        }
    };
    let answered = blocking(answer).await;
    answered.unwrap_or_else(|e| smeltr::leader::stats::Stats::failed(&group, kind, &id, e))
}

/// Every group `shared`'s branch holds that `access` may see, by the same enumerator `worker_groups_api` lists them
/// with -- `shared`'s reading already resolved the branch to a commit, so that is what "this branch's groups" means.
fn visible_groups(
    shared: &config_api::Reading,
    access: &smeltr::leader::access::Access,
) -> Result<Vec<String>, Refusal> {
    let repo = smeltr::leader::git::repo_path(&smeltr::startup_config::leader_data_dir());
    let all = smeltr::leader::tree::groups_at(&repo, &shared.commit).map_err(|e| Refusal::internal(e.to_string()))?;
    Ok(smeltr::leader::map::visible_groups(all, access))
}

/// Each of `ids`, read at the commit `shared` was.
fn group_bundles(
    shared: &config_api::Reading,
    ids: Vec<String>,
) -> Result<Vec<smeltr::leader::map::GroupBundle>, Refusal> {
    ids.into_iter()
        .map(|id| {
            let reading = shared.sibling(&id)?;
            Ok(smeltr::leader::map::GroupBundle {
                id,
                raw: reading.raw(),
                at: reading.at(),
            })
        })
        .collect()
}

// -- connections --------------------------------------------------------------
//
// What a change to one object reaches, in each group that runs it (user story A5, blast radius): the editor's
// question before anything changes, answered from the bundles alone (`leader::connections`).

/// The kind of object a section of an area's path holds: `sources`, `pipelines`, and so on.
fn kind_of_section(section: &str) -> Option<config_file::Kind> {
    use config_file::Kind;
    Some(match section {
        "sources" => Kind::Source,
        "routes" => Kind::Route,
        "pipelines" => Kind::Pipeline,
        "destinations" => Kind::Destination,
        "lookups" => Kind::LookupTable,
        "grok-patterns" => Kind::GrokPattern,
        _ => return None,
    })
}

async fn get_connections(
    axum::Extension(access): axum::Extension<smeltr::leader::access::Access>,
    axum::extract::Path((area, section, id)): axum::extract::Path<(String, String, String)>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
) -> Envelope<smeltr::leader::connections::ObjectConnections> {
    let Some(kind) = kind_of_section(&section) else {
        return Envelope::refused(Refusal::not_found(format!(
            "an area holds no section {section:?}; sections are sources, routes, pipelines, destinations, lookups, \
             grok-patterns"
        )));
    };
    let branch = q.name().to_string();
    let read = move || -> Result<_, Refusal> {
        let shared = config_api::Reading::open(&branch, config_file::SHARED_DIR)?;
        // A group's own object runs in that group alone; one `shared/` declares, in every group the caller may see.
        let groups = if area == config_file::SHARED_DIR {
            visible_groups(&shared, &access)?
        } else {
            vec![area.clone()]
        };
        let groups = group_bundles(&shared, groups)?;
        let object = config_file::Object::new(kind, &id);
        Ok(smeltr::leader::connections::of(&shared.raw(), &groups, &object))
    };
    blocking(read).await.into()
}

// -- pipeline callers -------------------------------------------------------
//
// Pipeline id -> the pipelines that call it, directly: what the map's `calls:` search reads, since a call is never
// drawn. Answered from `Call` steps alone (`config_file::Links`), so a pipeline mid-edit that does not compile still
// answers it.

#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PipelineCallers {
    /// Pipeline id -> the pipelines that call it, directly.
    callers: HashMap<String, Vec<String>>,
}

async fn pipeline_callers(
    axum::extract::Path(area): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
) -> Envelope<PipelineCallers> {
    let branch = q.name().to_string();
    blocking_answer(move || match config_api::Reading::open(&branch, &area) {
        Ok(reading) => run_pipeline_callers(reading.raw()),
        Err(e) => Envelope::refused(e),
    })
    .await
}

fn run_pipeline_callers(raw: config_file::RawBundle) -> Envelope<PipelineCallers> {
    let errors = raw
        .unreadable
        .iter()
        .map(|(path, message)| ApiError::new(format!("{path}: {message}")))
        .collect();
    let callers = config_file::Links::of(&raw.file)
        .callers
        .into_iter()
        .map(|(called, by)| (called, by.into_iter().collect()))
        .collect();
    Envelope::with(
        PipelineCallers { callers },
        config_api::read_status(&raw.unreadable),
        errors,
    )
}

/// The captures of a grok pattern, with no sample and no pipeline.
///
/// Deliberately not part of an area's `preview/pipeline`: that needs a chosen sample and answers nothing once
/// compilation fails, and both are exactly when an editor most needs the names -- a step whose pattern just gained a
/// capture nothing declares does not compile, and it is this list that says what to declare.
async fn grok_captures(Json(req): Json<GrokCapturesRequest>) -> Envelope<GrokCaptures> {
    captures_of(req)
}

/// The records of a captured sample, for `/api/preview/onboard`.
#[derive(serde::Deserialize)]
pub struct OnboardRequest {
    records: Vec<smeltr::io::RecordObject>,
    /// Every captured record's `__source`, in order, when the caller sends fewer records than it captured: the
    /// route condition must hold for all of them.
    #[serde(default)]
    filed: Option<Vec<serde_json::Value>>,
}

/// What a captured sample reads as, and the first steps and route condition a pipeline for it would start from
/// (`leader::onboard`). The records are the caller's own, so nothing here reads a branch.
async fn onboard_propose(Json(req): Json<OnboardRequest>) -> Envelope<smeltr::leader::onboard::Proposal> {
    Envelope::ok(smeltr::leader::onboard::propose(&req.records, req.filed.as_deref()))
}

/// Pipeline code laid out in the style, asked when the editor leaves a code field.
async fn format(Json(req): Json<FormatRequest>) -> Envelope<Formatted> {
    format_code(req)
}

/// Whether a save would take a line breaker, asked as it is typed: the save's own check, with no bundle around it.
async fn line_breaker(Json(req): Json<LineBreakerRequest>) -> Envelope {
    line_breaker_verdict(req)
}

async fn preview(
    axum::extract::Path(area): axum::extract::Path<String>,
    axum::extract::Query(q): axum::extract::Query<config_api::BranchQuery>,
    Json(req): Json<PreviewRequest>,
) -> Envelope<Preview> {
    // Reading the branch spawns git, so it goes off the async executor.
    let branch = q.name().to_string();
    blocking_answer(move || preview_on(&smeltr::startup_config::leader_data_dir(), &branch, &area, req)).await
}

/// A preview of what the editor is looking at on `branch`: its sample and any lookup path in it resolve the same way
/// a save would, against the bundle on that branch. A group's preview runs with `shared/` merged in, as the group
/// runs once deployed, so a pipeline calling a shared pipeline or reading a shared lookup table previews rather than
/// naming it unknown. `data_dir` explicit, as for `Reading::open_in`.
fn preview_on(data_dir: &std::path::Path, branch: &str, scope: &str, mut req: PreviewRequest) -> Envelope<Preview> {
    let reading = match config_api::Reading::open_in(data_dir, branch, scope) {
        Ok(reading) => reading,
        Err(e) => return Envelope::refused(e),
    };
    let sample = match &req.sample {
        None => Ok((Vec::new(), Vec::new())),
        Some(id) => match config_file::read_sample(&mut reading.files(), id) {
            Ok(Some(sample)) => Ok((sample.columns, sample.records)),
            Ok(None) => Err(format!("there is no sample {id:?} on this branch")),
            Err(e) => Err(e.to_string()),
        },
    };
    match sample {
        Ok((columns, records)) => {
            let declared_at = if scope == config_file::SHARED_DIR {
                single_origin_declared_at(&req.config, &reading.at())
            } else {
                merge_shared_into(&mut req, &reading)
                    .unwrap_or_else(|| single_origin_declared_at(&req.config, &reading.at()))
            };
            run_preview(req, declared_at, &columns, records)
        }
        Err(message) => Preview::failed("sample", vec![ApiError::new(message)], vec![]),
    }
}

/// Rewrites `req.config`, a group's own bundle, into that bundle merged with `shared/` at the commit `reading`
/// resolved, and answers where each table in the merge resolves. `None` leaves `req` as it was: a config that does
/// not parse is `run_preview`'s to report.
fn merge_shared_into(req: &mut PreviewRequest, reading: &config_api::Reading) -> Option<config_file::DeclaredAt> {
    let merged = merged_with_shared(&req.config, reading)?;
    // Shared's pipelines come first in the merge, so the default -- the first pipeline -- is pinned to the group's
    // own first before the merge would change which one that is. A group with none previews unmerged, and says so.
    if req.pipeline.is_none() {
        req.pipeline = Some(merged.group.pipelines.first()?.id.clone());
    }
    req.config = merged.config;
    Some(merged.declared_at)
}

/// A group's own bundle merged with `shared/`, for a preview to run as the group runs once deployed.
struct MergedWithShared {
    /// The group's own bundle, as the editor sent it.
    group: config_file::FileConfig,
    /// The merge, as `config_file` JSON.
    config: String,
    /// Where each table in the merge resolves: a shared table under `shared/`, the group's own under its subtree.
    declared_at: config_file::DeclaredAt,
}

/// `config`, a group's own bundle, merged with `shared/` at the commit `reading` resolved -- the merge validation and
/// `run_on_branch` use. `None` when `config` does not parse or `shared/` cannot be read.
fn merged_with_shared(config: &str, reading: &config_api::Reading) -> Option<MergedWithShared> {
    let group = config_file::RawBundle {
        file: serde_json::from_str(config).ok()?,
        unreadable: Vec::new(),
    };
    let shared_reading = reading.sibling(config_file::SHARED_DIR).ok()?;
    let shared = shared_reading.raw();
    let merged = serde_json::to_string(&smeltr::leader::map::merged_file(&shared, &group, reading.group_id()?)).ok()?;
    let declared_at = smeltr::leader::map::merged_declared_at(&shared, &shared_reading.at(), &group, &reading.at());
    Some(MergedWithShared {
        group: group.file,
        config: merged,
        declared_at,
    })
}

// -- lookup file upload ------------------------------------------------
//
// A CSV or MMDB file lands in a directory on this host, where a lookup step can name it.

#[derive(Default, serde::Serialize)]
struct Upload {
    /// The path a lookup table should name, relative to the bundle it is declared in.
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    /// The header row of a CSV; `None` for an MMDB.
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Vec<String>>,
    /// The commit the file landed in, so the editor saving a table that names it works from the same base.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

#[derive(serde::Deserialize)]
struct UploadQuery {
    /// A CSV's delimiter, one ASCII character. Absent means a comma.
    delimiter: Option<String>,
}

async fn upload_lookup(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    axum::extract::Extension(actor): axum::extract::Extension<smeltr::leader::store::Actor>,
    at: config_api::Addressed,
    axum::extract::Query(upload): axum::extract::Query<UploadQuery>,
    mut multipart: Multipart,
) -> Envelope<Upload> {
    let delimiter = match config_file::to_byte(upload.delimiter.as_deref().unwrap_or(","), "delimiter") {
        Ok(delimiter) => delimiter,
        Err(e) => return Envelope::refused(Refusal::invalid(e)),
    };
    // Opened before anything is stored: the store sits inside the repository, which a first save creates.
    let writing = match blocking(move || at.open()).await {
        Ok(writing) => writing,
        Err(e) => return Envelope::refused(e),
    };
    let mut upload: Option<(String, smeltr::lookup::store::Incoming)> = None;
    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return Envelope::refused(Refusal::bad_request(format!("malformed upload: {e}"))),
        };
        let (Some("file"), Some(filename)) = (field.name(), field.file_name().map(str::to_string)) else {
            continue;
        };
        // Streamed into the store as it arrives: a lookup file can be gigabytes, and this process holds a chunk.
        let mut incoming = match smeltr::lookup::store::Incoming::new(writing.repo()) {
            Ok(incoming) => incoming,
            Err(e) => return Envelope::refused(Refusal::internal(format!("couldn't store the upload: {e}"))),
        };
        loop {
            match field.chunk().await {
                Ok(Some(chunk)) => {
                    if let Err(e) = tokio::task::block_in_place(|| std::io::Write::write_all(&mut incoming, &chunk)) {
                        return Envelope::refused(Refusal::internal(format!("couldn't store the upload: {e}")));
                    }
                }
                Ok(None) => break,
                Err(e) => return Envelope::refused(Refusal::bad_request(format!("couldn't read upload: {e}"))),
            }
        }
        upload = Some((filename, incoming));
    }
    let Some((filename, incoming)) = upload else {
        return Envelope::refused(Refusal::bad_request("no `file` field in this upload"));
    };

    // A lookup file is configuration the pipeline's behavior depends on, so its pointer belongs in the tree with the
    // config that names it: a rollback restores the table the previous deploy ran with, and the change is in the
    // review. Which makes it a save like any other -- one commit, refused if the branch has moved.
    let saved = blocking_answer(move || save_lookup_file(&writing, &filename, incoming, delimiter, &actor)).await;
    smeltr::leader::prune_lookups(&state).await;
    saved
}

/// Name an upload in the store and commit its pointer into the lookup directory `writing` names. A CSV's headers are
/// read with `delimiter`. Blocking.
fn save_lookup_file(
    writing: &config_api::Writing,
    filename: &str,
    incoming: smeltr::lookup::store::Incoming,
    delimiter: u8,
    author: &smeltr::leader::store::Actor,
) -> Envelope<Upload> {
    let stored = match config_api::store_lookup_file(
        writing.repo(),
        writing.prefix(),
        filename,
        incoming,
        delimiter,
        config_api::Naming::ByContent,
    ) {
        Ok(stored) => stored,
        Err(e) => return Envelope::refused(e),
    };
    match writing.commit(
        vec![stored.edit],
        &format!("upload lookup file {}", stored.path),
        author,
    ) {
        Ok(commit) => Envelope::ok(Upload {
            path: Some(stored.path),
            headers: stored.headers,
            commit: Some(commit),
        }),
        Err(e) => {
            let refusal = Refusal::from(e);
            let message = format!("could not commit the upload: {}", refusal.message());
            Envelope::refused(Refusal::new(refusal.status, message))
        }
    }
}

// -- leader tier -------------------------------------------------------

/// The name the worker inside this process calls its leader by, where this process runs one.
///
/// A certificate that does not serve this name is one that leaves that worker unable to reach the leader hosting
/// it, however well it serves every browser. `None` when no worker runs here, or when the operator pointed it at
/// a leader elsewhere -- this leader's certificate says nothing about how a worker reaches that one.
///
/// An address written out is read rather than taken as elsewhere: a `leader_url` naming this leader's own port, on
/// loopback or by a name of `origin_hosts`, is the same leader, spelled out, and a certificate that does not serve the
/// name in it cuts this worker off exactly as one that does not serve `127.0.0.1` would.
fn embedded_worker_reaches_us_at(origin_hosts: &[String]) -> Option<String> {
    if !smeltr::startup_config::worker_enabled() {
        return None;
    }
    match smeltr::startup_config::leader_url() {
        Some(url) => smeltr::startup_config::names_this_leader(
            &url,
            smeltr::startup_config::gui_port(),
            &smeltr::startup_config::leader_certificate_names(origin_hosts),
        )
        .then(|| smeltr::startup_config::leader_url_host(&url))
        .flatten(),
        None => Some(reached_at(&smeltr::startup_config::gui_bind()).to_string()),
    }
}

/// The address the worker inside this process reaches its leader at when no `leader_url` names one: the one `bind`
/// listens on, or loopback of the same family when it listens on every address. A bind that does not parse never
/// gets this far, since `run_leader` refuses it first.
fn reached_at(bind: &str) -> std::net::IpAddr {
    match bind.parse() {
        Ok(std::net::IpAddr::V6(v6)) if v6.is_unspecified() => std::net::Ipv6Addr::LOCALHOST.into(),
        Ok(ip) if ip.is_unspecified() => std::net::Ipv4Addr::LOCALHOST.into(),
        Ok(ip) => ip,
        Err(_) => std::net::Ipv4Addr::LOCALHOST.into(),
    }
}

/// The certificate the leader serves, and the authority a worker verifies it against (`leader::tls`). TLS off
/// serves plain HTTP for a deployment that terminates TLS in front of this process.
async fn leader_tls_config(
    data_dir: &std::path::Path,
    origin_hosts: &[String],
) -> Option<(std::sync::Arc<smeltr::leader::resolver::Resolver>, Vec<u8>)> {
    if !smeltr::startup_config::leader_tls_enabled() {
        return None;
    }

    let tls_dir = data_dir.join("tls");
    let materials = smeltr::leader::tls::ensure(
        &tls_dir,
        smeltr::startup_config::leader_certificate_names(origin_hosts),
        smeltr::startup_config::leader_ca_validity(),
        &embedded_worker_reaches_us_at(origin_hosts)
            .into_iter()
            .collect::<Vec<_>>(),
    )
    .unwrap_or_else(|e| {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    });

    if materials.authority_created {
        tracing::info!(
            target: "leader.tls_authority_created",
            path = %tls_dir.join(smeltr::leader::tls::CA_CERT).display(),
            "created this leader's certificate authority: copy this file into every worker's bootstrap config, \
             and trust it in any browser that opens this leader. It is what says a leader really is this one; \
             the certificate issued under it is the entry \"leader\" in the certificate list"
        );
    }

    // The state of the disk this socket is built from, read before it is built and handed over after. `ensure`
    // above replaced what is served, so until this says so the two have parted -- which at a start is exactly
    // true, there being no socket at all until the line between these two.
    let generation = smeltr::leader::tls::disk_generation(&tls_dir);
    let resolver = smeltr::leader::tls::to_serve(&tls_dir)
        .and_then(|(_, chain, key)| smeltr::leader::resolver::Resolver::load(&chain, &key))
        .unwrap_or_else(|e| {
            eprintln!("smeltr: refusing to start -- {e}");
            std::process::exit(1);
        });
    // Where the entry named could not be served, the socket presents the leader's own and the two stay parted.
    if smeltr::leader::tls::unserved(&tls_dir).is_none() {
        smeltr::leader::tls::socket_took(&tls_dir, generation);
    }
    Some((resolver, materials.ca_pem))
}

/// The leader's authority certificate, where a worker was given one out of band. `None` leaves the platform's own
/// roots in place, which is what a leader on plain HTTP or behind a proxy needs.
fn read_leader_ca() -> Option<Vec<u8>> {
    smeltr::startup_config::leader_ca_path()
        .map(|path| std::fs::read(&path).unwrap_or_else(|e| panic!("reading SMELTR_LEADER_CA ({path}): {e}")))
}

/// Runs this leader's own worker on its own thread, talking to a leader over the same API an external worker uses.
/// Only the enrollment code is minted directly.
fn spawn_embedded_worker(
    store: std::sync::Arc<dyn smeltr::leader::store::Store>,
    data_dir: &std::path::Path,
    leader_url: String,
    leader_ca_pem: Option<Vec<u8>>,
    leader_authority_dir: Option<std::path::PathBuf>,
) {
    let group_id = smeltr::startup_config::worker_group();

    // A group id no worker could use is one this worker could never run: refused before it ever tries to enroll.
    if let Err(e) = smeltr::leader::git::group_subtree(&group_id) {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }

    // Under the leader's own data directory: this worker's id and credential are the leader's to keep.
    let state_dir = data_dir.join("worker");

    // Only when there is nothing to enroll with. Minting on every start would leave a live enrollment code behind at
    // each one, outstanding on the Workers page and never spendable by anybody.
    let code = smeltr::worker::needs_an_enrollment_code(&state_dir)
        .then(|| mint_embedded_enrollment_code(&store, &group_id, &smeltr::leader::tls::dir_of(data_dir)));

    std::thread::spawn(move || {
        start_worker(
            leader_url,
            leader_ca_pem,
            leader_authority_dir,
            group_id,
            state_dir,
            code,
            true,
        )
    });
}

/// An enrollment code for the worker inside this process.
///
/// Names the authority this leader signs under, like every other enrollment code. The worker inside this process is
/// handed that authority out of band and never reads the field -- but the leader does, everywhere it decides whether an
/// enrollment code still brings a worker in, and an enrollment code naming nothing is one it would have to make an
/// exception for.
fn mint_embedded_enrollment_code(
    store: &std::sync::Arc<dyn smeltr::leader::store::Store>,
    group_id: &str,
    tls_dir: &std::path::Path,
) -> String {
    let fingerprint = smeltr::leader::enrollment_codes::Presenting::read(tls_dir)
        .mints_against()
        .to_string();
    let secret = smeltr::leader::auth::generate_secret();
    let now_ms = smeltr::leader::warnings::now_millis();
    store
        .create_enrollment_code(&smeltr::leader::store::NewEnrollmentCode {
            id: format!("ec_embedded_{now_ms}"),
            code_hash: smeltr::leader::auth::hash_secret(&secret),
            fingerprint: fingerprint.clone(),
            group_id: group_id.to_string(),
            minted_by: smeltr::leader::store::Actor::SYSTEM_ID.to_string(),
            minted_at: now_ms,
            // It is spent within the second, on this thread. A window is what an operator needs to walk to a
            // machine, and there is no walk.
            expires_at: Some(now_ms + 60 * 60 * 1000),
            max_uses: Some(1),
            // It lives in the leader's own data directory, so a restart brings it back as itself.
            kind: smeltr::leader::store::WorkerKind::Permanent,
        })
        .expect("mint an enrollment code for this leader's own worker");
    smeltr::enrollment_code::EnrollmentCode {
        authority: match fingerprint.as_str() {
            "" => smeltr::enrollment_code::Authority::System,
            fingerprint => smeltr::enrollment_code::Authority::Pinned(fingerprint.to_string()),
        },
        secret,
    }
    .to_string()
}

/// Runs this process's worker and never returns. Both ways in end here; what differs is only where the group, the
/// state directory and the first credential come from.
///
/// Every refusal is made before the line that says a worker is starting, so a rejected setting never reads as a crash
/// -- which is what lets the embedded caller run this on a thread of its own.
fn start_worker(
    leader_url: String,
    leader_ca_pem: Option<Vec<u8>>,
    leader_authority_dir: Option<std::path::PathBuf>,
    group_id: String,
    state_dir: std::path::PathBuf,
    enrollment_code: Option<String>,
    in_leader: bool,
) -> ! {
    use smeltr::startup_config as cfg;

    // The check `content_id` and `load_group_dir` make of every commit, made once here instead: an id they refuse
    // leaves this worker with nothing it could ever run.
    if let Err(e) = smeltr::config_file::group_subtree(&group_id) {
        // Printed rather than recorded, here and at every other `refusing to start`: a path about to exit has
        // nothing to gain from a buffer it will not outlive (design doc, Observability).
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }
    // After the first run the credential under `state_dir` is what authenticates this worker, and a value left
    // behind in a unit file -- redacted, rotated, or minted by a leader of a newer age -- is not a reason to refuse
    // to start something already enrolled. It is kept, unread, for the worker to enroll again with if its leader
    // ever forgets it.
    let needs_an_enrollment_code = smeltr::worker::needs_an_enrollment_code(&state_dir);
    if enrollment_code.is_none() && needs_an_enrollment_code {
        eprintln!(
            "smeltr: refusing to start -- this worker has never enrolled ({} holds no \
             credential) and no enrollment code is configured. Mint one on the leader's Workers page \
             and set SMELTR_WORKER_ENROLLMENT_CODE, or `[worker] enrollment_code` in smeltr.toml. It is needed \
             only on this worker's first run.",
            state_dir.display()
        );
        std::process::exit(1);
    }
    // An enrollment code that cannot be read is refused here rather than at the moment it would be spent, so a worker
    // never starts on a value that was never going to work.
    if let Some(Err(e)) = enrollment_code
        .as_ref()
        .filter(|_| needs_an_enrollment_code)
        .map(|value| value.parse::<smeltr::enrollment_code::EnrollmentCode>())
    {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }
    // Every setting here can refuse, so all are read before anything says a worker is starting.
    cfg::worker_listen_address();
    let thread_limit = cfg::worker_thread_limit();
    let memory = smeltr::worker::memory_to_fit();
    let heartbeat_interval = cfg::worker_heartbeat_interval();
    let stop_after_unreachable_secs = cfg::worker_stop_after_unreachable_secs();
    let leader_trust_public = cfg::leader_trust_public();
    let metrics_endpoint = smeltr::worker::metrics_endpoint::open().unwrap_or_else(|e| {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    });

    let describe = if in_leader { "an embedded worker" } else { "worker" };
    tracing::info!(target: "worker.starting", role = %describe, group = %group_id, "starting a worker");
    smeltr::worker::run_worker(smeltr::worker::WorkerConfig {
        leader_url,
        leader_ca_pem,
        leader_trust_public,
        leader_authority_dir,
        group_id,
        thread_limit,
        memory,
        state_dir,
        enrollment_code,
        heartbeat_interval,
        stop_after_unreachable_secs,
        in_leader,
        metrics_endpoint,
    });
    smeltr::logs::stop();
    std::process::exit(0);
}

/// The leader's routes and TLS config.
struct LeaderSetup {
    routes: Router,
    tls: Option<(std::sync::Arc<smeltr::leader::resolver::Resolver>, Vec<u8>)>,
    /// What the certificates page needs to finish a rotation while somebody watches it. The heartbeat is where
    /// the evidence arrives; this is where a leader with nothing beating still has a reason to look.
    leader: smeltr::leader::AppState,
}

async fn setup_leader(
    port: u16,
    auth: gate::Auth,
    store: std::sync::Arc<dyn smeltr::leader::store::Store>,
    registry: smeltr::leader::registry::Registry,
) -> LeaderSetup {
    use std::sync::Arc;

    let data_dir = smeltr::startup_config::leader_data_dir();
    std::fs::create_dir_all(&data_dir).expect("create leader data directory");
    // Absolute: `leader::git` runs from a different cwd. Portable: git refuses a Windows verbatim path.
    let data_dir =
        smeltr::paths::canonicalize_portable(&data_dir).expect("resolve leader data directory to an absolute path");

    // Before the first sync: gix's curl reads the roots its fetch from the remote trusts only from this file.
    smeltr::io::trust::write_pem_file(
        smeltr::io::trust::public(),
        &smeltr::leader::remote::roots_path(&data_dir),
    )
    .expect("write the roots the remote's fetch trusts");

    // Before TLS: a certificate the leader serves from the list is read from it.
    smeltr::leader::tls::read_list(&smeltr::leader::tls::dir_of(&data_dir), store.clone());
    let hosts = origin_hosts(&auth.origin);
    let tls = leader_tls_config(&data_dir, &hosts).await;
    let state = smeltr::leader::AppState {
        store: store.clone(),
        registry: Arc::new(registry),
        captures: Arc::new(smeltr::leader::capture::Captures::default()),
        data_dir: data_dir.clone(),
        entitlements: Arc::new(smeltr::leader::Entitlements::default()),
        verify_limiter: Arc::new(smeltr::leader::VerifyLimiter::default()),
        expiries: Default::default(),
        throughput: Default::default(),
        deploy_checks: Default::default(),
        origin_hosts: hosts,
        remote: Default::default(),
        updates: Default::default(),
        directives: Default::default(),
    };
    state.deploy_checks.set_runner(Arc::new(test_api::failing_at));
    tokio::spawn(smeltr::leader::remote::poll(state.clone()));
    tokio::spawn(smeltr::leader::updates::poll(state.clone()));
    tokio::spawn(smeltr::leader::remote::push_saves(state.clone()));
    // Without a worker in this process, what the leader's threads did not raise is the leader's too.
    if !smeltr::startup_config::worker_enabled() {
        smeltr::logs::runs_no_worker();
    }
    tokio::spawn(smeltr::leader::keep_own_logs(state.clone()));

    // A cutover changes the files and then withdraws the enrollment codes that named the root it retired. A process
    // that stopped between the two came back to a finished cutover with no rotation left to notice it, so the same
    // withdrawal runs here -- where it is the whole answer rather than the second half of one.
    if tls.is_some() {
        smeltr::leader::enrollment_codes::withdraw_off_authority(&state, &smeltr::leader::store::Actor::system()).await;
    }

    if smeltr::startup_config::worker_enabled() {
        // Its own address unless the operator named another leader -- and the authority has to follow the address,
        // since this leader's own authority says nothing about somebody else's. An address written out that names
        // this leader's own port, on loopback or by a name its certificate serves, is this leader, and gets the trust
        // of leaving it unset rather than the trust of somewhere else.
        let configured = smeltr::startup_config::leader_url();
        let names = smeltr::startup_config::leader_certificate_names(&state.origin_hosts);
        let here = configured
            .as_deref()
            .is_none_or(|url| smeltr::startup_config::names_this_leader(url, port, &names));
        let scheme = if tls.is_some() { "https" } else { "http" };
        let leader_url = configured.unwrap_or_else(|| {
            let at = std::net::SocketAddr::new(reached_at(&smeltr::startup_config::gui_bind()), port);
            format!("{scheme}://{at}")
        });
        // An operator who named an authority file manages it themselves and it is never quietly replaced, so
        // following this leader's own replacements is for the worker that was given nothing.
        let (leader_ca_pem, authority_dir) = match (here, read_leader_ca()) {
            (_, Some(named)) => (Some(named), None),
            // This worker's leader is this process, so the certificate it verifies against can be replaced under
            // it. Where that leader keeps it is what lets the worker read the new one for itself.
            (true, None) => (
                tls.as_ref().map(|(_, ca_pem)| ca_pem.clone()),
                tls.is_some().then(|| smeltr::leader::tls::dir_of(&data_dir)),
            ),
            (false, None) => (None, None),
        };
        spawn_embedded_worker(store, &data_dir, leader_url, leader_ca_pem, authority_dir);
    }

    let admin_and_cluster = smeltr::leader::admin_router()
        .route("/api/cluster", get(cluster::cluster_status))
        .route("/api/map", get(get_map))
        .route("/api/map/measurements", get(get_map_measurements))
        .route("/api/areas/:area/stats", get(get_stats))
        .route("/api/branches/:name/deploy", post(branch_api::deploy_change))
        .route("/api/main/deploy", post(branch_api::deploy_commit))
        // Here for `State<AppState>`: the listing says what of each branch the remote holds.
        .route(
            "/api/branches",
            get(branch_api::list_branches).post(branch_api::create_branch),
        )
        .route("/api/preview/sql", post(sql_api::sql_statement))
        .route("/api/captures", post(capture_api::start_capture))
        .route(
            "/api/captures/:session",
            get(capture_api::capture_status).delete(capture_api::discard_capture),
        )
        .route("/api/captures/:session/stop", post(capture_api::stop_capture))
        // Not on `app` below with the rest of the worker-groups routes: both need `State<AppState>` -- the listing
        // for `shared`'s hue, the create for a new group's -- the same reason `get_map` is here. `deploy_change`
        // asks the store whether a remote owns `main`.
        .route(
            "/api/worker-groups",
            get(worker_groups_api::list_worker_groups).post(worker_groups_api::create_worker_group),
        )
        .route("/api/worker-groups/:id/hue", put(worker_groups_api::set_group_hue))
        // Not on `app` below with the rest of the config routes: saving a connector asks the certificate list whether
        // it holds the certificate named.
        .route("/api/areas/:area/sources", put(config_api::put_sources))
        .route("/api/areas/:area/destinations", put(config_api::put_destinations))
        // Not on `app` below with the rest of the samples routes: the capture a sample is made from is held in
        // `State<AppState>`.
        .route("/api/areas/:area/samples", post(capture_api::save_capture))
        // Not on `app` below with the rest of the config routes: retention after an upload keeps what the fleet runs,
        // which the registry knows. A lookup file is streamed to disk, so the limit is what a disk takes rather than
        // what memory holds.
        .route(
            "/api/areas/:area/lookups/upload",
            post(upload_lookup).layer(axum::extract::DefaultBodyLimit::max(
                smeltr::lookup::store::MAX_FILE_BYTES as usize,
            )),
        )
        .route_layer(axum::middleware::from_fn_with_state(auth, gate::require_principal));
    let routes = smeltr::leader::compose(state.clone(), admin_and_cluster);

    LeaderSetup {
        routes,
        tls,
        leader: state,
    }
}

// -- startup config file ---------------------------------------------------
//
// `smeltr.toml` (or `SMELTR_CONFIG_FILE`), optional. It backfills env
// vars that are not already set (`startup_config`). Every section is
// applied; the role decides which does anything. `smeltr.example.toml`
// documents the fields.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    log_level: Option<String>,
    #[serde(default)]
    leader: LeaderFileConfig,
    #[serde(default)]
    worker: WorkerFileConfig,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerFileConfig {
    /// Whether this process runs a worker at all. Everything below is read only when it does.
    enabled: Option<bool>,
    /// The group this worker serves. Required whenever it is enabled.
    group: Option<String>,
    /// The availability zone this worker runs in.
    zone: Option<String>,
    /// The address every receiver this worker runs listens on.
    listen_address: Option<String>,
    /// The enrollment code a worker spends for a credential on its first run.
    enrollment_code: Option<String>,
    #[serde(default)]
    leader: WorkerLeaderFileConfig,
    #[serde(default)]
    storage: WorkerStorageFileConfig,
    #[serde(default)]
    runtime: WorkerRuntimeFileConfig,
    #[serde(default)]
    heartbeat: WorkerHeartbeatFileConfig,
    #[serde(default)]
    metrics_endpoint: WorkerMetricsEndpointFileConfig,
    /// Read and ignored, so an upgrade does not stop a worker over a line it no longer needs.
    enroll_code: Option<String>,
}

/// The leader this worker reports to. Required unless `[leader] enabled`, whose address a worker takes by default.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerLeaderFileConfig {
    url: Option<String>,
    ca: Option<String>,
    trust_public: Option<bool>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerStorageFileConfig {
    state_dir: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRuntimeFileConfig {
    thread_limit: Option<usize>,
    memory: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerHeartbeatFileConfig {
    interval_ms: Option<u64>,
    // -1 is never, as the example file shows; `startup_config` refuses other negatives.
    stop_after_unreachable_secs: Option<i64>,
}

/// The port a worker serves its metrics on for Prometheus. See `startup_config::worker_metrics_endpoint_enabled`.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerMetricsEndpointFileConfig {
    enabled: Option<bool>,
    port: Option<u16>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    client_ca: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderFileConfig {
    /// Whether this process runs a leader at all. Everything below is read only when it does.
    enabled: Option<bool>,
    /// Whether this start regenerates the built-in administrator's password. See `startup_config::recover_admin`.
    recover_admin: Option<bool>,
    #[serde(default)]
    http: LeaderHttpFileConfig,
    #[serde(default)]
    tls: LeaderTlsFileConfig,
    #[serde(default)]
    storage: LeaderStorageFileConfig,
    #[serde(default)]
    secrets: LeaderSecretsFileConfig,
    #[serde(default)]
    lookups: LeaderLookupsFileConfig,
}

/// The port the leader's GUI and both APIs listen on, and the address a browser reaches it under.
#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderHttpFileConfig {
    port: Option<u16>,
    bind: Option<String>,
    public_origin: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderTlsFileConfig {
    enabled: Option<bool>,
    #[serde(default)]
    extra_san: Vec<String>,
    /// How long the certificate authority created on this leader's first start lives, written the way an operator
    /// writes a duration. See `leader::tls`.
    ca_validity: Option<String>,
    acme_http_port: Option<u16>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderStorageFileConfig {
    data_dir: Option<String>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderSecretsFileConfig {
    key: Option<String>,
    versions: Option<i64>,
}

#[derive(Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderLookupsFileConfig {
    versions: Option<i64>,
}

/// The listen address. Pure, so the decision is testable without a socket.
///
/// Every bind is allowed now. What used to make a non-loopback one dangerous was a config API that answered
/// anonymously without a token; every operator route requires a session, whatever this binds.
fn resolve_listen_addr(bind: &str, port: u16) -> Result<std::net::SocketAddr, String> {
    let ip: std::net::IpAddr = bind
        .parse()
        .map_err(|_| format!("SMELTR_GUI_BIND: {bind:?} is not a valid IP address"))?;
    Ok(std::net::SocketAddr::new(ip, port))
}

/// The session deadlines this leader starts with: whatever the store holds, or the built-in defaults for a
/// leader that has never had them set. Admin -> System -> Sessions (or `PUT /api/settings/sessions`) is the only
/// way to change them.
fn resolve_lifetimes(store: &dyn smeltr::leader::store::Store) -> sessions::Lifetimes {
    match sessions::Lifetimes::read(store) {
        Ok(lifetimes) => lifetimes,
        Err(e) => {
            // Carrying on with the defaults beats refusing to start, which would be a leader nobody can reach in
            // order to fix it.
            eprintln!("smeltr: {e}. Using the defaults until it is corrected.");
            sessions::Lifetimes::default()
        }
    }
}

/// The origin a browser reaches this leader at.
///
/// Configured, or derived from the TLS setting and the bind address. The derivation is right for a laptop and for
/// a leader serving its own certificate, and cannot be right for one behind a proxy: the proxy's name is not
/// something this process can observe. So a deployment that terminates TLS elsewhere has to say, and one that
/// would otherwise be told to send cookies over plain HTTP across a network is refused rather than guessed at.
///
/// A leader whose bind reaches loopback is also reached at `localhost` and `127.0.0.1` on its port -- the names
/// its certificate always carries, for the worker inside the process -- and a browser on its machine is as likely
/// to be pointed at one as the other. It accepts both as aliases when the origin is derived, since nothing here
/// says which name to expect, and when it is configured and serves its own TLS, since a browser reaching it
/// directly is its own page under either name. Every configured origin must be https then, as a browser reaching
/// it directly has no other scheme to use. A configured origin with TLS off accepts exactly what it lists: its
/// cookie is Secure when the origins are https, so a plain loopback alias would carry nothing. A bind that does
/// not reach loopback gets no alias, because no browser reaches it there.
///
/// Pure, for the same reason as above.
fn resolve_origin(configured: Option<&str>, bind: &str, port: u16, tls: bool) -> Result<gate::Origin, String> {
    let ip: std::net::IpAddr = bind
        .parse()
        .map_err(|_| format!("SMELTR_GUI_BIND: {bind:?} is not a valid IP address"))?;
    let mut origin = match configured {
        Some(configured) => configured_origin(configured, tls)?,
        None => derived_origin(ip, port, tls)?,
    };
    let reaches_loopback = ip.is_loopback() || ip.is_unspecified();
    if reaches_loopback && (configured.is_none() || tls) {
        let scheme = if origin.secure { "https" } else { "http" };
        for host in smeltr::startup_config::LOOPBACK_NAMES {
            let alias = origin_url(scheme, host, port);
            if !origin.accepts(&alias) {
                origin.aliases.push(alias);
            }
        }
    }
    Ok(origin)
}

/// The origins `SMELTR_PUBLIC_ORIGIN` lists: the first is the value, the rest its aliases, all one scheme.
fn configured_origin(configured: &str, tls: bool) -> Result<gate::Origin, String> {
    let mut origins = configured.split(',').map(str::trim);
    let Some(first) = origins.next().filter(|s| !s.is_empty()) else {
        return Err("SMELTR_PUBLIC_ORIGIN is set but names no origin.".to_string());
    };
    let (value, secure) = parse_configured_origin(first)?;
    let mut aliases = Vec::new();
    for entry in origins {
        if entry.is_empty() {
            continue;
        }
        let (alias, alias_secure) = parse_configured_origin(entry)?;
        if alias_secure != secure {
            return Err(format!(
                "SMELTR_PUBLIC_ORIGIN: {first:?} and {entry:?} disagree on http vs https. Every origin listed \
                 for one leader shares one answer to whether its cookie is encrypted in transit."
            ));
        }
        aliases.push(alias);
    }
    // This leader's own socket speaks TLS or it does not; a browser reaching it directly has no third
    // option. A plain-HTTP origin here is not the proxy case -- that leader runs with SMELTR_LEADER_TLS off
    // -- it is a configuration that named the wrong scheme for what this process actually serves.
    if tls && !secure {
        return Err(format!(
            "SMELTR_PUBLIC_ORIGIN: {first:?} is plain HTTP, but this leader serves its own HTTPS \
             (SMELTR_LEADER_TLS is on). A browser reaches it at https, not http -- turn SMELTR_LEADER_TLS off \
             if a proxy in front terminates it instead."
        ));
    }
    // A configured https origin on a leader serving plain HTTP is one a proxy terminates TLS for.
    let proxied = secure && !tls;
    Ok(gate::Origin {
        value,
        aliases,
        secure,
        proxied,
    })
}

/// The origin the bind address and TLS setting imply, with no `SMELTR_PUBLIC_ORIGIN` to say otherwise.
fn derived_origin(ip: std::net::IpAddr, port: u16, tls: bool) -> Result<gate::Origin, String> {
    if !tls && !ip.is_loopback() {
        return Err(format!(
            "SMELTR_LEADER_TLS is off and this leader binds {ip}, so it cannot know what origin a browser \
             reaches it at and cannot promise a session cookie is encrypted in transit. Set \
             SMELTR_PUBLIC_ORIGIN to the https origin your proxy serves, or leave TLS on."
        ));
    }
    let scheme = if tls { "https" } else { "http" };
    // A wildcard bind is an address to listen on, never one to be reached at.
    let host = match ip {
        _ if ip.is_unspecified() => "127.0.0.1".to_string(),
        std::net::IpAddr::V6(v6) => format!("[{v6}]"),
        v4 => v4.to_string(),
    };
    Ok(gate::Origin {
        value: origin_url(scheme, &host, port),
        aliases: Vec::new(),
        secure: tls,
        proxied: false,
    })
}

/// `scheme://host:port`, except a port a browser's own `Origin` header omits because it is the scheme's default
/// -- 80 for `http`, 443 for `https`. Serialized any other way, this origin would never match one a browser
/// actually sends.
fn origin_url(scheme: &str, host: &str, port: u16) -> String {
    match (scheme, port) {
        ("http", 80) | ("https", 443) => format!("{scheme}://{host}"),
        _ => format!("{scheme}://{host}:{port}"),
    }
}

/// The bare host in a `scheme://host[:port]` origin, discarding both -- what a certificate's SAN list needs,
/// which cares about neither.
fn origin_host(origin: &str) -> &str {
    host_of(origin.split_once("://").map_or(origin, |(_, rest)| rest))
}

/// Every host `origin` reaches this leader at. The same hosts a browser's `Origin` header is checked against, so
/// also every host this leader's own certificate has to carry, when it signs one for itself.
fn origin_hosts(origin: &gate::Origin) -> Vec<String> {
    std::iter::once(origin_host(&origin.value))
        .chain(origin.aliases.iter().map(|alias| origin_host(alias)))
        .map(str::to_string)
        .collect()
}

/// One origin out of a (possibly comma-separated) `SMELTR_PUBLIC_ORIGIN`: canonicalized the way a browser would
/// serialize it, and whether it is secure. Refuses anything that is not a bare scheme, host and port, and any
/// plain-HTTP origin that does not name this machine -- a session cookie sent under it would cross a network
/// unencrypted.
fn parse_configured_origin(origin: &str) -> Result<(String, bool), String> {
    // One trailing slash is how people write an origin and means nothing; anything further is a path, and a
    // browser would never send it, so the value could never match what it is compared against.
    let origin = origin.trim_end_matches('/');
    // An origin is a scheme, a host and optionally a port, and nothing else. A browser sends exactly that, so a
    // configured value carrying a path or a query would never equal the header and would leave every login
    // refused -- which is worse than refusing the configuration that caused it.
    let secure = match origin.split_once("://") {
        Some(("https", rest)) if is_authority(rest) => true,
        Some(("http", rest)) if is_authority(rest) => false,
        _ => {
            return Err(format!(
                "SMELTR_PUBLIC_ORIGIN: {origin:?} is not an origin. It is a scheme, a host and optionally a \
                 port, with no path, query or fragment -- \"https://leader.example.com\"."
            ));
        }
    };
    let authority = origin.split_once("://").expect("split above").1;
    // A plain-HTTP origin sends the session cookie in the clear, so it is allowed only where "the network" is one
    // machine. Anything else is a deployment that has not been told about its proxy.
    if !secure && !is_local_host(host_of(authority)) {
        return Err(format!(
            "SMELTR_PUBLIC_ORIGIN: {origin:?} is plain HTTP on a host that is not this one, so a session cookie \
             would cross a network unencrypted. Serve HTTPS, or terminate TLS in front and name that https \
             origin here."
        ));
    }
    let scheme = if secure { "https" } else { "http" };
    let default_port = if secure { 443 } else { 80 };
    // A configured origin naming the scheme's own default port is written out in full, but a browser's `Origin`
    // never carries one -- so it is canonicalized the same way a derived origin already is, or it would never
    // match what is compared against it.
    let (host, port) = match port_of(authority) {
        Some(port) => (authority.strip_suffix(&format!(":{port}")).unwrap_or(authority), port),
        None => (authority, default_port),
    };
    Ok((origin_url(scheme, host, port), secure))
}

/// The explicit port in an authority, if it names one. An IPv6 literal is bracketed, so the port is whatever
/// follows the closing bracket rather than the authority's first colon.
fn port_of(authority: &str) -> Option<u16> {
    let tail = match authority.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map(|(_, tail)| tail)?,
        None => return authority.split_once(':').and_then(|(_, port)| port.parse().ok()),
    };
    tail.strip_prefix(':').and_then(|p| p.parse().ok())
}

/// Whether `rest` is a bare authority: a host, optionally a port, and nothing after it.
fn is_authority(rest: &str) -> bool {
    !rest.is_empty() && !rest.contains(['/', '?', '#', '@', ' ']) && !host_of(rest).is_empty()
}

/// The host in an authority, without its port.
///
/// An IPv6 literal is bracketed and full of colons, so splitting on the first one answers `"["`. The brackets are
/// what say where the address ends.
fn host_of(authority: &str) -> &str {
    match authority.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map(|(host, _)| host).unwrap_or(rest),
        None => authority.split(':').next().unwrap_or(authority),
    }
}

/// Whether a host names this machine. What decides if a plain-HTTP origin is a development convenience or a
/// session cookie crossing a network in the clear.
fn is_local_host(host: &str) -> bool {
    host == "localhost" || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The public routes: signing in, signing out, and asking who you are. Never behind the gate -- a person with no
/// session has to be able to reach them -- so each checks for itself what it needs.
fn auth_router(auth: gate::Auth) -> Router {
    Router::new()
        .route("/api/auth/session", get(auth_api::session).delete(auth_api::logout))
        .route("/api/auth/login", post(auth_api::login))
        .route("/api/auth/session/activity", post(auth_api::activity))
        .route("/api/auth/password", put(auth_api::password))
        .route("/api/auth/oidc", get(oidc_api::offered))
        .route(oidc_api::START_PATH, get(oidc_api::start))
        .route(oidc_api::CALLBACK_PATH, get(oidc_api::callback))
        .with_state(auth)
}

/// An API path nothing serves. JSON, so a caller parses a refusal rather than the SPA's HTML.
async fn unknown_api(uri: axum::http::Uri) -> axum::response::Response {
    gate::refuse(
        axum::http::StatusCode::NOT_FOUND,
        &format!("no API route at {}", uri.path()),
    )
}

/// Drop expired sessions on a schedule, so a leader nobody visits does not hold one per sign-in since it started.
///
/// Expiry is already enforced when a session is resolved; this is only about the memory. Hence the long period:
/// nothing is waiting on it.
async fn sweep_sessions(auth: gate::Auth) {
    const EVERY: std::time::Duration = std::time::Duration::from_secs(60);
    loop {
        tokio::time::sleep(EVERY).await;
        let dropped = auth.sessions.sweep(std::time::Instant::now());
        if dropped > 0 {
            tracing::debug!(target: "leader.sessions_swept", dropped, "expired sessions were dropped");
        }
    }
}

/// Refresh OIDC sessions at their provider as each comes due, so a group change there reaches the gate within an
/// interval and a grant the provider has withdrawn ends its session.
async fn refresh_oidc_sessions(auth: gate::Auth) {
    let every = smeltr::startup_config::oidc_refresh_interval();
    // Looked at more often than a grant comes due, so none waits much past its interval.
    let tick = every
        .min(std::time::Duration::from_secs(60))
        .max(std::time::Duration::from_millis(100));
    loop {
        tokio::time::sleep(tick).await;
        oidc_api::refresh_due(&auth, every).await;
    }
}

fn load_and_apply_config_file() {
    use smeltr::startup_config::{backfill_env, find_config_file, parse_config_file};

    let file: FileConfig = match find_config_file("smeltr.toml") {
        Some(path) => parse_config_file(&path),
        None => FileConfig::default(),
    };

    backfill_env("SMELTR_LOG_LEVEL", file.log_level);

    backfill_env("SMELTR_LEADER_ENABLED", file.leader.enabled);
    backfill_env("SMELTR_GUI_PORT", file.leader.http.port);
    backfill_env("SMELTR_GUI_BIND", file.leader.http.bind);
    backfill_env("SMELTR_PUBLIC_ORIGIN", file.leader.http.public_origin);
    backfill_env("SMELTR_LEADER_DATA_DIR", file.leader.storage.data_dir);
    backfill_env("SMELTR_LEADER_SECRET_KEY", file.leader.secrets.key);
    backfill_env("SMELTR_LEADER_SECRET_VERSIONS", file.leader.secrets.versions);
    backfill_env("SMELTR_LEADER_LOOKUP_VERSIONS", file.leader.lookups.versions);
    backfill_env("SMELTR_LEADER_TLS", file.leader.tls.enabled);
    backfill_env("SMELTR_LEADER_CA_VALIDITY", file.leader.tls.ca_validity);
    backfill_env("SMELTR_ACME_HTTP_PORT", file.leader.tls.acme_http_port);
    if !file.leader.tls.extra_san.is_empty() {
        backfill_env("SMELTR_LEADER_TLS_EXTRA_SAN", Some(file.leader.tls.extra_san.join(",")));
    }
    backfill_env("SMELTR_RECOVER_ADMIN", file.leader.recover_admin);

    backfill_env("SMELTR_WORKER_ENABLED", file.worker.enabled);
    backfill_env("SMELTR_WORKER_GROUP", file.worker.group);
    backfill_env("SMELTR_WORKER_ZONE", file.worker.zone);
    backfill_env("SMELTR_WORKER_LISTEN_ADDRESS", file.worker.listen_address);
    backfill_env("SMELTR_LEADER_URL", file.worker.leader.url);
    backfill_env("SMELTR_LEADER_CA", file.worker.leader.ca);
    backfill_env("SMELTR_LEADER_TRUST_PUBLIC", file.worker.leader.trust_public);
    backfill_env("SMELTR_WORKER_STATE_DIR", file.worker.storage.state_dir);
    backfill_env("SMELTR_WORKER_THREAD_LIMIT", file.worker.runtime.thread_limit);
    backfill_env("SMELTR_WORKER_MEMORY", file.worker.runtime.memory);
    backfill_env("SMELTR_WORKER_HEARTBEAT_INTERVAL_MS", file.worker.heartbeat.interval_ms);
    backfill_env(
        "SMELTR_WORKER_STOP_AFTER_UNREACHABLE_SECS",
        file.worker.heartbeat.stop_after_unreachable_secs,
    );
    backfill_env("SMELTR_WORKER_ENROLLMENT_CODE", file.worker.enrollment_code);
    backfill_env(
        "SMELTR_WORKER_METRICS_ENDPOINT_ENABLED",
        file.worker.metrics_endpoint.enabled,
    );
    backfill_env("SMELTR_WORKER_METRICS_ENDPOINT_PORT", file.worker.metrics_endpoint.port);
    backfill_env(
        "SMELTR_WORKER_METRICS_ENDPOINT_TLS_CERT",
        file.worker.metrics_endpoint.tls_cert,
    );
    backfill_env(
        "SMELTR_WORKER_METRICS_ENDPOINT_TLS_KEY",
        file.worker.metrics_endpoint.tls_key,
    );
    backfill_env(
        "SMELTR_WORKER_METRICS_ENDPOINT_CLIENT_CA",
        file.worker.metrics_endpoint.client_ca,
    );
    backfill_env("SMELTR_WORKER_METRICS_ENDPOINT_USER", file.worker.metrics_endpoint.user);
    backfill_env(
        "SMELTR_WORKER_METRICS_ENDPOINT_PASSWORD",
        file.worker.metrics_endpoint.password,
    );
    if file.worker.enroll_code.is_some() {
        eprintln!(
            "smeltr: `[worker] enroll_code` does nothing and can be removed. A worker that has not enrolled yet \
             takes `[worker] enrollment_code`, minted on the leader's Workers page."
        );
    }
}

/// Runs the data-plane worker and never returns. Only for a worker with no leader of its own; a process that runs both
/// spawns its worker from the leader, against its own address.
fn run_as_worker() -> ! {
    use smeltr::startup_config as cfg;

    let leader_url = cfg::leader_url().unwrap_or_else(|| {
        eprintln!(
            "smeltr: refusing to start -- a worker that does not run a leader needs one to \
             report to. Set `[worker.leader] url` in smeltr.toml, or SMELTR_LEADER_URL."
        );
        std::process::exit(1);
    });

    let group_id = cfg::worker_group();

    // No warning about a missing authority here: an enrollment code carries one, and where it does not, the platform's
    // own roots are the answer rather than an omission.
    start_worker(
        leader_url,
        read_leader_ca(),
        // A worker with a leader elsewhere reads its authority from a file an operator manages, or takes what its
        // enrollment code pinned; nothing here replaces either under them.
        None,
        group_id,
        cfg::worker_state_dir(),
        cfg::worker_enrollment_code(),
        false,
    )
}

/// SIGTERM or SIGINT, in any process but a runtime. What the process runs ends first -- a worker's runtime drains,
/// commits and leaves its consumer groups -- then the log buffer empties and the process goes.
///
/// The stop runs on a thread of its own: ctrlc calls this again for the next signal only once it returns, and a
/// second signal is somebody who will not wait for the drain.
fn stop_on_signal() {
    static SIGNALED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if SIGNALED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        smeltr::logs::stop();
        std::process::exit(1);
    }
    let started = std::thread::Builder::new()
        .name("smeltr-exit".to_string())
        .spawn(exit_after_stopping);
    // A host that will not start one more thread gets the stop on this one, and a second signal waits behind it.
    if started.is_err() {
        exit_after_stopping();
    }
}

/// Never returns. Typed `()` rather than `!` so a thread can be spawned on it.
fn exit_after_stopping() {
    smeltr::worker::stop_before_exit();
    smeltr::logs::stop();
    service::stopped();
    std::process::exit(0)
}

/// Runs the leader, GUI included, and never returns.
fn run_as_leader() -> ! {
    // Every thread the leader runs on, so its logs are stored as the leader's beside a worker in this process.
    smeltr::logs::mark_leader_thread();
    tokio::runtime::Builder::new_multi_thread()
        .on_thread_start(smeltr::logs::mark_leader_thread)
        .enable_all()
        .build()
        .expect("build tokio runtime")
        .block_on(run_leader());
    smeltr::logs::stop();
    std::process::exit(0);
}

fn main() {
    service::run(start, stop_on_signal)
}

fn start() {
    // AWS-LC is the only backend this build carries, and installing it is still what makes it the one in use:
    // `ureq` and `axum-server` are both given rustls without a provider of their own, and curl's own rustls takes
    // an installed default over the one its features name, so each reaches for this when it opens its first
    // connection. Nothing configures it, so it precedes every role and every setting.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls crypto provider (called exactly once, at startup)");
    // Before any thread, as setting the environment needs; every HTTP client reads it when it builds a request.
    let own = smeltr::startup_config::leader_enabled().then(|| reached_at(&smeltr::startup_config::gui_bind()));
    smeltr::startup_config::never_proxy_this_host(own);

    // A runtime is neither role: it is one deployment, started by the worker that supervises it and ended when
    // that worker says so. A role is configuration here rather than an argument, so this is one more setting and
    // not a subcommand.
    //
    // Read before the service configuration, and the only thing that is: a runtime is configured by its start
    // frame and by the environment it inherited, which its supervisor resolved from that file already. Reading it
    // again would put a file an operator is part-way through editing between a worker and the deployment it was
    // told to run -- and the supervisor has stopped the previous one by then.
    let runtime_child = std::env::var(smeltr::worker::child::RUNTIME_ENV).is_ok_and(|v| !v.is_empty());

    // The only orderly stop an operator performs. A runtime drains as its supervisor's `Stop` would have it; every
    // other process ends its worker's deployment first and empties the log buffer last.
    let on_signal: fn() = if runtime_child {
        smeltr::worker::child::stop_on_signal
    } else {
        stop_on_signal
    };
    ctrlc::set_handler(on_signal).expect("install the signal handler (called exactly once, at startup)");

    // Before any thread exists: `backfill_env` mutates the environment.
    if !runtime_child {
        load_and_apply_config_file();
        smeltr::startup_config::check_lazy_settings();
    }

    // Before either role starts, so nothing this process has to say about itself predates somewhere to say it.
    smeltr::logs::start();

    if runtime_child {
        smeltr::worker::child::run_runtime_child();
    }

    // After `logs::start`, so it reaches the deployed logs source like everything else this process says. The
    // offer is recorded per start because that is what ties it to a time and a version, which is what is asked
    // for afterward, when the binary is no longer in front of anyone.
    //
    // A runtime records its own, under its own target: it is a separate process, and a bundle's `smeltr logs`
    // carries that process's buffer alone. This one would not reach such a pipeline at all.
    let (suites, groups) = smeltr::crypto::offered();
    tracing::info!(
        target: "tls.offered",
        suites = suites.join(", "),
        groups = groups.join(", "),
        "TLS algorithms offered"
    );

    match (
        smeltr::startup_config::leader_enabled(),
        smeltr::startup_config::worker_enabled(),
    ) {
        (false, false) => {
            eprintln!(
                "smeltr: refusing to start -- this process has no role. Set `[leader] enabled` \
                 (SMELTR_LEADER_ENABLED), `[worker] enabled` (SMELTR_WORKER_ENABLED), or both."
            );
            smeltr::logs::stop();
            std::process::exit(1);
        }
        // The leader hosts the worker when both are on, so it owns the process either way.
        (true, _) => run_as_leader(),
        (false, true) => run_as_worker(),
    }
}

/// Stop on a config repository that does not read back whole. A leader that went on would deploy from whatever of it
/// still reads; what to restore it from is the operator's call, since a git host holds `main` and pushed branches
/// but not unpushed saves, changes or the deploy log.
fn refuse_damaged_repo(
    repo: &std::path::Path,
    damage: &smeltr::leader::git::GitError,
    store: &dyn smeltr::leader::store::Store,
) -> ! {
    let repair = match smeltr::leader::remote::configured(store) {
        Ok(Some(remote)) => format!(
            "To take {url}'s instead, move {repo} aside and start the leader again: it makes a new one, and its first \
             sync, on its poll or Sync now, deploys {url}'s main. Saves not pushed, changes and the deploy log stay only in the one moved \
             aside. Or repair it, or put a copy you know is sound in its place",
            url = remote.url,
            repo = repo.display()
        ),
        _ => format!(
            "Repair it, or put a copy you know is sound in its place, and start the leader again. {} is the only \
             copy this leader has; a git host to follow keeps another. With neither, move it aside and start the \
             leader again to begin from an empty configuration: the fleet then runs nothing until it is written \
             again",
            repo.display()
        ),
    };
    eprintln!("smeltr: refusing to start -- {damage}. {repair}.");
    std::process::exit(1);
}

/// Writes the secrets encryption key on a leader that has sealed nothing yet, so the first secret stored is sealed
/// under a key that has already been announced. A leader holding values its key cannot open says so as loudly as it
/// can, and goes on: everything but the secrets still works, and the fix is on the Secrets page.
fn check_secrets_key(store: &dyn smeltr::leader::store::Store, path: &std::path::Path) {
    use smeltr::leader::store::KeyState;
    let standing = match store.key_standing() {
        Ok(standing) => standing,
        Err(e) => {
            eprintln!("smeltr: refusing to start -- {e}");
            std::process::exit(1);
        }
    };
    if standing.key == KeyState::Absent
        && let Err(e) = smeltr::leader::secrets::load_or_create(path)
    {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }
    if let Some(problem) = standing.problem() {
        tracing::error!(target: "secrets.key_unusable", path = %path.display(), "{problem}");
    }
}

/// Make sure somebody can sign in, and say so on the console when a password was written.
///
/// Two cases, both of them the operator's first minute with this leader. A store with no account at all gets one,
/// so a fresh install is usable without a separate step. `SMELTR_RECOVER_ADMIN` regenerates the built-in
/// administrator's password, which is the way back in when nobody can sign in at all.
///
/// A generated password is printed with `eprintln!` and never through `tracing`. One log buffer serves the whole
/// process, and a deployed logs source reads it (`logs`), so on a standalone install an event raised here would
/// travel to whatever destination the fleet's pipelines deliver to and stay in an index. The console is where a
/// one-time password belongs, and it is the only place it goes.
fn admit_an_administrator(store: &dyn smeltr::leader::store::Store) {
    use smeltr::leader::accounts;

    let announce = |what: &str, password: accounts::OneTimePassword| {
        eprintln!();
        eprintln!("smeltr: {what}");
        eprintln!("           username: {}", password.username);
        eprintln!("  one-time password: {}", password.password);
        eprintln!("It is shown once and is not recoverable. Signing in with it requires choosing a new one.");
        eprintln!();
    };

    if smeltr::startup_config::recover_admin() {
        match accounts::recover_administrator(store) {
            Ok(Ok(password)) => announce("SMELTR_RECOVER_ADMIN reset the administrator account.", password),
            Ok(Err(e)) => {
                eprintln!("smeltr: refusing to start -- SMELTR_RECOVER_ADMIN could not reset an account: {e}");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("smeltr: refusing to start -- SMELTR_RECOVER_ADMIN could not reach the store: {e}");
                std::process::exit(1);
            }
        }
        // Set in a unit file or a compose file, it fires on every restart -- a new password each time and every
        // session signed out. Worth saying every time, because the symptom otherwise looks like the leader
        // forgetting the password that was just set.
        eprintln!("smeltr: unset SMELTR_RECOVER_ADMIN before the next restart, or it will reset the account again.");
        return;
    }

    match accounts::bootstrap_if_empty(store) {
        Ok(Some(password)) => announce("this leader had no accounts, so one was created.", password),
        Ok(None) => {}
        Err(e) => {
            eprintln!("smeltr: refusing to start -- the first administrator could not be created: {e}");
            std::process::exit(1);
        }
    }
}

async fn run_leader() {
    if assets::is_placeholder_only() {
        tracing::warn!(
            target: "leader.gui_missing",
            "this binary has no GUI baked in: the API works, but `/` will explain itself rather than render. \
             Build gui-frontend and rebuild."
        );
    }

    // Without git the leader would start and serve nothing a worker can deploy, so it is refused before the bind
    // for the same reason the bind comes first: a refused start leaves nothing behind.
    if let Err(e) = smeltr::leader::git::check_git_runs() {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }

    // Before anything below persists state -- the data directory, the account store, the bootstrap administrator
    // it may create and print a one-time password for. A bind failure (an unprivileged port 443, or the port
    // already taken) is meant to be retried after fixing the cause, and a retry that finds an administrator
    // already created, its one-time password already gone from a console nobody kept, is a leader nobody can
    // sign into short of SMELTR_RECOVER_ADMIN. Binding first means that never happens: nothing is created until
    // the socket this leader is actually going to serve is already held.
    let port = smeltr::startup_config::gui_port();
    let bind = smeltr::startup_config::gui_bind();
    let addr = resolve_listen_addr(&bind, port).unwrap_or_else(|e| {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    });
    // Before the bind: a bad origin is a refusal whatever the port, and must not hide behind a port in use.
    let origin = resolve_origin(
        smeltr::startup_config::public_origin().as_deref(),
        &bind,
        port,
        smeltr::startup_config::leader_tls_enabled(),
    )
    .unwrap_or_else(|e| {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    });
    let listener = bind_listener(addr);
    tracing::info!(
        target: "leader.origin",
        origin = %origin.value,
        aliases = ?origin.aliases,
        secure_cookies = origin.secure,
        "the origin a browser reaches this leader at"
    );

    // Before `main` is read for the first time, because reading it is what gives it its first commit: a fresh
    // install has a group before it has a worker in one. Without this the first thing a leader shows is a warning
    // about itself -- its own worker heartbeats for a group and `main` holds configuration for none.
    let data_dir = smeltr::startup_config::leader_data_dir();
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        tracing::warn!(
            target: "leader.data_dir_not_created",
            path = %data_dir.display(),
            error = %e,
            "could not create the leader data directory"
        );
    }

    // Before the database is opened, which is the first thing a second leader would corrupt. Held for the life of
    // the process: dropping it is what releases the directory.
    let _data_dir_lock = match smeltr::leader::lock::acquire(&data_dir) {
        Ok(lock) => lock,
        Err(e) => {
            eprintln!("smeltr: refusing to start -- {e}");
            std::process::exit(1);
        }
    };

    // Built here rather than in `setup_leader`, because seeding the default group needs it too, and seeding has to
    // happen before `main` is read for the first time (above). The store reads the secrets encryption key per
    // operation rather than holding one, but a key file anyone can read is a startup problem: finding out when an
    // admin first stores a secret is finding out too late.
    let secret_key = smeltr::startup_config::leader_secret_key_path();
    if let Err(e) = smeltr::leader::secrets::load(&secret_key) {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }
    let store: std::sync::Arc<dyn smeltr::leader::store::Store> = std::sync::Arc::new(
        smeltr::leader::sqlite_store::SqliteStore::open(&data_dir.join("leader.db"), &secret_key).unwrap_or_else(|e| {
            eprintln!("smeltr: refusing to start -- {e}");
            std::process::exit(1);
        }),
    );

    // `open` rather than `default`: the durable registry is what makes a warning's start time survive a restart, and
    // what gives `/api/flow` anything to serve. Its own file, since the series is written per heartbeat and
    // `leader.db` is for operator actions.
    let registry = smeltr::leader::registry::Registry::open(&data_dir.join("registry.db")).unwrap_or_else(|e| {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    });
    check_secrets_key(store.as_ref(), &secret_key);
    let repo = smeltr::leader::git::repo_path(&data_dir);
    if let Err(damage) = smeltr::leader::git::check_repo(&repo) {
        refuse_damaged_repo(&repo, &damage, store.as_ref());
    }
    // Removed rather than served: a worker would refuse its bytes anyway. With a git host the next sync fetches it
    // again from the host's LFS server; without one it is uploaded again.
    for (oid, removed) in smeltr::lookup::store::remove_damaged(&repo) {
        match removed {
            Ok(()) => tracing::warn!(
                target: "leader.lookup_file_damaged",
                subject = %oid,
                "a stored lookup file did not hash to its name and was removed"
            ),
            Err(e) => tracing::warn!(
                target: "leader.lookup_file_damaged",
                subject = %oid,
                error = %e,
                "a stored lookup file did not hash to its name and could not be removed"
            ),
        }
    }
    if let Err(e) = smeltr::leader::steps::upgrade_repo(&repo) {
        eprintln!("smeltr: refusing to start -- {e}");
        std::process::exit(1);
    }

    // After every store has been opened: one that refuses to start after this would take the one-time password with
    // it.
    admit_an_administrator(store.as_ref());

    let seed_group = smeltr::startup_config::worker_group();
    // A leader following a git host takes `main` from it. One seeded here would be deployed before the first sync
    // replaced it, and would run the fleet on a group with no pipelines meanwhile.
    let seeded = match smeltr::leader::remote::configured(store.as_ref()) {
        Ok(Some(_)) => smeltr::leader::git::init_bare_repo(&repo)
            .map(|()| false)
            .map_err(|e| e.to_string()),
        _ => worker_groups_api::seed_default_group(&data_dir, &seed_group, store.as_ref()),
    };
    match seeded {
        Ok(true) => {
            tracing::info!(target: "leader.group_seeded", subject = %seed_group, "seeded the default worker group")
        }
        Ok(false) => {}
        Err(e) => tracing::warn!(
            target: "leader.group_not_seeded",
            subject = %seed_group,
            error = %e,
            "the default worker group was not seeded"
        ),
    }

    match worker_groups_api::seed_shared_hue(store.as_ref()) {
        Ok(true) => tracing::info!(target: "leader.shared_hue_seeded", "seeded shared's color"),
        Ok(false) => {}
        Err(e) => tracing::warn!(
            target: "leader.shared_hue_not_seeded",
            error = %e,
            "shared's color was not seeded"
        ),
    }

    // `main` is what every deploy moves the fleet to, so it is what a startup check reports on.
    match config_api::Reading::open(smeltr::leader::git::MAIN_BRANCH, config_file::SHARED_DIR) {
        Ok(reading) => config_api::startup_validate(&reading),
        Err(e) => tracing::warn!(
            target: "leader.main_unreadable",
            subject = smeltr::leader::git::MAIN_BRANCH,
            error = %e.message(),
            "could not read the branch the fleet runs, so nothing was validated at startup"
        ),
    }

    let app = Router::new()
        .route("/api/areas/:area/preview/pipeline", post(preview))
        .route("/api/preview/grok", post(grok_captures))
        .route("/api/preview/onboard", post(onboard_propose))
        .route("/api/preview/line-breaker", post(line_breaker))
        .route("/api/preview/format", post(format))
        .route("/api/areas/:area/preview/routes", post(route_preview))
        .route("/api/areas/:area/lookups/file", get(config_api::get_lookup_file))
        .route("/api/areas/:area/lookups/mmdb", get(config_api::get_lookup_mmdb))
        .route(
            "/api/branches/:name",
            delete(branch_api::delete_branch).patch(branch_api::retitle_change),
        )
        // GET/POST `/api/worker-groups` and PUT `/api/worker-groups/:id/hue` are on `admin_and_cluster` instead:
        // both need `State<AppState>`, which only that router carries.
        .route(
            "/api/worker-groups/:id",
            put(worker_groups_api::update_worker_group).delete(worker_groups_api::delete_worker_group),
        )
        .route("/api/branches/:name/open", post(branch_api::open_change))
        .route("/api/branches/:name/close", post(branch_api::close_change))
        .route("/api/branches/:name/comments", post(branch_api::comment_on_change))
        .route("/api/branches/:name/threads/:id", post(branch_api::resolve_thread))
        .route("/api/branches/:name/reviews", post(branch_api::review_change))
        .route("/api/branches/:name/timeline", get(branch_api::change_timeline))
        // `/api/branches/:name/deploy` and `/api/main/deploy` are on `admin_and_cluster` instead: both wait their turn
        // with the remote's syncs, which needs `State<AppState>`.
        .route("/api/branches/:name/bring-current", post(branch_api::bring_current))
        .route("/api/main/history", get(branch_api::history))
        .route("/api/branches/:name/diff", get(branch_api::branch_diff))
        .route("/api/main/diff", get(branch_api::commit_diff))
        .route("/api/areas/:area/samples", get(config_api::list_samples))
        .route(
            "/api/areas/:area/samples/:id",
            get(config_api::get_sample)
                .put(config_api::put_sample)
                .delete(config_api::delete_sample),
        )
        .route("/api/areas/:area/tests", get(test_api::list_tests))
        .route(
            "/api/areas/:area/tests/:id",
            get(test_api::get_test)
                .put(test_api::put_test)
                .delete(test_api::delete_test),
        )
        .route("/api/areas/:area/test-runs", post(test_api::run_tests))
        .route("/api/branches/:name/tests", get(test_api::list_change_tests))
        .route("/api/branches/:name/test-runs", post(test_api::run_change_tests))
        .route("/api/areas/:area", get(config_api::get_config))
        .route("/api/areas/:area/unreadable", get(config_api::get_unreadable))
        .route(
            "/api/areas/:area/files/*path",
            get(config_api::get_file)
                .put(config_api::put_file)
                .delete(config_api::delete_file),
        )
        .route("/api/preview/certificate", post(config_api::describe_certificates))
        .route("/api/areas/:area/undo", post(config_api::undo))
        .route("/api/areas/:area/routes", put(config_api::put_routes))
        .route("/api/areas/:area/grok-patterns", put(config_api::put_grok_patterns))
        // An edited CSV travels whole in this body, so it is as large as the file.
        .route(
            "/api/areas/:area/lookups",
            put(config_api::put_lookup_tables).layer(axum::extract::DefaultBodyLimit::max(256 << 20)),
        )
        .route("/api/areas/:area/manifest", put(config_api::put_manifest))
        .route("/api/areas/:area/pipelines", get(config_api::list_pipelines))
        .route(
            "/api/areas/:area/pipelines/:id",
            get(config_api::get_pipeline)
                .put(config_api::put_pipeline)
                .delete(config_api::delete_pipeline),
        )
        .route("/api/areas/:area/pipeline-callers", get(pipeline_callers))
        .route("/api/areas/:area/:section/:id/connections", get(get_connections))
        .route("/api/docs", get(docs_api::list))
        .route("/api/docs/page", get(docs_api::page));

    let lifetimes = resolve_lifetimes(store.as_ref());
    let auth = gate::Auth::new(store.clone(), origin, lifetimes);

    // Account administration needs `State<Auth>` of its own, so it is merged here rather than declared with the
    // config routes above -- and it must be inside the gate, which is what `route_layer` below covers.
    let app = app.merge(
        Router::new()
            .route("/api/users", get(users_api::list_users).post(users_api::create_user))
            .route("/api/users/:id", patch(users_api::patch_user))
            .route("/api/users/:id/reset-password", post(users_api::reset_password))
            .route("/api/users/:id/revoke-sessions", post(users_api::revoke_sessions))
            .route("/api/security-events", get(users_api::list_security_events))
            .route(
                "/api/me/agent-permissions",
                get(agents_api::get_permissions).put(agents_api::put_permissions),
            )
            .route("/api/roles", get(roles_api::list_roles))
            .route(
                "/api/roles/:name",
                put(roles_api::put_role).delete(roles_api::delete_role),
            )
            .route(
                "/api/settings/sessions",
                get(users_api::get_session_settings).put(users_api::put_session_settings),
            )
            .route(
                "/api/settings/oidc",
                get(oidc_api::get_settings).put(oidc_api::put_settings),
            )
            .route(
                "/api/settings/agent",
                get(agents_api::get_settings).put(agents_api::put_settings),
            )
            .route("/api/agent/models", get(chat_api::models))
            .route("/api/agent/conversations", get(chat_api::list).post(chat_api::start))
            .route(
                "/api/agent/conversations/:id",
                get(chat_api::read).delete(chat_api::delete),
            )
            .route("/api/agent/conversations/:id/messages", post(chat_api::add_message))
            .route("/api/agent/conversations/:id/stop", post(chat_api::stop))
            .route("/api/agent/conversations/:id/context", post(chat_api::context))
            .with_state(auth.clone()),
    );

    // `route_layer` covers only the routes above it; the auth routes below are merged after it because a person
    // with no session has to be able to reach them.
    let app = app.route_layer(axum::middleware::from_fn_with_state(
        auth.clone(),
        gate::require_principal,
    ));

    let LeaderSetup { routes, tls, leader } = setup_leader(port, auth.clone(), store, registry).await;
    // Before `leader` goes into the certificate state, which a leader serving plain HTTP has none of.
    let list = leader.clone();
    let certificates = tls.as_ref().map(|(resolver, _)| {
        std::sync::Arc::new(certificates_api::Tls {
            // Spelled as the leader spells it, from the one absolute data directory. The certificate layer keeps
            // what the socket loaded per directory by its path as written, so a second spelling of this one is a
            // second directory to it: a cutover the heartbeat runs would bump one and the reload here settle the
            // other, leaving every authority read refusing until a restart.
            dir: smeltr::leader::tls::dir_of(&leader.data_dir),
            resolver: resolver.clone(),
            proving: std::sync::Arc::new(smeltr::leader::acme::Proving::new(
                resolver.clone(),
                std::net::SocketAddr::new(addr.ip(), smeltr::startup_config::acme_http_port()),
            )),
            names: smeltr::startup_config::leader_certificate_names(&origin_hosts(&auth.origin)),
            from_inside: embedded_worker_reaches_us_at(&origin_hosts(&auth.origin)),
            validity: smeltr::startup_config::leader_ca_validity(),
            leader,
        })
    });
    // How the paths outside this layer make the socket serve what they wrote -- a cutover, which the heartbeat
    // decides and this process's certificate layer holds the socket for.
    if let Some(serving) = certificates.clone() {
        let handle = tokio::runtime::Handle::current();
        let ordering = auth.clone();
        // The directory it is told about is this leader's only one, and `serving` already holds it.
        smeltr::leader::tls::on_reload(move |_dir| {
            let serving = serving.clone();
            let ordering = ordering.clone();
            handle.spawn(async move {
                // The same lock every certificate write takes. Without it this can read the paths it is about to
                // serve, have an upload install and reload a supplied set underneath, and then put the leader's
                // own certificate back on the socket -- with both reloads reporting success, so nothing says the
                // socket and the disk have parted.
                let _ordered = ordering.settings_writes.lock().await;
                // The state of the disk this reload is for, taken with the files. Recorded only if the socket
                // took them, and never as "the disk as it is now": another replacement during the await leaves
                // the socket on this one, which is the two having parted and is what every read then says.
                if let Err(e) = certificates_api::reload_socket(&serving) {
                    tracing::warn!(
                        target: "leader.certificate_not_reloaded",
                        error = %e,
                        "the certificate on disk changed and this leader is still serving the previous one"
                    );
                }
            });
        });
    }

    let app = app
        .merge(routes)
        .merge(certificates_api::router(
            auth.clone(),
            certificates.clone(),
            list.clone(),
        ))
        .merge(acme_api::router(auth.clone(), certificates.clone()))
        .merge(auth_router(auth.clone()))
        // Before the SPA fallback, so an API path nobody serves answers as an API. Without it a mistyped route
        // returns `index.html` with a `200`, and a caller parses a login page as its data.
        .route("/api/*rest", axum::routing::any(unknown_api))
        .fallback(assets::serve)
        .layer(axum::middleware::from_fn(gate::shape))
        .layer(axum::middleware::from_fn(gate::uncached))
        // Outermost, so every answer carries the browser hardening headers, the assets and the fallback included.
        .layer(axum::middleware::from_fn(gate::harden));
    // What an agent's calls go through: this router, gate and all.
    auth.chats.serve(app.clone());

    tokio::spawn(chat_api::sweep(auth.clone()));
    tokio::spawn(refresh_oidc_sessions(auth.clone()));
    if let Some(serving) = certificates {
        tokio::spawn(certificates_api::keep_renewed(auth.clone(), serving, list));
    }
    tokio::spawn(sweep_sessions(auth));

    match tls {
        Some((resolver, _ca_pem)) => {
            tracing::info!(target: "leader.listening", %addr, tls = true, "smeltr is listening");
            let config = axum_server::tls_rustls::RustlsConfig::from_config(
                resolver
                    .server_config()
                    .expect("the socket's TLS over the installed provider"),
            );
            let mut server =
                axum_server::tls_rustls::from_tcp_rustls(listener, config).expect("wrap smeltr listener with TLS");
            smeltr::leader::listen::bound(server.http_builder(), smeltr::leader::listen::HEADER_READ);
            server
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await
                .expect("smeltr server error");
        }
        None => {
            tracing::info!(target: "leader.listening", %addr, tls = false, "smeltr is listening, with TLS disabled by SMELTR_LEADER_TLS");
            let mut server = axum_server::from_tcp(listener).expect("hand smeltr listener to the server");
            smeltr::leader::listen::bound(server.http_builder(), smeltr::leader::listen::HEADER_READ);
            // `into_make_service_with_connect_info` rather than the plain one: the login throttle keys on the
            // socket's own peer, and nothing else may stand in for it.
            server
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await
                .expect("smeltr server error");
        }
    }
}

/// `addr`, bound and ready to accept -- or a clear explanation on the console instead of the OS's raw
/// `os error 13`/`os error 98`.
///
/// A privileged port (below 1024) needs a capability this process may not have: root, or, on Linux,
/// `CAP_NET_BIND_SERVICE` on the binary (`setcap 'cap_net_bind_service=+ep' /path/to/smeltr`) -- Windows has no
/// such restriction at all. The other common cause, the port already being
/// in use by something else, gets its own message rather than being folded into this one.
///
/// Built through `socket2` rather than `std::net::TcpListener::bind` so `SO_REUSEADDR` is set on Unix, matching
/// what `tokio::net::TcpListener::bind` already did: without it, a restart lands in the previous listener's
/// `TIME_WAIT` and this leader refuses to start for a port nothing is actually listening on.
///
/// The phrase "bind smeltr listen address" is load-bearing: it is what `tests/gate-tests.rs` greps a leader's
/// stderr for to tell a lost race for an ephemeral port -- rare, and cured by trying another port -- from a
/// leader that failed to start for its own reasons. It stays in every branch below, including the ones this
/// function's own callers never trigger in that test.
fn bind_listener(addr: std::net::SocketAddr) -> std::net::TcpListener {
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let bound = (|| -> std::io::Result<std::net::TcpListener> {
        let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        #[cfg(unix)]
        socket.set_reuse_address(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        Ok(socket.into())
    })();
    match bound {
        Ok(listener) => listener,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!(
                "smeltr: refusing to start -- could not bind smeltr listen address {addr}: already in use by another process."
            );
            std::process::exit(1);
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!(
                "smeltr: refusing to start -- could not bind smeltr listen address {addr}: not permitted. Ports \
                 below 1024 need root, or, on Linux, the CAP_NET_BIND_SERVICE capability (setcap \
                 'cap_net_bind_service=+ep' <path to smeltr>). Set \
                 SMELTR_GUI_PORT to an unprivileged port instead if this leader runs behind a proxy."
            );
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("smeltr: refusing to start -- could not bind smeltr listen address {addr}: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FormatRequest, GrokCapturesRequest, LineBreakerRequest, captures_of, format_code, line_breaker_verdict,
    };
    use smeltr::expr::pretty::Kind;

    fn column(name: &str, ty: &str) -> smeltr::heartbeat::CaptureColumn {
        smeltr::heartbeat::CaptureColumn {
            name: name.to_string(),
            ty: ty.to_string(),
        }
    }

    /// Two captured records: `id` and `user.name` with their offsets, the second's `id` failed as `true`.
    fn captured() -> (Vec<smeltr::heartbeat::CaptureColumn>, Vec<smeltr::io::RecordObject>) {
        let record = |v: serde_json::Value| v.as_object().unwrap().clone();
        (
            vec![
                column("id", "i64"),
                column("user.name", "str"),
                column("__source.offset", "i64"),
            ],
            vec![
                record(serde_json::json!({"id": 1, "user": {"name": "ann"}, "__source": {"offset": 7}})),
                record(
                    serde_json::json!({"id": "1", "user": {"name": "bo"}, "__source": {"offset": 8},
                    "smeltr": {"mistyped": ["id", "user.name"]}}),
                ),
            ],
        )
    }

    /// A captured sample reads by the columns it was captured under, each value typed and each one its record lists
    /// as failed failed again as the source spelled it, though its type would take that text.
    #[test]
    fn a_captured_sample_reads_by_its_own_columns() {
        let (columns, records) = captured();
        let meta = smeltr::plan::topology::SourceMeta::of_columns(&columns).unwrap();
        let smeltr::leader::preview::SampleRead {
            records: read,
            warnings,
            ..
        } = smeltr::leader::preview::read_sample(&meta, &columns, &records).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        let chunk = &read[1].chunk;
        assert_eq!(chunk.failed(), &[(1, 0, "1".to_string()), (1, 1, "bo".to_string())]);
        assert_eq!(
            meta.record_object(&read[0], ""),
            *serde_json::json!({"id": 1, "user": {"name": "ann"}, "__source": {"offset": 7}})
                .as_object()
                .unwrap()
        );
    }

    /// A sample's internal fields -- where a record was read from, why a block held it, how it was captured -- are its
    /// fields like any other: a pipeline previewed over the sample reads them, and a route preview, which reads the
    /// record as its live source sets it, checks only the record's own fields and says nothing of them.
    #[test]
    fn a_sample_s_internal_fields_are_read_as_any_field() {
        let (mut columns, mut records) = captured();
        columns.push(column(smeltr::worker::capture::EXCEPTION_MESSAGE, "str"));
        columns.push(column("__capture.group", "str"));
        for record in &mut records {
            record.insert(
                "__exception".to_string(),
                serde_json::json!({"message": "matched no route"}),
            );
            record.insert("__capture".to_string(), serde_json::json!({"group": "west"}));
        }
        let own = smeltr::plan::topology::SourceMeta::of_columns(&columns).unwrap();
        let read = smeltr::leader::preview::read_sample(&own, &columns, &records).unwrap();
        assert!(read.warnings.is_empty(), "{:?}", read.warnings);
        let object = own.record_object(&read.records[0], "");
        assert_eq!(
            object["__exception"],
            serde_json::json!({"message": "matched no route"})
        );
        assert_eq!(object["__capture"], serde_json::json!({"group": "west"}));

        let live = smeltr::plan::topology::SourceMeta::KAFKA
            .declaring(vec![
                ("id".into(), smeltr::core::column::DataType::I64),
                ("user.name".into(), smeltr::core::column::DataType::Str),
            ])
            .unwrap();
        let read = smeltr::leader::preview::read_sample(&live, &columns, &records).unwrap();
        assert!(read.warnings.is_empty(), "{:?}", read.warnings);
    }

    /// A route preview reads a sample as its source declares now, and says how that differs from what was captured.
    #[test]
    fn a_sample_read_under_a_changed_source_says_what_changed() {
        let (columns, records) = captured();
        let now = smeltr::plan::topology::SourceMeta::KAFKA
            .declaring(vec![
                ("id".into(), smeltr::core::column::DataType::Str),
                ("region".into(), smeltr::core::column::DataType::Str),
            ])
            .unwrap();
        let smeltr::leader::preview::SampleRead {
            records: read,
            warnings,
            ..
        } = smeltr::leader::preview::read_sample(&now, &columns, &records).unwrap();
        assert_eq!(
            warnings,
            [
                "`id` was captured as i64 and its source now declares it str",
                "`user.name` was captured, and its source no longer declares it, so it is left out",
                "`region` is declared and was not captured, so it reads null",
            ]
        );
        assert!(
            read[0].chunk.failed().is_empty(),
            "a number reads as its text under str, and the second `id`'s mark is dropped"
        );
        let ids: Vec<serde_json::Value> = read.iter().map(|r| now.record_object(r, "")["id"].clone()).collect();
        assert_eq!(ids, [serde_json::json!("1"), serde_json::json!("1")]);
    }

    /// A capture keeps the payload as read and the text its source decoded: a Latin-1 `café` previews as `café`, its
    /// `__source.bytes` still `caf\xe9`.
    #[test]
    fn a_captured_byte_record_previews_with_the_text_its_source_decoded() {
        let columns = vec![column("_raw", "str"), column("__source.bytes", "bytes")];
        let records = vec![
            serde_json::json!({"_raw": "caf\u{e9}", "__source": {"bytes": "Y2Fm6Q=="}})
                .as_object()
                .unwrap()
                .clone(),
        ];
        let meta = smeltr::plan::topology::SourceMeta::of_columns(&columns).unwrap();
        let read = smeltr::leader::preview::read_sample(&meta, &columns, &records).unwrap();
        assert_eq!(read.texts, ["caf\u{e9}"]);
        assert_eq!(bytes_of(&read.records[0]), b"caf\xe9");
    }

    /// A capture from a source that decodes none has `__source` columns alone; it previews with no `_raw`, and its
    /// `__source.bytes` is a column like any other.
    #[test]
    fn a_sample_captured_with_no_raw_reads_its_bytes() {
        let columns = vec![column("__source.offset", "i64"), column("__source.bytes", "bytes")];
        let meta = smeltr::plan::topology::SourceMeta::of_columns(&columns).unwrap();
        assert_eq!(meta.field_type("_raw"), None);
        let records = vec![
            serde_json::json!({"__source": {"offset": 7, "bytes": "Y2Fm6Q=="}})
                .as_object()
                .unwrap()
                .clone(),
        ];
        let read = smeltr::leader::preview::read_sample(&meta, &columns, &records).unwrap();
        assert_eq!(bytes_of(&read.records[0]), b"caf\xe9");
    }

    /// The `__source.bytes` a sample's record holds, read back from its column, the second.
    fn bytes_of(record: &smeltr::io::SourceRecord) -> Vec<u8> {
        record.chunk.column(1).var().unwrap().get(record.row as usize).to_vec()
    }

    /// A sample captured at a destination previews as its source's records, each starting with the `__destination`
    /// it was sent with: a pipeline that sets none keeps it, and one that sets it replaces it.
    #[test]
    fn a_sample_captured_at_a_destination_previews_under_its_source() {
        let config = r#"{"version":1,"sources":[],"destinations":[],"routes":[],
            "pipelines":[{"id":"p","name":"p","steps":[{"id":"c","type":"code",
                "body":"doubled = n * 2; seen = __destination.route; __destination.id = \"d2\"; id = __destination.id;"}]}]}"#;
        let records: Vec<smeltr::io::RecordObject> = serde_json::from_value(serde_json::json!([{
            "n": 2, "__source": {"topic": "t"},
            "__destination": {"route": "orders", "id": "d1"},
        }]))
        .unwrap();
        let previewed = super::run_preview(
            super::PreviewRequest {
                config: config.to_string(),
                pipeline: None,
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[
                column("n", "i64"),
                column("__source.topic", "str"),
                column("__destination.route", "str"),
                column("__destination.id", "str"),
            ],
            records,
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(
            previewed.rows[0].json,
            Some(serde_json::json!({"n": 2, "doubled": 4, "seen": "orders", "id": "d2"}))
        );
    }

    /// With attribution, each row says which steps changed each of its fields, in the order they ran, and which step
    /// dropped it: a step in an `if` branch the row did not take changed nothing on it, and a dropped row has no fields.
    #[test]
    fn attribution_names_the_steps_that_changed_each_field_and_dropped_each_row() {
        let config = r#"{"version":1,"sources":[],"destinations":[],"routes":[],
            "pipelines":[{"id":"p","name":"p","steps":[
                {"id":"set","type":"code","body":"a = n * 2; b = 1;"},
                {"id":"big","type":"if","condition":"n > 1",
                    "then":[{"id":"bump","type":"code","body":"a = a + 1;"}],
                    "else":[{"id":"gone","type":"drop","condition":"true"}]},
                {"id":"trim","type":"code","body":"delete(b);"}]}]}"#;
        let previewed = super::run_preview(
            super::PreviewRequest {
                config: config.to_string(),
                pipeline: None,
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: true,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[column("n", "i64")],
            serde_json::from_value(serde_json::json!([{"n": 2}, {"n": 1}])).unwrap(),
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let steps = |row: usize| serde_json::to_value(&previewed.rows[row].steps).unwrap();
        assert_eq!(steps(0), serde_json::json!({"a": ["set", "bump"]}));
        assert_eq!(previewed.rows[0].dropped_by, None);
        assert_eq!(steps(1), serde_json::Value::Null);
        assert_eq!(previewed.rows[1].dropped_by.as_deref(), Some("gone"));
    }

    /// A called pipeline's steps are the call's doing, its exceptions included, and a write to an object's field is a
    /// change to that field.
    #[test]
    fn attribution_names_a_call_for_what_its_pipeline_changed() {
        let config = r#"{"version":1,"sources":[],"destinations":[],"routes":[],
            "pipelines":[
                {"id":"p","name":"p","steps":[
                    {"id":"use_q","type":"call","pipeline":"q"},
                    {"id":"nest","type":"code","body":"o.x = n; o.y = 2;"},
                    {"id":"again","type":"code","body":"o.x = n + 1;"}]},
                {"id":"q","name":"q","steps":[{"id":"inner","type":"code","body":"c = 1; z = 1 / 0;"}]}]}"#;
        let previewed = super::run_preview(
            super::PreviewRequest {
                config: config.to_string(),
                pipeline: Some("p".to_string()),
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: true,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[column("n", "i64")],
            serde_json::from_value(serde_json::json!([{"n": 2}])).unwrap(),
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(
            serde_json::to_value(&previewed.rows[0].steps).unwrap(),
            serde_json::json!({"c": ["use_q"], "o.x": ["nest", "again"], "o.y": ["nest"]})
        );
        assert_eq!(previewed.rows[0].exceptions[0].step.as_deref(), Some("use_q"));
    }

    /// An exception names the step that raised it.
    #[test]
    fn an_exception_names_its_step() {
        let previewed = preview_of(r#"[{"_raw": "x"}]"#, "y = 1 / 0;");
        assert_eq!(previewed.rows[0].exceptions[0].step.as_deref(), Some("c"));
    }

    /// A pasted sample has no columns, and reads as `_raw` text.
    #[test]
    fn a_sample_with_no_columns_reads_as_raw_text() {
        let meta = smeltr::plan::topology::SourceMeta::of_columns(&[]).unwrap();
        let records = vec![smeltr::io::raw_object("a line")];
        let smeltr::leader::preview::SampleRead {
            records: read,
            warnings,
            ..
        } = smeltr::leader::preview::read_sample(&meta, &[], &records).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(read[0].bytes(), Some(&b"a line"[..]));
    }

    fn preview_of(records: &str, code: &str) -> super::Envelope<super::Preview> {
        preview_over(&[], records, code)
    }

    fn preview_over(
        columns: &[smeltr::heartbeat::CaptureColumn],
        records: &str,
        code: &str,
    ) -> super::Envelope<super::Preview> {
        let config = format!(
            r#"{{"version":1,"sources":[],"destinations":[],"routes":[],
                "pipelines":[{{"id":"p","name":"p","steps":[{{"id":"c","type":"code","body":{code:?}}}]}}]}}"#
        );
        super::run_preview(
            super::PreviewRequest {
                config,
                pipeline: None,
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            columns,
            serde_json::from_str(records).unwrap(),
        )
    }

    fn filtered(code: &str, records: &str, filter: &str) -> super::Envelope<super::Preview> {
        let config = format!(
            r#"{{"version":1,"sources":[],"destinations":[],"routes":[],
                "pipelines":[{{"id":"p","name":"p","steps":[{{"id":"c","type":"code","body":{code:?}}}]}}]}}"#
        );
        super::run_preview(
            super::PreviewRequest {
                config,
                pipeline: None,
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: Some(filter.to_string()),
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[],
            serde_json::from_str(records).unwrap(),
        )
    }

    /// A filter keeps the rows it is true for, and says how many there were. The dictionary is still of every row.
    #[test]
    fn a_preview_filter_keeps_the_rows_it_is_true_for() {
        let records = r#"[{"_raw":"a"},{"_raw":"b"},{"_raw":"c"}]"#;
        let previewed = filtered(r#"x = _raw; if (_raw == "a") { y = 1; }"#, records, r#"x != "b""#);
        assert!(previewed.succeeded(), "{:?}", previewed.errors);
        let kept: Vec<u32> = previewed.rows.iter().map(|r| r.row).collect();
        assert_eq!(kept, [0, 2]);
        assert_eq!(previewed.unfiltered, Some(3));
        let sampled = &previewed.dictionary.as_ref().unwrap().sampled.as_ref().unwrap().fields;
        assert!(sampled.iter().any(|(name, _)| name == "y"), "{sampled:?}");
    }

    /// A filter reads the exception tag, as a delivery rule does.
    #[test]
    fn a_preview_filter_reads_the_exception_tag() {
        let records = r#"[{"_raw":"1"},{"_raw":"x"}]"#;
        let previewed = filtered("n: i64 = _raw as i64;", records, "smeltr.exception != null");
        assert!(previewed.succeeded(), "{:?}", previewed.errors);
        let kept: Vec<u32> = previewed.rows.iter().map(|r| r.row).collect();
        assert_eq!(kept, [1]);
    }

    /// A filter that does not compile to a Bool fails at its own stage, so the editor can tell it from the pipeline's.
    #[test]
    fn a_preview_filter_that_does_not_compile_fails_at_its_own_stage() {
        for filter in ["x ==", "x"] {
            let previewed = filtered("x = _raw;", r#"[{"_raw":"a"}]"#, filter);
            assert!(!previewed.succeeded(), "{filter}");
            assert_eq!(previewed.stage, Some("filter"), "{filter}: {:?}", previewed.errors);
        }
    }

    /// A preview names its pipeline by id: a renamed pipeline's name no longer matches it.
    #[test]
    fn a_preview_finds_its_pipeline_by_id() {
        let config = r#"{"version":1,"sources":[],"destinations":[],"routes":[],"pipelines":[
            {"id":"pipeline-a","name":"Same","steps":[{"id":"c","type":"code","body":"a = 1;"}]},
            {"id":"pipeline-b","name":"Same","steps":[{"id":"c","type":"code","body":"a = 2;"}]}]}"#;
        let previewed = super::run_preview(
            super::PreviewRequest {
                config: config.to_string(),
                pipeline: Some("pipeline-b".to_string()),
                sample: None,
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
            Default::default(),
            &[],
            serde_json::from_str(r#"[{"_raw":"{}"}]"#).unwrap(),
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(previewed.rows[0].json.as_ref().unwrap()["a"], 2);
    }

    /// A captured record's `__source` is what a pipeline preview reads, each field a column of its own record: two
    /// records from two sources keep their own names.
    #[test]
    fn a_previewed_pipeline_reads_what_the_records_source_set() {
        let records = r#"[{"_raw":"{}","__source":{"name":"orders","type":"kafka","topic":"t","partition":1,
            "offset":7,"headers":{"trace":"t1"}}},
            {"_raw":"{}","__source":{"name":"returns","type":"kafka","topic":"t","partition":1,
            "offset":8,"headers":{"trace":"t2"}}}]"#;
        let columns = [
            column("_raw", "str"),
            column("__source.name", "str"),
            column("__source.type", "str"),
            column("__source.topic", "str"),
            column("__source.partition", "i64"),
            column("__source.offset", "i64"),
            column("__source.headers", "variant"),
        ];
        let previewed = preview_over(
            &columns,
            records,
            "a = __source.name + \"/\" + (__source.headers.trace as str) + \"/\" + toString(__source.offset);",
        );
        assert!(
            previewed.succeeded(),
            "{:?}",
            previewed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(previewed.rows[0].json.as_ref().unwrap()["a"], "orders/t1/7");
        assert_eq!(previewed.rows[1].json.as_ref().unwrap()["a"], "returns/t2/8");
    }

    /// A pasted record names no source, so its pipeline may not read one.
    #[test]
    fn a_pasted_record_declares_no_source_fields() {
        let previewed = preview_of(r#"[{"_raw":"{}"}]"#, "a = __source.topic;");
        assert!(!previewed.succeeded());
        assert!(
            previewed.errors[0].message.contains("__source.topic"),
            "{}",
            previewed.errors[0].message
        );
    }

    /// Every file config denies unknown fields, so a key left behind by an upgrade stops the process before it
    /// reaches the credential that made the key unnecessary.
    #[test]
    fn a_worker_config_still_naming_an_enroll_code_starts() {
        let file: super::FileConfig = toml::from_str(
            "[worker]\nenabled = true\ngroup = \"edge\"\nenroll_code = \"whatever the leader minted\"\n",
        )
        .expect("a file left as an upgrade found it");
        assert_eq!(file.worker.group.as_deref(), Some("edge"));
    }

    /// What the example file shows is what an admin uncomments, so every value in it must load.
    #[test]
    fn the_example_file_loads_with_every_setting_uncommented() {
        let example = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/smeltr.example.toml"))
            .expect("read smeltr.example.toml");
        let uncommented: String = example
            .lines()
            .map(|line| match line.strip_prefix("# ") {
                Some(setting) if setting.split_once(" = ").is_some_and(|(key, _)| !key.contains(' ')) => setting,
                _ => line,
            })
            .map(|line| format!("{line}\n"))
            .collect();
        let file: super::FileConfig = toml::from_str(&uncommented).expect("every value the example shows loads");
        assert_eq!(file.worker.heartbeat.stop_after_unreachable_secs, Some(-1));
    }

    fn captures(patterns: &[&str]) -> super::Envelope<super::GrokCaptures> {
        captures_of(GrokCapturesRequest {
            patterns: patterns.iter().map(|p| p.to_string()).collect(),
            custom_patterns: vec![],
            grok_patterns: vec![],
        })
    }

    /// The editor sends the bundle's patterns with the step's, so a capture minted inside a shared one is listed.
    #[test]
    fn a_capture_inside_a_shared_pattern_is_listed() {
        let got = captures_of(GrokCapturesRequest {
            patterns: vec!["%{VENDOR}".to_string()],
            custom_patterns: vec![],
            grok_patterns: vec![smeltr::config_file::GrokPatternDto {
                id: "VENDOR".to_string(),
                pattern: "%{WORD:vendor}".to_string(),
            }],
        });
        assert_eq!(got.captures, vec!["vendor"]);
    }

    /// No sample, no pipeline, no compile of anything but the patterns: the captures are in the pattern text, which is
    /// what lets an editor fill a step's field names in while the step itself does not compile.
    #[test]
    fn a_patterns_captures_need_no_records() {
        let r = captures(&["%{WORD:action} %{INT:status}"]);
        assert!(r.succeeded());
        assert_eq!(r.captures, vec!["action", "status"]);
        assert!(r.errors.is_empty());
    }

    /// The union over candidate patterns, deduped, since any of them may match a row.
    #[test]
    fn every_candidate_patterns_captures_are_offered_once() {
        let r = captures(&["^%{INT:n}$", "^%{WORD:w} %{INT:n}$"]);
        assert_eq!(r.captures, vec!["n", "w"]);
    }

    /// A line breaker as it is typed gets the save's verdict, in the save's words.
    #[test]
    fn a_typed_line_breaker_gets_the_saves_verdict() {
        let verdict = |pattern: &str| {
            line_breaker_verdict(LineBreakerRequest {
                pattern: pattern.to_string(),
            })
            .errors
            .pop()
            .map(|e| e.message)
        };
        assert_eq!(verdict(r"([\r\n]+)\d{4}-"), None);
        assert!(
            verdict(r"\n")
                .unwrap()
                .contains("which has no capture group to mark the break")
        );
        assert!(verdict("(").unwrap().contains("which is not a pattern"));
        assert!(verdict(r"(\n*)").unwrap().contains("can match no bytes at all"));
    }

    /// Code is laid out as the leader's formatter lays it out, and code that does not parse yet is not touched.
    #[test]
    fn typed_code_is_formatted() {
        let format = |code: &str, kind| {
            format_code(FormatRequest {
                code: code.into(),
                kind,
            })
            .body
            .code
        };
        assert_eq!(format("a&&b", Kind::Expr).as_deref(), Some("a && b"));
        assert_eq!(format("x=1", Kind::Block).as_deref(), Some("x = 1;"));
        assert_eq!(format("x = (", Kind::Block), None);
    }

    /// An unnamed capture declares nothing, so it is not offered.
    #[test]
    fn an_unnamed_capture_is_not_a_field() {
        assert_eq!(captures(&["%{WORD:action} %{INT}"]).captures, vec!["action"]);
    }

    /// A half-typed pattern is the normal state of an editor, so it answers rather than failing the request -- the
    /// editor keeps the fields it already has.
    #[test]
    fn a_pattern_that_does_not_compile_says_so() {
        let r = captures(&["%{NOT_A_PATTERN:x}"]);
        assert!(!r.succeeded());
        assert!(r.captures.is_empty());
        assert!(r.errors[0].message.contains("NOT_A_PATTERN"), "names the pattern");
    }

    fn upload(data: &std::path::Path, bytes: &[u8], delimiter: u8) -> super::Envelope<super::Upload> {
        let writing = super::config_api::Writing::open_in(data, "work", None, "shared").unwrap();
        let mut incoming = smeltr::lookup::store::Incoming::new(writing.repo()).unwrap();
        std::io::Write::write_all(&mut incoming, bytes).unwrap();
        super::save_lookup_file(
            &writing,
            "t.csv",
            incoming,
            delimiter,
            &smeltr::leader::store::Actor::system(),
        )
    }

    /// The bytes go to the store and a pointer to the branch, named by content: a counter restarts at zero with the
    /// process, so a second run's first upload would overwrite a committed file some other table still names. The
    /// same bytes are the same file, the one collision worth having.
    #[test]
    fn an_upload_is_stored_by_content_and_committed_as_a_pointer() {
        let data = std::env::temp_dir().join(format!("smeltr-upload-pointer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let first = upload(&data, b"ip,host\n10.0.0.1,web\n", b',');
        assert_eq!(
            first.headers,
            Some(vec!["ip".to_string(), "host".to_string()]),
            "{:?}",
            first.errors
        );
        let path = first.path.clone().unwrap();
        assert_eq!(
            upload(&data, b"ip,host\n10.0.0.1,web\n", b',').path.as_ref(),
            Some(&path)
        );
        assert_ne!(
            upload(&data, b"ip,host\n10.0.0.2,db\n", b',').path.as_ref(),
            Some(&path)
        );

        let repo = smeltr::leader::git::repo_path(&data);
        let commit = first.body.commit.unwrap();
        let file = smeltr::plan::config::LookupFile {
            text: path.clone(),
            at: smeltr::plan::config::LookupAt::InCommit {
                repo,
                commit,
                path: format!("shared/{path}"),
            },
        };
        assert!(file.pointer().unwrap().is_some(), "the commit holds a pointer");
        assert_eq!(&file.map().unwrap()[..], b"ip,host\n10.0.0.1,web\n");
        std::fs::remove_dir_all(&data).ok();
    }

    #[test]
    fn an_upload_reads_its_headers_with_the_delimiter_it_names() {
        let data = std::env::temp_dir().join(format!("smeltr-upload-delimiter-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let r = upload(&data, b"ip;host\n10.0.0.1;web\n", b';');
        assert_eq!(r.body.headers.expect("headers"), ["ip", "host"]);
        std::fs::remove_dir_all(&data).ok();
    }

    fn empty_file() -> smeltr::config_file::FileConfig {
        smeltr::config_file::FileConfig {
            version: smeltr::config_file::CURRENT_VERSION,
            sources: vec![],
            destinations: vec![],
            pipelines: vec![],
            routes: vec![],
            lookup_tables: vec![],
            grok_patterns: vec![],
            on_exception: None,
            name: None,
        }
    }

    /// A group pipeline, `audit`, that calls `enrich` and reads table `geo`, both of which only `shared/` declares.
    fn audit_calling_shared() -> smeltr::config_file::FileConfig {
        use smeltr::config_file::{self, PipelineDto, StepDto};
        smeltr::config_file::FileConfig {
            pipelines: vec![PipelineDto {
                id: "audit".to_string(),
                name: "audit".to_string(),
                steps: vec![
                    StepDto::Call {
                        id: "call-enrich".to_string(),
                        filter: None,
                        disabled: false,
                        pipeline: "enrich".to_string(),
                    },
                    eval_step("bind-city-id", "city_id", "\"1\""),
                    StepDto::Lookup {
                        id: "lookup-geo".to_string(),
                        filter: None,
                        disabled: false,
                        input: "city_id".to_string(),
                        table: "geo".to_string(),
                        match_kind: config_file::MatchKindDto::Exact,
                        multiplicity: Default::default(),
                        key_column: "city_id".to_string(),
                        outputs: vec![config_file::LookupOutputDto {
                            source: "country".to_string(),
                            field: "country".to_string(),
                            default: None,
                            ty: None,
                        }],
                    },
                ],
            }],
            ..empty_file()
        }
    }

    fn eval_step(id: &str, target: &str, expr: &str) -> smeltr::config_file::StepDto {
        smeltr::config_file::StepDto::Eval {
            id: id.to_string(),
            filter: None,
            disabled: false,
            assignments: vec![smeltr::config_file::AssignmentDto {
                target: target.to_string(),
                expr: expr.to_string(),
            }],
        }
    }

    /// Commits to branch `work` under `data` a `shared/` bundle -- source `orders`, pipeline `enrich` (sets
    /// `enriched`), table `geo` (`city_id` 1 is `US`) with its file -- and `group` with `sample` as `groups/west`.
    fn seed_west_over_shared(
        data: &std::path::Path,
        group: &smeltr::config_file::FileConfig,
        sample: &smeltr::config_file::SampleDto,
    ) {
        use smeltr::config_file::{self, FileConfig, LookupSourceDto, LookupTableDto, PipelineDto};
        use smeltr::leader::git;
        use smeltr::leader::store::Actor;

        let _ = std::fs::remove_dir_all(data);
        let repo = git::repo_path(data);
        git::init_bare_repo(&repo).unwrap();
        git::ensure_main_commit(&repo).unwrap();

        let shared = FileConfig {
            sources: serde_json::from_value(serde_json::json!([
                { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
            ]))
            .unwrap(),
            lookup_tables: vec![LookupTableDto {
                id: "geo".to_string(),
                source: LookupSourceDto::Csv {
                    path: "lookups/geo.csv".to_string(),
                    delimiter: ",".to_string(),
                    headers: vec![],
                },
            }],
            pipelines: vec![PipelineDto {
                id: "enrich".to_string(),
                name: "enrich".to_string(),
                steps: vec![eval_step("mark", "enriched", "true")],
            }],
            ..empty_file()
        };

        let mut csv = smeltr::lookup::store::Incoming::new(&repo).unwrap();
        std::io::Write::write_all(&mut csv, b"city_id,country\n1,US\n").unwrap();
        let pointer = csv.finish(None).unwrap().text().into_bytes();
        let bundle = |prefix: &str, file: &FileConfig| {
            config_file::bundle_files(file)
                .unwrap()
                .into_iter()
                .map(move |(path, content)| git::FileEdit {
                    path: format!("{prefix}/{path}"),
                    content: Some(content),
                })
                .collect::<Vec<_>>()
        };
        let mut edits = bundle(config_file::SHARED_DIR, &shared);
        edits.extend(bundle("groups/west", group));
        edits.push(git::FileEdit {
            path: format!("{}/lookups/geo.csv", config_file::SHARED_DIR),
            content: Some(pointer),
        });
        edits.push(git::FileEdit {
            path: format!("groups/west/{}", config_file::sample_path(&sample.id).unwrap()),
            content: Some(config_file::sample_bytes(sample)),
        });
        let parent = git::main_head(&repo).unwrap();
        git::commit_edits(&repo, "work", &parent, None, &edits, "seed", &Actor::system()).unwrap();
    }

    /// A group's pipeline previews against `shared/` merged in, as it runs once deployed: it calls a pipeline and
    /// reads a lookup table that only `shared/` declares, and the table's file resolves under `shared/` rather than
    /// the group's own subtree.
    #[test]
    fn a_group_preview_calls_a_shared_pipeline_and_reads_a_shared_table() {
        use smeltr::config_file;

        let data = std::env::temp_dir().join(format!("smeltr-preview-shared-{}", std::process::id()));
        let group = audit_calling_shared();
        let sample = config_file::SampleDto {
            id: "s".to_string(),
            name: "s".to_string(),
            source: None,
            records: vec![smeltr::io::raw_object("{}".to_string())],
            columns: Vec::new(),
        };
        seed_west_over_shared(&data, &group, &sample);

        // The editor sends only the group's own bundle.
        let response = super::preview_on(
            &data,
            "work",
            "west",
            super::PreviewRequest {
                config: serde_json::to_string(&group).unwrap(),
                pipeline: None,
                sample: Some(sample.id.clone()),
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
        );

        assert!(
            response.succeeded(),
            "{:?}",
            response.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let row = response.rows[0].json.as_ref().expect("the row survives");
        assert_eq!(
            row["enriched"],
            serde_json::json!(true),
            "the shared pipeline ran: {row}"
        );
        assert_eq!(
            row["country"],
            serde_json::json!("US"),
            "the shared table's file resolved: {row}"
        );

        // A group with no pipeline of its own does not preview one of shared's.
        let response = super::preview_on(
            &data,
            "work",
            "west",
            super::PreviewRequest {
                config: serde_json::to_string(&empty_file()).unwrap(),
                pipeline: None,
                sample: Some(sample.id.clone()),
                show_internal: false,
                at_micros: None,
                filter: None,
                attribution: false,
                after_dot: None,
                capture_filter: None,
            },
        );
        assert!(!response.succeeded(), "{} rows", response.rows.len());
        std::fs::remove_dir_all(&data).ok();
    }

    /// A group's routes preview against `shared/` merged in, as they run once deployed: a group route reads a source
    /// only `shared/` declares into a pipeline that calls a shared pipeline and reads a shared lookup table.
    #[test]
    fn a_group_route_preview_calls_a_shared_pipeline_and_reads_a_shared_table() {
        use smeltr::config_file;

        let data = std::env::temp_dir().join(format!("smeltr-route-preview-shared-{}", std::process::id()));
        let group = config_file::FileConfig {
            destinations: serde_json::from_value(serde_json::json!([
                { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
            ]))
            .unwrap(),
            routes: serde_json::from_value(serde_json::json!([
                { "id": "audit-orders", "source": "orders", "condition": null, "pipeline": "audit",
                  "deliver": [ { "to": ["es-prod"] } ] }
            ]))
            .unwrap(),
            ..audit_calling_shared()
        };
        let sample = config_file::SampleDto {
            id: "s".to_string(),
            name: "s".to_string(),
            source: Some("orders".to_string()),
            records: vec![smeltr::io::raw_object("{}".to_string())],
            columns: Vec::new(),
        };
        seed_west_over_shared(&data, &group, &sample);

        // The editor sends only the group's own bundle.
        let response = super::route_preview_on(
            &data,
            "work",
            "west",
            super::RoutePreviewRequest {
                config: serde_json::to_string(&group).unwrap(),
                sample: sample.id.clone(),
                at_micros: None,
            },
        );

        assert!(
            response.succeeded(),
            "{:?}",
            response.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(response.route_ids, vec!["audit-orders"]);
        let through = &response.rows[0].through;
        assert_eq!(through.len(), 1, "the group's route took the record");
        let document: serde_json::Value = serde_json::from_str(&through[0].documents[0].document).unwrap();
        assert_eq!(
            document["enriched"],
            serde_json::json!(true),
            "the shared pipeline ran: {document}"
        );
        assert_eq!(
            document["country"],
            serde_json::json!("US"),
            "the shared table's file resolved: {document}"
        );
        std::fs::remove_dir_all(&data).ok();
    }

    use super::{gate, origin_hosts, reached_at, resolve_listen_addr, resolve_origin};

    /// Every bind is allowed. What used to make a public one dangerous was a config API that answered without a
    /// credential; every operator route needs a session now, whatever this binds.
    #[test]
    fn any_address_may_be_bound() {
        let addr = resolve_listen_addr("127.0.0.1", 4000).unwrap();
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 4000);
        for bind in ["::1", "0.0.0.0", "10.0.0.5", "::"] {
            assert!(resolve_listen_addr(bind, 4000).is_ok(), "{bind}");
        }
    }

    #[test]
    fn a_malformed_bind_is_an_error() {
        assert!(resolve_listen_addr("not-an-ip", 4000).is_err());
    }

    /// The worker inside this process calls its leader where the leader listens, so a bind that does not take
    /// `127.0.0.1` still leaves it a way in.
    #[test]
    fn the_worker_inside_reaches_its_leader_where_it_listens() {
        for (bind, at) in [
            ("127.0.0.1", "127.0.0.1"),
            ("127.0.0.2", "127.0.0.2"),
            ("10.0.0.5", "10.0.0.5"),
            ("0.0.0.0", "127.0.0.1"),
            ("::1", "::1"),
            ("::", "::1"),
            ("fd00::5", "fd00::5"),
        ] {
            assert_eq!(reached_at(bind).to_string(), at, "{bind}");
        }
    }

    /// A leader serving its own certificate is reached at its own address, and the cookie is `Secure` because the
    /// browser really is speaking HTTPS.
    #[test]
    fn a_tls_leader_derives_its_own_https_origin() {
        let origin = resolve_origin(None, "127.0.0.1", 4000, true).unwrap();
        assert_eq!(origin.value, "https://127.0.0.1:4000");
        assert!(origin.secure);
    }

    /// TLS off on loopback is a laptop. The cookie cannot be `Secure` -- a browser would drop it over plain HTTP
    /// -- and it never leaves the machine.
    #[test]
    fn a_plain_http_leader_on_loopback_derives_an_insecure_origin() {
        let origin = resolve_origin(None, "127.0.0.1", 4000, false).unwrap();
        assert_eq!(origin.value, "http://127.0.0.1:4000");
        assert!(!origin.secure);
    }

    /// TLS off and a public bind is a proxy this process cannot see. Guessing would mean promising a cookie is
    /// encrypted in transit without knowing whether it is.
    #[test]
    fn plain_http_on_a_public_bind_must_name_its_origin() {
        for bind in ["0.0.0.0", "10.0.0.5", "::"] {
            let refused = resolve_origin(None, bind, 4000, false).unwrap_err();
            assert!(refused.contains("SMELTR_PUBLIC_ORIGIN"), "{bind}: {refused}");
        }
    }

    /// The proxy case: this process serves plain HTTP and the browser speaks HTTPS to something in front of it,
    /// so the cookie follows the origin rather than the socket.
    #[test]
    fn a_configured_https_origin_makes_the_cookie_secure_however_this_process_listens() {
        let origin = resolve_origin(Some("https://leader.example.com"), "0.0.0.0", 4000, false).unwrap();
        assert_eq!(origin.value, "https://leader.example.com");
        assert!(origin.secure);
    }

    /// A wildcard is an address to listen on, never one to be reached at.
    #[test]
    fn a_wildcard_bind_is_not_an_origin_host() {
        assert_eq!(
            resolve_origin(None, "0.0.0.0", 4000, true).unwrap().value,
            "https://127.0.0.1:4000"
        );
    }

    /// `localhost` and the loopback address are the same machine, and a browser may be pointed at either --
    /// unlike the configured case, nothing here says which one to expect.
    #[test]
    fn a_derived_loopback_origin_accepts_localhost_as_an_alias() {
        let origin = resolve_origin(None, "127.0.0.1", 4000, false).unwrap();
        assert!(origin.accepts("http://127.0.0.1:4000"));
        assert!(origin.accepts("http://localhost:4000"));
        assert!(
            !origin.accepts("http://localhost:4001"),
            "a different port is a different origin"
        );
        assert!(
            !origin.accepts("https://localhost:4000"),
            "a different scheme is a different origin"
        );
    }

    /// A browser never puts a scheme's default port in the `Origin` it sends -- `http://localhost`, not
    /// `http://localhost:80`. A leader listening on that port must accept the header exactly as a browser
    /// serializes it, in both the derived value and its loopback alias.
    #[test]
    fn a_derived_origin_on_a_default_port_carries_no_port() {
        let http = resolve_origin(None, "127.0.0.1", 80, false).unwrap();
        assert_eq!(http.value, "http://127.0.0.1");
        assert!(http.accepts("http://127.0.0.1"));
        assert!(http.accepts("http://localhost"));

        let https = resolve_origin(None, "127.0.0.1", 443, true).unwrap();
        assert_eq!(https.value, "https://127.0.0.1");
        assert!(https.accepts("https://127.0.0.1"));
        assert!(https.accepts("https://localhost"));
    }

    /// A configured origin is canonicalized the same way a derived one is -- naming the scheme's default port
    /// explicitly would otherwise never match the `Origin` a browser actually sends.
    #[test]
    fn a_configured_origin_on_a_default_port_carries_no_port() {
        let origin = resolve_origin(Some("https://leader.example.com:443"), "0.0.0.0", 4000, false).unwrap();
        assert_eq!(origin.value, "https://leader.example.com");
        assert!(origin.accepts("https://leader.example.com"));

        let ipv6 = resolve_origin(Some("http://[::1]:80"), "0.0.0.0", 4000, false).unwrap();
        assert_eq!(ipv6.value, "http://[::1]");
        assert!(ipv6.accepts("http://[::1]"));
    }

    /// What a certificate's SAN list needs from an origin: the host alone, scheme and port both discarded, an
    /// IPv6 literal's brackets included in the count of nothing extra.
    #[test]
    fn origin_hosts_strips_scheme_and_port_from_every_entry() {
        let origin = gate::Origin {
            value: "https://leader.example.com".to_string(),
            aliases: vec![
                "https://leader.internal:8443".to_string(),
                "https://[::1]:4000".to_string(),
            ],
            secure: true,
            proxied: false,
        };
        assert_eq!(
            origin_hosts(&origin),
            vec![
                "leader.example.com".to_string(),
                "leader.internal".to_string(),
                "::1".to_string(),
            ]
        );
    }

    /// Naming one origin accepts that one. With TLS off, nothing about it says a browser could just as well reach
    /// this leader at `localhost` instead, and a Secure cookie would not travel there anyway, so an origin not
    /// listed is refused.
    #[test]
    fn a_configured_origin_accepts_no_unlisted_alias() {
        let origin = resolve_origin(Some("http://127.0.0.1:4000"), "0.0.0.0", 4000, false).unwrap();
        assert!(origin.accepts("http://127.0.0.1:4000"));
        assert!(!origin.accepts("http://localhost:4000"));
    }

    /// A leader serving its own TLS is reached directly, and its certificate carries the loopback names for the
    /// worker inside it. A browser on its machine using either name is the leader's own page, so the origin
    /// check accepts every name the certificate carries, on the leader's port -- and nothing on another port,
    /// another scheme, or a name the certificate does not carry.
    #[test]
    fn a_tls_leader_accepts_its_loopback_names_beside_a_configured_origin() {
        let origin = resolve_origin(Some("https://192.168.1.97"), "0.0.0.0", 443, true).unwrap();
        assert_eq!(origin.value, "https://192.168.1.97");
        for name in smeltr::startup_config::LOOPBACK_NAMES {
            assert!(origin.accepts(&format!("https://{name}")), "{name}");
        }
        assert!(
            !origin.accepts("https://localhost:4000"),
            "a different port is a different origin"
        );
        assert!(
            !origin.accepts("http://localhost"),
            "a different scheme is a different origin"
        );
        assert!(!origin.accepts("https://[::1]"), "the certificate does not carry it");

        let listed = resolve_origin(
            Some("https://leader.example.com,https://localhost"),
            "0.0.0.0",
            443,
            true,
        )
        .unwrap();
        assert_eq!(
            listed.aliases,
            vec!["https://localhost".to_string(), "https://127.0.0.1".to_string()],
            "a loopback name already listed is not added twice"
        );
    }

    /// The same rule on a derived origin: a TLS leader bound to one loopback address answers to the other name too.
    #[test]
    fn a_derived_tls_origin_accepts_both_loopback_names() {
        let origin = resolve_origin(None, "::1", 4000, true).unwrap();
        assert_eq!(origin.value, "https://[::1]:4000");
        assert!(origin.accepts("https://localhost:4000"));
        assert!(origin.accepts("https://127.0.0.1:4000"));

        let v4 = resolve_origin(None, "127.0.0.1", 4000, true).unwrap();
        assert_eq!(
            v4.aliases,
            vec!["https://localhost:4000".to_string()],
            "its own value is not an alias"
        );
    }

    /// No browser reaches a leader bound to one non-loopback address at `localhost`, so that leader answers to no
    /// loopback name: the alias would let nothing legitimate in and would still stand in for a login's CSRF token.
    #[test]
    fn a_bind_that_does_not_reach_loopback_gets_no_loopback_alias() {
        let configured = resolve_origin(Some("https://192.168.1.97"), "192.168.1.97", 443, true).unwrap();
        assert!(configured.aliases.is_empty(), "{:?}", configured.aliases);
        assert!(!configured.accepts("https://localhost"));

        let derived = resolve_origin(None, "10.0.0.5", 4000, true).unwrap();
        assert_eq!(derived.value, "https://10.0.0.5:4000");
        assert!(derived.aliases.is_empty(), "{:?}", derived.aliases);
    }

    /// A leader reached under more than one name lists them all, separated by commas.
    #[test]
    fn a_configured_origin_list_accepts_every_name_listed() {
        let origin = resolve_origin(
            Some("https://leader.example.com, https://leader.internal:8443"),
            "0.0.0.0",
            4000,
            false,
        )
        .unwrap();
        assert!(origin.accepts("https://leader.example.com"));
        assert!(origin.accepts("https://leader.internal:8443"));
        assert!(!origin.accepts("https://someone-else.example.com"));
    }

    /// One cookie, one answer to whether it is encrypted in transit -- a list mixing http and https names two
    /// leaders that disagree on that, which is not a configuration this leader can serve.
    #[test]
    fn a_configured_origin_list_may_not_mix_http_and_https() {
        let refused = resolve_origin(
            Some("https://leader.example.com,http://127.0.0.1:8443"),
            "0.0.0.0",
            4000,
            false,
        )
        .unwrap_err();
        assert!(refused.contains("disagree"), "{refused}");
    }

    /// This leader's socket speaks TLS or it does not; a plain-HTTP origin while it does is not the proxy case
    /// (that leader runs with TLS off), it is a configuration that named the wrong scheme.
    #[test]
    fn a_configured_plain_origin_is_refused_while_this_leader_serves_its_own_tls() {
        let refused = resolve_origin(Some("http://127.0.0.1"), "0.0.0.0", 4000, true).unwrap_err();
        assert!(refused.contains("SMELTR_LEADER_TLS"), "{refused}");

        // Off, the very same origin is exactly the proxy case and is accepted.
        assert!(resolve_origin(Some("http://127.0.0.1"), "0.0.0.0", 4000, false).is_ok());
    }

    #[test]
    fn a_configured_origin_keeps_no_trailing_slash() {
        assert_eq!(
            resolve_origin(Some("https://leader.example.com/"), "0.0.0.0", 4000, false)
                .unwrap()
                .value,
            "https://leader.example.com"
        );
    }

    /// A plain-HTTP origin sends the session cookie in the clear. Allowed where the network is one machine, and
    /// refused where it is not.
    #[test]
    fn a_plain_http_origin_is_refused_unless_it_names_this_machine() {
        for local in ["http://localhost:4000", "http://127.0.0.1:4000", "http://[::1]:4000"] {
            assert!(resolve_origin(Some(local), "0.0.0.0", 4000, false).is_ok(), "{local}");
        }
        for remote in ["http://leader.example.com", "http://10.0.0.5:4000"] {
            let refused = resolve_origin(Some(remote), "0.0.0.0", 4000, false).unwrap_err();
            assert!(refused.contains("unencrypted"), "{remote}: {refused}");
        }
    }

    /// An IPv6 literal is bracketed and full of colons, so the port separator is not simply the first one.
    #[test]
    fn a_host_is_read_out_of_an_authority_whatever_shape_it_is() {
        use super::host_of;
        assert_eq!(host_of("leader.example.com"), "leader.example.com");
        assert_eq!(host_of("leader.example.com:4000"), "leader.example.com");
        assert_eq!(host_of("[::1]:4000"), "::1");
        assert_eq!(host_of("[2001:db8::1]"), "2001:db8::1");
        assert_eq!(host_of("127.0.0.1:4000"), "127.0.0.1");
    }

    #[test]
    fn something_that_is_not_an_origin_is_refused() {
        for bad in ["leader.example.com", "https://", "ftp://leader", "", "://x"] {
            assert!(resolve_origin(Some(bad), "127.0.0.1", 4000, true).is_err(), "{bad:?}");
        }
    }

    /// `RoutePreviewThrough::route` and `RoutePreviewRow::matched` are indices into the compiled topology, which
    /// has no id of its own; `route_ids` is what resolves one of those indices back to the id the config gave that
    /// route, stable across an edit that reindexes the routes above it.
    #[test]
    fn route_ids_resolves_an_index_to_the_id_the_config_gave_that_route() {
        use smeltr::config_file;

        let config = r#"{
          "version": 1,
          "sources": [
            { "id": "orders", "type": "kafka", "brokers": "b1:9092", "topics": ["orders"], "groupId": "smeltr" }
          ],
          "destinations": [
            { "id": "es-prod", "type": "elasticsearch", "url": "http://es:9200", "defaultIndex": "orders" }
          ],
          "pipelines": [
            { "id": "audit", "name": "Audit", "steps": [
              { "id": "mark", "type": "eval", "assignments": [ { "target": "seen", "expr": "true" } ] }
            ] }
          ],
          "routes": [
            { "id": "first-route", "source": "orders", "condition": null, "pipeline": "audit",
              "deliver": [ { "to": ["es-prod"] } ] },
            { "id": "second-route", "source": "orders", "condition": null, "pipeline": "audit",
              "deliver": [ { "to": ["es-prod"] } ] }
          ]
        }"#;
        let sample = config_file::SampleDto {
            id: "s1".to_string(),
            name: "s1".to_string(),
            source: Some("orders".to_string()),
            records: vec![smeltr::io::raw_object("{}".to_string())],
            columns: Vec::new(),
        };
        let at = config_file::BundleAt::Dir(std::path::PathBuf::from("."));
        let declared_at = super::single_origin_declared_at(config, &at);

        let response = super::run_route_preview(config, sample, declared_at, None);

        assert!(
            response.succeeded(),
            "{:?}",
            response.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        assert_eq!(response.route_ids, vec!["first-route", "second-route"]);
        // A locked test records the pipeline's id, which a rename keeps; a reader is shown its name.
        assert_eq!(response.route_pipeline_ids, vec!["audit", "audit"]);
        assert_eq!(response.route_pipelines, vec!["Audit", "Audit"]);
    }

    /// Reuses `config_file::Links` rather than a second walk over `StepDto::Call`, so a pipeline several
    /// others call reports every one of them.
    #[test]
    fn pipeline_callers_reports_every_pipeline_that_calls_one() {
        use smeltr::config_file::{FileConfig, PipelineDto, RawBundle, StepDto};

        let calls = |target: &str| StepDto::Call {
            id: format!("call-{target}"),
            filter: None,
            disabled: false,
            pipeline: target.to_string(),
        };
        let pipeline = |id: &str, steps: Vec<StepDto>| PipelineDto {
            id: id.to_string(),
            name: id.to_string(),
            steps,
        };
        let raw = RawBundle {
            file: FileConfig {
                version: 1,
                sources: vec![],
                destinations: vec![],
                pipelines: vec![
                    pipeline("orders", vec![calls("enrich-geo")]),
                    pipeline("clicks", vec![calls("enrich-geo")]),
                    pipeline("audit", vec![calls("enrich-geo")]),
                    pipeline("enrich-geo", vec![]),
                ],
                routes: vec![],
                lookup_tables: vec![],
                grok_patterns: vec![],
                on_exception: None,
                name: None,
            },
            unreadable: Vec::new(),
        };

        let response = super::run_pipeline_callers(raw);

        assert!(response.succeeded(), "{:?}", response.errors);
        assert_eq!(
            response.callers.get("enrich-geo"),
            Some(&vec!["audit".to_string(), "clicks".to_string(), "orders".to_string()])
        );
    }
}
