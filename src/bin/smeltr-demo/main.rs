//! `smeltr-demo`, the website demo's preview service, built only into the demo's image.
//!
//! The demo runs the real GUI in a visitor's browser over a leader the page itself plays: it holds the visitor's
//! bundle, samples and fleet. What only the engine can answer comes here -- a preview, a route preview, a save's
//! validation, a grok pattern's captures, a line breaker's verdict, which records a capture filter keeps, a scope's
//! tests and what they make of it, a branch's map, and what a captured sample reads as. Each request carries the
//! bundle and the sample it is about, so nothing is stored and one visitor never sees another. Every answer comes from
//! the functions the leader's own routes call (`smeltr::leader::preview`, `smeltr::leader::map`,
//! `smeltr::leader::onboard`).
//!
//! It is public and unauthenticated, so it reads nothing a request names but the lookup tables under
//! `SMELTR_DEMO_BUNDLE`'s `lookups/`, takes a bounded body, bounded samples and bounded routes, and runs as many
//! requests at once as there are cores. Rate limits, cross-origin headers and the network it may not reach are the
//! container's.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Json, State};
use axum::routing::{get, post};
use smeltr::config_file::{self, BundleAt, DeclaredAt, FileConfig, RawBundle, SampleDto, TestDto, TestSummary};
use smeltr::leader::envelope::{ApiError, Empty, Envelope};
use smeltr::leader::preview::{
    AfterDotRequest, CaptureFilterRequest, FormatRequest, Formatted, GrokCaptures, GrokCapturesRequest,
    LineBreakerRequest, Preview, PreviewRequest, RoutePreview, TestResult, captures_of, format_code,
    line_breaker_verdict, run_preview, run_route_preview, run_test,
};
use tokio::sync::Semaphore;

/// `work`, holding `_core` until it ends.
fn work_on<T>(_core: tokio::sync::OwnedSemaphorePermit, work: impl FnOnce() -> T) -> T {
    work()
}

/// A demo's bundle and samples are a few hundred records; anything far past that is not a visitor's.
const BODY_LIMIT_BYTES: usize = 4 * 1024 * 1024;

/// How long a request may wait for a core and run. A preview cannot be stopped part way, so one that runs past this
/// is answered as failed and finishes on its own, holding its core: the semaphore keeps such runs from piling up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The most routes a request's bundles may declare between them. A route preview runs every record through every
/// route, so routes and records together bound what one request costs.
const MAX_ROUTES: usize = 200;

/// `Err` with what to tell the visitor when a request is bigger than the demo runs: a sample past what a sample may
/// hold (`config_file::MAX_SAMPLE_RECORDS`), or more than [`MAX_ROUTES`] routes. Refused before it takes a core.
fn refuse_oversized<'a>(
    bundles: impl IntoIterator<Item = &'a FileConfig>,
    samples: impl IntoIterator<Item = &'a SampleDto>,
) -> Result<(), String> {
    let routes: usize = bundles.into_iter().map(|b| b.routes.len()).sum();
    if routes > MAX_ROUTES {
        return Err(format!(
            "the demo runs at most {MAX_ROUTES} routes and this request has {routes}"
        ));
    }
    samples
        .into_iter()
        .try_for_each(|s| config_file::refuse_sample_size(s.records.len()))
}

#[derive(Clone)]
struct Service {
    /// Where every lookup table a request declares resolves: its file must sit under this directory's `lookups/`.
    bundle: PathBuf,
    cores: Arc<Semaphore>,
}

/// The bundle a request is about: a worker group's own, merged with `shared/` as the group runs once deployed. With
/// no group, `shared` is the bundle.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bundle {
    #[serde(default)]
    shared: Option<FileConfig>,
    #[serde(default)]
    group: Option<FileConfig>,
    #[serde(default)]
    group_id: Option<String>,
}

impl Bundle {
    fn files(&self) -> impl Iterator<Item = &FileConfig> {
        self.shared.iter().chain(self.group.iter())
    }

    /// What a preview of `pipeline` runs, as the leader's does: the merged bundle with `pipeline`, or, naming none,
    /// the group's own first. A group with none of its own previews unmerged, and says so.
    fn previewed(self, pipeline: Option<String>) -> Result<(FileConfig, Option<String>), String> {
        match (self.group, pipeline) {
            (Some(group), None) if group.pipelines.is_empty() => Ok((group, None)),
            (group, pipeline) => {
                let bundle = Bundle { group, ..self };
                let (file, first) = bundle.merged()?;
                Ok((file, pipeline.or(first)))
            }
        }
    }

    /// The merged bundle, and the first pipeline that is the group's own.
    fn merged(self) -> Result<(FileConfig, Option<String>), String> {
        let raw = |file: FileConfig| RawBundle {
            file,
            unreadable: Vec::new(),
        };
        match (self.shared, self.group, self.group_id) {
            (Some(shared), Some(group), Some(id)) => {
                let first = group.pipelines.first().map(|p| p.id.clone());
                Ok((smeltr::leader::map::merged_file(&raw(shared), &raw(group), &id), first))
            }
            (shared, None, _) => {
                let file = shared.ok_or("a request names no bundle")?;
                Ok((file, None))
            }
            (_, Some(_), _) => Err("a group's bundle needs the shared bundle and the group's id beside it".into()),
        }
    }
}

impl Service {
    fn declared_at(&self, file: &FileConfig) -> DeclaredAt {
        let at = BundleAt::Dir(self.bundle.clone());
        file.lookup_tables.iter().map(|t| (t.id.clone(), at.clone())).collect()
    }

    /// `work` on a core, or a failure once [`REQUEST_TIMEOUT`] passes.
    async fn run<T: Send + 'static>(
        &self,
        failed: impl FnOnce(String) -> T,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let cores = self.cores.clone();
        let ran = tokio::time::timeout(REQUEST_TIMEOUT, async move {
            let core = cores.acquire_owned().await.map_err(|e| e.to_string())?;
            // Held by the run itself, not by this wait: a run past the timeout keeps its core until it ends.
            tokio::task::spawn_blocking(move || work_on(core, work))
                .await
                .map_err(|e| e.to_string())
        })
        .await;
        match ran {
            Ok(Ok(answer)) => answer,
            Ok(Err(e)) => failed(format!("internal error: {e}")),
            Err(_) => failed("this took too long for the demo to run".into()),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreviewBody {
    #[serde(flatten)]
    bundle: Bundle,
    #[serde(default)]
    pipeline: Option<String>,
    #[serde(default)]
    sample: Option<SampleDto>,
    #[serde(default)]
    show_internal: bool,
    #[serde(default)]
    at_micros: Option<i64>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    after_dot: Option<AfterDotRequest>,
    #[serde(default)]
    capture_filter: Option<CaptureFilterRequest>,
}

async fn preview(State(service): State<Service>, Json(body): Json<PreviewBody>) -> Json<Envelope<Preview>> {
    let failed = |e: String| Preview::failed("load", vec![ApiError::new(e)], vec![]);
    if let Err(e) = refuse_oversized(body.bundle.files(), body.sample.iter()) {
        return Json(failed(e));
    }
    let s = service.clone();
    Json(
        service
            .run(failed, move || {
                let (file, pipeline) = match body.bundle.previewed(body.pipeline) {
                    Ok(previewed) => previewed,
                    Err(e) => return failed(e),
                };
                let declared_at = s.declared_at(&file);
                let config = match serde_json::to_string(&file) {
                    Ok(config) => config,
                    Err(e) => return failed(e.to_string()),
                };
                let (columns, records) = body.sample.map(|s| (s.columns, s.records)).unwrap_or_default();
                let req = PreviewRequest {
                    config,
                    pipeline,
                    sample: None,
                    show_internal: body.show_internal,
                    at_micros: body.at_micros,
                    filter: body.filter,
                    after_dot: body.after_dot,
                    capture_filter: body.capture_filter,
                };
                run_preview(req, declared_at, &columns, records)
            })
            .await,
    )
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutePreviewBody {
    #[serde(flatten)]
    bundle: Bundle,
    sample: SampleDto,
    #[serde(default)]
    at_micros: Option<i64>,
}

async fn route_preview(
    State(service): State<Service>,
    Json(body): Json<RoutePreviewBody>,
) -> Json<Envelope<RoutePreview>> {
    if let Err(e) = refuse_oversized(body.bundle.files(), [&body.sample]) {
        return Json(Envelope::failed_one(e));
    }
    let s = service.clone();
    Json(
        service
            .run(Envelope::failed_one, move || {
                let (file, _) = match body.bundle.merged() {
                    Ok(merged) => merged,
                    Err(e) => return Envelope::failed_one(e),
                };
                let declared_at = s.declared_at(&file);
                match serde_json::to_string(&file) {
                    Ok(config) => run_route_preview(&config, body.sample, declared_at, body.at_micros),
                    Err(e) => Envelope::failed_one(e.to_string()),
                }
            })
            .await,
    )
}

/// What a save of this bundle would refuse it for, each error where the editor puts it. `ok` when a save would take
/// it.
async fn validate(State(service): State<Service>, Json(bundle): Json<Bundle>) -> Json<Envelope> {
    if let Err(e) = refuse_oversized(bundle.files(), []) {
        return Json(Envelope::failed_one(e));
    }
    let s = service.clone();
    Json(
        service
            .run(Envelope::failed_one, move || {
                let (file, _) = match bundle.merged() {
                    Ok(merged) => merged,
                    Err(e) => return Envelope::failed_one(e),
                };
                let declared_at = s.declared_at(&file);
                let errors = config_file::validation_errors_at(file, &declared_at);
                Envelope::with(Empty, errors.iter().map(ApiError::from_validation).collect())
            })
            .await,
    )
}

async fn grok_captures(
    State(service): State<Service>,
    Json(req): Json<GrokCapturesRequest>,
) -> Json<Envelope<GrokCaptures>> {
    Json(service.run(Envelope::failed_one, move || captures_of(req)).await)
}

async fn line_breaker(State(service): State<Service>, Json(req): Json<LineBreakerRequest>) -> Json<Envelope> {
    Json(
        service
            .run(Envelope::failed_one, move || line_breaker_verdict(req))
            .await,
    )
}

async fn format(State(service): State<Service>, Json(req): Json<FormatRequest>) -> Json<Envelope<Formatted>> {
    Json(service.run(Envelope::failed_one, move || format_code(req)).await)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestsBody {
    #[serde(flatten)]
    bundle: Bundle,
    /// Every test the scope holds.
    #[serde(default)]
    tests: Vec<TestDto>,
    /// Every sample the scope holds, which the tests run over.
    #[serde(default)]
    samples: Vec<SampleDto>,
    /// The tests to run, by id. None runs none: the summaries alone are a listing.
    #[serde(default)]
    run: Vec<String>,
}

#[derive(Default, serde::Serialize)]
struct Tests {
    tests: Vec<TestSummary>,
    results: Vec<TestResult>,
}

/// A scope's tests as the leader lists them, and what each named in `run` makes of the bundle, as the leader runs
/// them (`smeltr::leader::preview::run_test`).
async fn tests(State(service): State<Service>, Json(body): Json<TestsBody>) -> Json<Envelope<Tests>> {
    if let Err(e) = refuse_oversized(body.bundle.files(), &body.samples) {
        return Json(Envelope::failed_one(e));
    }
    let s = service.clone();
    Json(
        service
            .run(Envelope::failed_one, move || {
                let scope = body
                    .bundle
                    .group_id
                    .clone()
                    .unwrap_or_else(|| config_file::SHARED_DIR.to_string());
                let (file, _) = match body.bundle.merged() {
                    Ok(merged) => merged,
                    Err(e) => return Envelope::failed_one(e),
                };
                let declared_at = s.declared_at(&file);
                let config = match serde_json::to_string(&file) {
                    Ok(config) => config,
                    Err(e) => return Envelope::failed_one(e.to_string()),
                };
                let results = body
                    .run
                    .iter()
                    .filter_map(|id| body.tests.iter().find(|t| &t.id == id))
                    .map(|test| {
                        let sample = body.samples.iter().find(|s| s.id == test.sample).cloned();
                        run_test(test, &scope, &config, Ok(sample), &declared_at)
                    })
                    .collect();
                Envelope::ok(Tests {
                    tests: body.tests.iter().map(TestDto::summary).collect(),
                    results,
                })
            })
            .await,
    )
}

/// A whole branch: `shared/` and every worker group's own bundle.
#[derive(serde::Deserialize)]
struct Fleet {
    shared: FileConfig,
    #[serde(default)]
    groups: Vec<FleetGroup>,
}

#[derive(serde::Deserialize)]
struct FleetGroup {
    id: String,
    config: FileConfig,
}

/// The fleet's map of a branch, as the leader's `/api/map` draws it. The demo's fleet raises no warnings and keeps no
/// series; its measurements are the page's.
async fn map(State(service): State<Service>, Json(fleet): Json<Fleet>) -> Json<Envelope<smeltr::leader::map::Map>> {
    let bundles = std::iter::once(&fleet.shared).chain(fleet.groups.iter().map(|g| &g.config));
    if let Err(e) = refuse_oversized(bundles, []) {
        return Json(Envelope::failed_one(e));
    }
    let at = BundleAt::Dir(service.bundle.clone());
    let raw = |file: FileConfig| RawBundle {
        file,
        unreadable: Vec::new(),
    };
    Json(
        service
            .run(Envelope::failed_one, move || {
                let groups = fleet
                    .groups
                    .into_iter()
                    .map(|g| smeltr::leader::map::GroupBundle {
                        id: g.id,
                        raw: raw(g.config),
                        at: at.clone(),
                    })
                    .collect();
                Envelope::ok(smeltr::leader::map::build(raw(fleet.shared), &at, groups, &[], None))
            })
            .await,
    )
}

/// One object of a branch: the leader's `/api/connections` question, over the groups that run it.
#[derive(serde::Deserialize)]
struct ConnectionsBody {
    #[serde(flatten)]
    fleet: Fleet,
    kind: config_file::Kind,
    id: String,
}

/// What a change to one object reaches, as the leader's `/api/connections` answers it.
async fn connections(
    State(service): State<Service>,
    Json(body): Json<ConnectionsBody>,
) -> Json<Envelope<smeltr::leader::connections::ObjectConnections>> {
    let bundles = std::iter::once(&body.fleet.shared).chain(body.fleet.groups.iter().map(|g| &g.config));
    if let Err(e) = refuse_oversized(bundles, []) {
        return Json(Envelope::failed_one(e));
    }
    let at = BundleAt::Dir(service.bundle.clone());
    let raw = |file: FileConfig| RawBundle {
        file,
        unreadable: Vec::new(),
    };
    Json(
        service
            .run(Envelope::failed_one, move || {
                let groups: Vec<_> = body
                    .fleet
                    .groups
                    .into_iter()
                    .map(|g| smeltr::leader::map::GroupBundle {
                        id: g.id,
                        raw: raw(g.config),
                        at: at.clone(),
                    })
                    .collect();
                let object = config_file::Object::new(body.kind, &body.id);
                Envelope::ok(smeltr::leader::connections::of(
                    &raw(body.fleet.shared),
                    &groups,
                    &object,
                ))
            })
            .await,
    )
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptureFilterBody {
    #[serde(flatten)]
    bundle: Bundle,
    /// The source captured, by id.
    source: String,
    #[serde(default)]
    filter: Option<String>,
    /// What the source's workers read: the records the filter is asked about.
    sample: SampleDto,
}

#[derive(Default, serde::Serialize)]
struct Kept {
    /// By index into the sample's records.
    kept: Vec<usize>,
}

/// Which records a capture at a source keeps, by the filter a worker would compile for it
/// (`worker::capture::Filter`).
async fn capture_filter(State(service): State<Service>, Json(body): Json<CaptureFilterBody>) -> Json<Envelope<Kept>> {
    if let Err(e) = refuse_oversized(body.bundle.files(), [&body.sample]) {
        return Json(Envelope::failed_one(e));
    }
    Json(
        service
            .run(Envelope::failed_one, move || {
                let (file, _) = match body.bundle.merged() {
                    Ok(merged) => merged,
                    Err(e) => return Envelope::failed_one(e),
                };
                let Some(source) = file.sources.iter().find(|s| s.id == body.source) else {
                    return Envelope::failed_one(format!("this bundle declares no source {:?}", body.source));
                };
                let meta = source.kind.meta();
                let filter =
                    match smeltr::worker::capture::Filter::compile(body.filter.as_deref(), meta.clone(), &source.id) {
                        Ok(filter) => filter,
                        Err(e) => return Envelope::failed_one(e),
                    };
                let read = match smeltr::leader::preview::read_sample(&meta, &body.sample.columns, &body.sample.records)
                {
                    Ok(read) => read,
                    Err(e) => return Envelope::failed_one(e),
                };
                let texts: Vec<&str> = read.texts.iter().map(String::as_str).collect();
                let kept = filter.keeps(&read.records, &texts);
                Envelope::ok(Kept {
                    kept: kept.iter().enumerate().filter(|(_, k)| **k).map(|(i, _)| i).collect(),
                })
            })
            .await,
    )
}

#[derive(serde::Deserialize)]
struct OnboardBody {
    records: Vec<smeltr::io::RecordObject>,
    /// Every captured record's `__source`, in order, when the page sends fewer records than it captured.
    #[serde(default)]
    filed: Option<Vec<serde_json::Value>>,
}

/// What a captured sample reads as, answered as the leader's `/api/onboard/propose` answers it. A sample past what one
/// may hold is refused with 413.
async fn onboard_propose(State(service): State<Service>, Json(body): Json<OnboardBody>) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Err(e) = config_file::refuse_sample_size(body.records.len()) {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(Envelope::<Empty>::failed_one(e)),
        )
            .into_response();
    }
    let proposed = service
        .run(Err, move || {
            Ok(smeltr::leader::onboard::propose(&body.records, body.filed.as_deref()))
        })
        .await;
    match proposed {
        Ok(proposal) => Json(proposal).into_response(),
        Err(e) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(Envelope::<Empty>::failed_one(e)),
        )
            .into_response(),
    }
}

fn app(service: Service) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/preview", post(preview))
        .route("/route-preview", post(route_preview))
        .route("/validate", post(validate))
        .route("/grok-captures", post(grok_captures))
        .route("/line-breaker", post(line_breaker))
        .route("/format", post(format))
        .route("/capture-filter", post(capture_filter))
        .route("/map", post(map))
        .route("/connections", post(connections))
        .route("/tests", post(tests))
        .route("/onboard/propose", post(onboard_propose))
        .layer(DefaultBodyLimit::max(BODY_LIMIT_BYTES))
        .with_state(service)
}

fn main() {
    // The container runs this as PID 1, which the kernel lets ignore a SIGTERM it has no handler for, so the stop an
    // idle container is sent would never take. A preview holds nothing to drain.
    ctrlc::set_handler(|| std::process::exit(0)).expect("install the signal handler (called exactly once, at startup)");
    // The budgets a preview reads, refused now rather than part way through a request.
    smeltr::startup_config::check_lazy_settings();
    let bind = std::env::var("SMELTR_DEMO_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let bundle = PathBuf::from(std::env::var("SMELTR_DEMO_BUNDLE").unwrap_or_else(|_| "/demo".into()));
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let service = Service {
        bundle,
        cores: Arc::new(Semaphore::new(cores)),
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("start the async runtime")
        .block_on(async move {
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
            eprintln!("smeltr-demo: serving previews on {bind}");
            axum::serve(listener, app(service)).await.expect("serve");
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A service over a bundle directory holding `lookups/users.csv`, with `secret.csv` beside `lookups/` where no
    /// table may reach it.
    fn service(name: &str) -> Service {
        let dir = std::env::temp_dir().join(format!("smeltr-demo-{name}-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("lookups")).unwrap();
        std::fs::write(dir.join("lookups/users.csv"), "user,dept\nann,netsec\n").unwrap();
        std::fs::write(dir.join("secret.csv"), "user,dept\nann,leaked\n").unwrap();
        Service {
            bundle: dir,
            cores: Arc::new(Semaphore::new(1)),
        }
    }

    /// A bundle with one pipeline looking each record's text up in a CSV table at `path`.
    fn looking_up(path: &str) -> serde_json::Value {
        serde_json::json!({
            "shared": {
                "version": 1,
                "lookupTables": [{"id": "users", "type": "csv", "path": path, "delimiter": ","}],
                "pipelines": [{"id": "p", "name": "p", "steps": [{
                    "id": "l", "type": "lookup", "input": "_raw", "table": "users", "matchKind": "exact",
                    "keyColumn": "user", "outputs": [{"source": "dept", "field": "dept"}]
                }]}]
            }
        })
    }

    fn sample(records: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"id": "s", "name": "s", "records": records})
    }

    async fn previewed(service: &Service, mut body: serde_json::Value) -> serde_json::Value {
        body["sample"] = sample(serde_json::json!([{"_raw": "ann"}]));
        let Json(answer) = preview(State(service.clone()), Json(serde_json::from_value(body).unwrap())).await;
        serde_json::to_value(answer).unwrap()
    }

    #[tokio::test]
    async fn a_preview_reads_a_table_under_the_bundle_s_lookups() {
        let answer = previewed(&service("reads"), looking_up("lookups/users.csv")).await;
        assert_eq!(answer["ok"], true, "{answer}");
        assert!(answer.to_string().contains("netsec"), "{answer}");
    }

    #[tokio::test]
    async fn a_table_outside_the_bundle_s_lookups_is_refused() {
        let service = service("refused");
        for path in ["secret.csv", "lookups/../secret.csv", "/etc/passwd"] {
            let answer = previewed(&service, looking_up(path)).await;
            assert_eq!(answer["ok"], false, "{path}: {answer}");
            assert!(!answer.to_string().contains("leaked"), "{path}: {answer}");
        }
    }

    #[tokio::test]
    async fn validation_reports_what_a_save_would_refuse() {
        let service = service("validate");
        let good = serde_json::from_value(looking_up("lookups/users.csv")).unwrap();
        let Json(answer) = validate(State(service.clone()), Json(good)).await;
        assert!(answer.ok, "{:?}", answer.errors);

        let bad = serde_json::from_value(looking_up("lookups/missing.csv")).unwrap();
        let Json(answer) = validate(State(service), Json(bad)).await;
        assert!(!answer.ok);
    }

    #[tokio::test]
    async fn onboarding_proposes_from_the_records_sent_and_refuses_too_many() {
        let propose = |records: serde_json::Value| async move {
            let body = serde_json::from_value(serde_json::json!({ "records": records })).unwrap();
            let answer = onboard_propose(State(service("onboard")), Json(body)).await;
            let status = answer.status();
            let bytes = axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice::<serde_json::Value>(&bytes).unwrap())
        };
        let (status, proposal) = propose(serde_json::json!([{"_raw": "{\"a\": 1}"}])).await;
        assert_eq!(status, 200);
        assert_eq!(proposal["format"], "json", "{proposal}");

        let many = vec![serde_json::json!({"_raw": "x"}); config_file::MAX_SAMPLE_RECORDS + 1];
        let (status, refused) = propose(serde_json::json!(many)).await;
        assert_eq!(status, 413);
        assert_eq!(refused["ok"], false);
    }

    #[test]
    fn a_request_past_a_sample_s_records_or_the_demo_s_routes_is_refused() {
        let routed = |n: usize| -> FileConfig {
            let routes: Vec<_> = (0..n)
                .map(|i| serde_json::json!({"id": format!("r{i}"), "source": "s", "pipeline": "p", "deliver": []}))
                .collect();
            serde_json::from_value(serde_json::json!({"version": 1, "routes": routes})).unwrap()
        };
        let holding = |n: usize| -> SampleDto {
            serde_json::from_value(sample(serde_json::json!(vec![serde_json::json!({"_raw": "x"}); n]))).unwrap()
        };
        let (most, too_many) = (routed(MAX_ROUTES), routed(MAX_ROUTES + 1));
        let (full, over) = (
            holding(config_file::MAX_SAMPLE_RECORDS),
            holding(config_file::MAX_SAMPLE_RECORDS + 1),
        );

        assert!(refuse_oversized([&most], [&full]).is_ok());
        assert!(refuse_oversized([&too_many], []).is_err());
        assert!(
            refuse_oversized([&most, &routed(1)], []).is_err(),
            "routes count across a request's bundles"
        );
        assert!(refuse_oversized([&most], [&full, &over]).is_err());
    }

    #[tokio::test]
    async fn a_capture_filter_keeps_what_it_selects() {
        let body = serde_json::json!({
            "shared": {"version": 1, "sources": [{"id": "fw", "type": "syslogReceiver", "port": 1514}]},
            "source": "fw",
            "filter": "_raw != \"allow tcp\"",
            "sample": sample(serde_json::json!([{"_raw": "allow tcp"}, {"_raw": "deny udp"}, {"_raw": "deny tcp"}])),
        });
        let Json(answer) = capture_filter(State(service("filter")), Json(serde_json::from_value(body).unwrap())).await;
        assert!(answer.ok, "{:?}", answer.errors);
        assert_eq!(answer.body.kept, vec![1, 2]);
    }

    #[tokio::test]
    async fn a_group_with_no_pipeline_of_its_own_previews_unmerged() {
        let body = serde_json::json!({
            "shared": {"version": 1, "pipelines": [{"id": "s", "name": "s", "steps": [
                {"id": "c", "type": "code", "body": "from = \"shared\";"}]}]},
            "group": {"version": 1},
            "groupId": "network",
        });
        let answer = previewed(&service("unmerged"), body).await;
        assert_eq!(answer["ok"], false, "{answer}");
        assert!(!answer.to_string().contains("\"shared\""), "{answer}");
    }

    #[tokio::test]
    async fn a_group_previews_merged_with_shared() {
        let body = serde_json::json!({
            "shared": {"version": 1, "pipelines": [{"id": "s", "name": "s", "steps": [
                {"id": "c", "type": "code", "body": "from = \"shared\";"}]}]},
            "group": {"version": 1, "pipelines": [{"id": "g", "name": "g", "steps": [
                {"id": "c", "type": "code", "body": "from = \"group\";"}]}]},
            "groupId": "network",
        });
        let answer = previewed(&service("merged"), body).await;
        assert_eq!(answer["ok"], true, "{answer}");
        assert!(answer.to_string().contains("\"group\""), "{answer}");
    }

    #[tokio::test]
    async fn a_map_draws_each_group_s_sources() {
        let body = serde_json::json!({
            "shared": {"version": 1},
            "groups": [{"id": "network", "config": {"version": 1, "sources": [
                {"id": "firewalls", "type": "syslogReceiver", "port": 5514}]}}],
        });
        let Json(answer) = map(State(service("map")), Json(serde_json::from_value(body).unwrap())).await;
        let answer = serde_json::to_value(answer).unwrap();
        assert_eq!(answer["ok"], true, "{answer}");
        assert!(answer.to_string().contains("firewalls"), "{answer}");
    }

    #[tokio::test]
    async fn a_shared_pipeline_connects_to_each_group_that_routes_it() {
        let body = serde_json::json!({
            "shared": {"version": 1, "pipelines": [{"id": "normalize", "name": "n", "steps": []}]},
            "groups": [
                {"id": "edge", "config": {"version": 1,
                    "sources": [{"id": "fw", "type": "syslogReceiver", "port": 5514}],
                    "routes": [{"id": "r", "source": "fw", "pipeline": "normalize", "deliver": []}]}},
                {"id": "core", "config": {"version": 1}},
            ],
            "kind": "pipeline",
            "id": "normalize",
        });
        let Json(answer) = connections(
            State(service("connections")),
            Json(serde_json::from_value(body).unwrap()),
        )
        .await;
        assert!(answer.ok, "{:?}", answer.errors);
        assert_eq!(answer.body.groups, vec!["edge", "core"]);
        assert_eq!(answer.body.drawn_in, vec!["edge"]);
    }

    #[tokio::test]
    async fn a_locked_test_passes_until_its_pipeline_changes() {
        let bundle = |body: &str| {
            serde_json::json!({"version": 1, "pipelines": [{"id": "p", "name": "p", "steps": [
                {"id": "c", "type": "code", "body": body}]}]})
        };
        let service = service("tests");
        let at = 1_790_000_000_000_000_i64;
        let locked = previewed(
            &service,
            serde_json::json!({"shared": bundle("from = \"a\";"), "atMicros": at}),
        )
        .await;
        let expected: Vec<_> = locked["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["json"].clone())
            .collect();
        let test = serde_json::json!({"id": "t", "name": "t", "pipeline": "p", "sample": "s", "lockedAt": at,
            "rowsIn": 1, "expected": expected});
        let run = |body: &str| {
            let request = serde_json::json!({"shared": bundle(body), "tests": [test.clone()],
                "samples": [sample(serde_json::json!([{"_raw": "ann"}]))], "run": ["t"]});
            let service = service.clone();
            async move {
                let Json(answer) = tests(State(service), Json(serde_json::from_value(request).unwrap())).await;
                serde_json::to_value(answer).unwrap()
            }
        };

        let passing = run("from = \"a\";").await;
        assert_eq!(passing["tests"][0]["rowsIn"], 1, "{passing}");
        assert_eq!(passing["results"][0]["verdict"]["passed"], true, "{passing}");
        let failing = run("from = \"b\";").await;
        assert_eq!(failing["results"][0]["verdict"]["passed"], false, "{failing}");
    }
}
