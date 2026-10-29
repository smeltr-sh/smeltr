//! Forgetting an ephemeral worker once it is gone.
//!
//! An ephemeral worker's identity lives and dies with the pod or process holding it, so once it is gone it does not
//! come back as itself: whatever replaces it enrolls as a new worker. Kept, it would grow the roster by every pod an
//! autoscaler ever started, and hold every rotation of the leader's authority open for workers that will never
//! answer. So the leader forgets it: its credential, the enrollment code's record of bringing it in, and every row the
//! registry holds for it. The roster, the registry and a rotation's wait then have nothing of it to account for.
//!
//! Gone is one of two things. The worker said it was stopping, and is forgotten on that heartbeat. Or it has said
//! nothing for longer than this leader's setting allows, and is forgotten by the next look -- which rides the
//! heartbeat and the pages that read the fleet, since the leader has no clock of its own. Until then its silence is
//! reported like any other worker's.
//!
//! Silence is what this leader was up to hear. While it was down it heard nobody, and that says nothing about who
//! was still there: a fleet that ran on through a leader outage is not forgotten for it.
//!
//! A revoked worker is never forgotten: revoking it is an operator saying it stays out, and a forgotten worker's
//! credential is one it could enroll past with the enrollment code it still holds.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use axum::Json;
use axum::extract::State;

use crate::leader::envelope::{Envelope, Refusal};
use crate::leader::registry::ThreadHealth;
use crate::leader::store::{Actor, Store, WorkerCredentialRecord, WorkerKind};
use crate::leader::{AppState, run_store};

/// Where the setting is kept. Seconds, as the session deadlines are.
pub const FORGET_AFTER_KEY: &str = "workers.ephemeral_forget_after_seconds";

/// How long an ephemeral worker may be silent before it is forgotten, where nobody has said. Past a container
/// restarting in the same pod, so a crash is not a new worker; short enough that a lost node's workers are gone
/// from the roster and from any rotation's wait well inside the hour.
pub const DEFAULT_FORGET_AFTER: Duration = Duration::from_secs(10 * 60);

/// Its bounds. Silence shorter than a minute is a few missed heartbeats, not a worker gone. Longer than a month, and
/// an autoscaled fleet carries a month of pods it will never see again.
pub const MIN_FORGET_AFTER: Duration = Duration::from_secs(60);
pub const MAX_FORGET_AFTER: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// How often the fleet is looked at for gone workers, however many heartbeats and page reads ask.
const LOOK_EVERY: Duration = Duration::from_secs(10);

/// How long an ephemeral worker may be silent before it is forgotten: what the operator set, or the default.
///
/// A stored value that is no longer allowed -- written by hand, or by an older build with other bounds -- is
/// reported rather than replaced.
pub fn forget_after(store: &dyn Store) -> Result<Duration, String> {
    match store.setting(FORGET_AFTER_KEY).map_err(|e| e.to_string())? {
        None => Ok(DEFAULT_FORGET_AFTER),
        Some(text) => {
            let seconds: u64 = text
                .parse()
                .map_err(|_| format!("{FORGET_AFTER_KEY} holds {text:?}, which is not a number of seconds"))?;
            checked(Duration::from_secs(seconds))
        }
    }
}

fn checked(forget_after: Duration) -> Result<Duration, String> {
    if forget_after < MIN_FORGET_AFTER || forget_after > MAX_FORGET_AFTER {
        return Err(format!(
            "an ephemeral worker is forgotten after between {} seconds and {} days of silence, not {} seconds",
            MIN_FORGET_AFTER.as_secs(),
            MAX_FORGET_AFTER.as_secs() / (24 * 60 * 60),
            forget_after.as_secs()
        ));
    }
    Ok(forget_after)
}

/// Forgets `worker_id` if it is ephemeral and not revoked. What a heartbeat saying an ephemeral worker is stopping
/// ends in.
pub async fn forget(state: &AppState, worker_id: &str) {
    let id = worker_id.to_string();
    match run_store(state.store.clone(), move |s| s.forget_worker(&id)).await {
        Ok(true) => {
            state.registry.forget_worker(worker_id);
            state.directives.forget(worker_id);
            free_holds(state, worker_id).await;
            tracing::info!(
                target: "leader.worker_forgotten",
                subject = %worker_id,
                "an ephemeral worker stopped, so this leader forgot it"
            );
        }
        Ok(false) => {}
        Err(e) => tracing::warn!(
            target: "leader.worker_not_forgotten",
            subject = %worker_id,
            error = %e,
            "an ephemeral worker stopped and could not be forgotten, so it is looked for again"
        ),
    }
}

/// Frees the single-worker sources `worker_id` held, off the async threads: a beat holds the assignment while it
/// writes the store.
pub(crate) async fn free_holds(state: &AppState, worker_id: &str) {
    let (registry, worker) = (state.registry.clone(), worker_id.to_string());
    if let Err(e) = tokio::task::spawn_blocking(move || registry.holds.forget(&worker)).await {
        tracing::warn!(
            target: "leader.holds_not_freed",
            subject = %worker_id,
            error = %e,
            "a gone worker's single-worker sources wait out the hold time instead of moving at once"
        );
    }
}

/// Forgets every ephemeral worker that has been silent past this leader's setting, at most once every
/// [`LOOK_EVERY`] however often it is asked.
///
/// A worker the registry holds rows for and the roster does not know is forgotten on the same terms. The one way
/// there is a heartbeat already past the gate when its worker was forgotten, and nothing else would ever remove
/// what it wrote.
pub async fn sweep(state: &AppState, now_ms: i64) {
    if !state.registry.sweep_due(now_ms, LOOK_EVERY) {
        return;
    }
    let read = run_store(state.store.clone(), |s| {
        let forget_after = forget_after(s).map_err(crate::leader::store::StoreError::failed)?;
        Ok((forget_after, s.list_worker_credentials()?))
    })
    .await;
    let (forget_after, credentials) = match read {
        Ok(read) => read,
        Err(e) => {
            tracing::warn!(
                target: "leader.forgetting_unreadable",
                error = %e,
                "could not read which ephemeral workers are gone, so none is forgotten this time"
            );
            return;
        }
    };
    let (rostered, unrostered) = gone(
        &credentials,
        &state.registry.snapshot(),
        now_ms,
        forget_after,
        state.registry.up_for(),
    );

    let mut forgotten = Vec::new();
    for worker_id in rostered {
        let id = worker_id.clone();
        match run_store(state.store.clone(), move |s| s.forget_worker(&id)).await {
            Ok(true) => {
                state.registry.forget_worker(&worker_id);
                state.directives.forget(&worker_id);
                free_holds(state, &worker_id).await;
                forgotten.push(worker_id);
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(
                target: "leader.worker_not_forgotten",
                subject = %worker_id,
                error = %e,
                "a gone ephemeral worker could not be forgotten, so it is looked for again"
            ),
        }
    }
    for worker_id in unrostered {
        state.registry.forget_worker(&worker_id);
        state.directives.forget(&worker_id);
        free_holds(state, &worker_id).await;
        forgotten.push(worker_id);
    }
    if !forgotten.is_empty() {
        tracing::info!(
            target: "leader.workers_forgotten",
            workers = %forgotten.join(", "),
            forget_after_secs = forget_after.as_secs(),
            "ephemeral workers silent longer than this leader keeps them were forgotten"
        );
    }
}

/// Which workers are gone, as of `now_ms`: the ephemeral, unrevoked workers of `credentials` silent longer than
/// `forget_after`, and the workers `snapshot` holds rows for that `credentials` does not name, silent as long.
///
/// A worker's silence is its freshest row's age. One never heard from has been silent since it enrolled. Neither
/// counts for longer than `up_for`, which is how long this leader has been there to hear.
fn gone(
    credentials: &[WorkerCredentialRecord],
    snapshot: &[ThreadHealth],
    now_ms: i64,
    forget_after: Duration,
    up_for: Duration,
) -> (Vec<String>, Vec<String>) {
    let forget_after_ms = forget_after.as_millis() as i64;
    let up_ms = i64::try_from(up_for.as_millis()).unwrap_or(i64::MAX);
    let silent_too_long = |silent_ms: i64| silent_ms.min(up_ms) > forget_after_ms;
    let mut silent_for: HashMap<&str, i64> = HashMap::new();
    for thread in snapshot {
        let ago = thread.last_seen_millis_ago as i64;
        silent_for
            .entry(thread.worker_id.as_str())
            .and_modify(|held| *held = (*held).min(ago))
            .or_insert(ago);
    }

    let rostered: Vec<String> = credentials
        .iter()
        .filter(|c| c.kind == WorkerKind::Ephemeral && !c.revoked)
        .filter(|c| match silent_for.get(c.worker_id.as_str()) {
            Some(ago) => silent_too_long(*ago),
            None => c.enrolled_at.is_some_and(|at| silent_too_long(now_ms - at)),
        })
        .map(|c| c.worker_id.clone())
        .collect();
    let named: HashSet<&str> = credentials.iter().map(|c| c.worker_id.as_str()).collect();
    let mut unrostered: Vec<String> = silent_for
        .into_iter()
        .filter(|(worker_id, ago)| !named.contains(worker_id) && silent_too_long(*ago))
        .map(|(worker_id, _)| worker_id.to_string())
        .collect();
    unrostered.sort();
    (rostered, unrostered)
}

/// `GET` and `PUT /api/settings/workers`: how long an ephemeral worker may be silent before it is forgotten, the
/// bounds the form may offer, and the default it marks a change from. Seconds on the wire.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSettingsDto {
    ephemeral_forget_after_seconds: u64,
    default_seconds: u64,
    min_seconds: u64,
    max_seconds: u64,
}

#[derive(Default, serde::Serialize)]
pub struct WorkerSettings {
    settings: Option<WorkerSettingsDto>,
}

impl WorkerSettings {
    fn answer(forget_after: Result<Duration, Refusal>) -> Envelope<WorkerSettings> {
        match forget_after {
            Ok(forget_after) => Envelope::ok(WorkerSettings {
                settings: Some(WorkerSettingsDto {
                    ephemeral_forget_after_seconds: forget_after.as_secs(),
                    default_seconds: DEFAULT_FORGET_AFTER.as_secs(),
                    min_seconds: MIN_FORGET_AFTER.as_secs(),
                    max_seconds: MAX_FORGET_AFTER.as_secs(),
                }),
            }),
            Err(refusal) => Envelope::refused(refusal),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutWorkerSettings {
    ephemeral_forget_after_seconds: u64,
}

/// `GET /api/settings/workers`.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<WorkerSettings> {
    // A stored setting the bounds no longer allow is the form's to correct.
    let read = run_store(state.store, |s| Ok(forget_after(s).map_err(Refusal::invalid))).await;
    WorkerSettings::answer(read.unwrap_or_else(|e| Err(e.into())))
}

/// `PUT /api/settings/workers`. Recorded with who set it, in the transaction that sets it.
///
/// `actor` is optional for the reason [`crate::leader::enrollment_codes::mint`]'s is: a router composed without a gate.
pub async fn put_settings(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<PutWorkerSettings>,
) -> Envelope<WorkerSettings> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let forget_after = match checked(Duration::from_secs(request.ephemeral_forget_after_seconds)) {
        Ok(forget_after) => forget_after,
        Err(e) => return WorkerSettings::answer(Err(Refusal::invalid(e))),
    };
    let event = super::accounts::event_for(&actor, "worker-settings-change", "", "ok");
    let written = run_store(state.store, move |s| {
        s.put_settings(&[(FORGET_AFTER_KEY, forget_after.as_secs().to_string())], Some(&event))
    })
    .await;
    WorkerSettings::answer(written.map(|()| forget_after).map_err(Refusal::from))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat::{ThreadHeartbeat, ThreadKind};

    const NOW: i64 = 1_800_000_000_000;
    const TEN_MINUTES: Duration = Duration::from_secs(10 * 60);

    fn worker(worker_id: &str, kind: WorkerKind, revoked: bool, enrolled_ago_ms: i64) -> WorkerCredentialRecord {
        WorkerCredentialRecord {
            worker_id: worker_id.to_string(),
            token_hash: format!("hash-{worker_id}"),
            revoked,
            used: true,
            kind,
            enrolled_at: Some(NOW - enrolled_ago_ms),
        }
    }

    fn row(worker_id: &str, thread_id: &str, silent_ms: u64) -> ThreadHealth {
        ThreadHealth {
            worker_id: worker_id.to_string(),
            last_seen_millis_ago: silent_ms,
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            supervising: None,
            resources: None,
            authority_expires_millis: None,
            stopped_at_millis: None,
            report: ThreadHeartbeat {
                thread_id: thread_id.to_string(),
                group_id: "g".to_string(),
                kind: Some(ThreadKind::Reader),
                partitions_owned: vec![],
                metrics: vec![],
                metrics_held: false,
            },
        }
    }

    const ELEVEN_MINUTES: u64 = 11 * 60 * 1000;

    /// A leader up long enough to have heard every silence these tests describe.
    const UP_A_DAY: Duration = Duration::from_secs(24 * 60 * 60);

    /// Gone is ephemeral, unrevoked and silent past the setting, where silent is the freshest row's age. A
    /// permanent worker is missed rather than forgotten, and a revoked one is kept so it stays out.
    #[test]
    fn only_an_ephemeral_worker_silent_past_the_setting_is_gone() {
        let credentials = [
            worker("pod-gone", WorkerKind::Ephemeral, false, 0),
            worker("pod-quiet-briefly", WorkerKind::Ephemeral, false, 0),
            worker("pod-with-one-fresh-thread", WorkerKind::Ephemeral, false, 0),
            worker("vm-gone", WorkerKind::Permanent, false, 0),
            worker("pod-revoked", WorkerKind::Ephemeral, true, 0),
        ];
        let snapshot = [
            row("pod-gone", "t0", ELEVEN_MINUTES),
            row("pod-quiet-briefly", "t0", 60_000),
            row("pod-with-one-fresh-thread", "old", ELEVEN_MINUTES),
            row("pod-with-one-fresh-thread", "t0", 1_000),
            row("vm-gone", "t0", ELEVEN_MINUTES),
            row("pod-revoked", "t0", ELEVEN_MINUTES),
        ];
        let (rostered, unrostered) = gone(&credentials, &snapshot, NOW, TEN_MINUTES, UP_A_DAY);
        assert_eq!(rostered, ["pod-gone"]);
        assert!(unrostered.is_empty());
    }

    /// One never heard from has been silent since an enrollment code brought it in: a pod killed before its first
    /// heartbeat is as gone as one killed after it.
    #[test]
    fn a_worker_never_heard_from_is_silent_since_it_enrolled() {
        let credentials = [
            worker("pod-long-ago", WorkerKind::Ephemeral, false, ELEVEN_MINUTES as i64),
            worker("pod-just-now", WorkerKind::Ephemeral, false, 1_000),
            WorkerCredentialRecord {
                enrolled_at: None,
                ..worker("pod-unknown", WorkerKind::Ephemeral, false, 0)
            },
        ];
        let (rostered, _) = gone(&credentials, &[], NOW, TEN_MINUTES, UP_A_DAY);
        assert_eq!(rostered, ["pod-long-ago"], "one with no enrollment date is left alone");
    }

    /// Rows for a worker the roster does not name are what a heartbeat already past the gate writes as its worker
    /// is forgotten. Nothing else would ever remove them, so they go on the same terms.
    #[test]
    fn rows_the_roster_does_not_name_go_on_the_same_terms() {
        let snapshot = [row("forgotten-mid-beat", "t0", ELEVEN_MINUTES), row("new", "t0", 1_000)];
        let (rostered, unrostered) = gone(&[], &snapshot, NOW, TEN_MINUTES, UP_A_DAY);
        assert!(rostered.is_empty());
        assert_eq!(unrostered, ["forgotten-mid-beat"]);
    }

    /// A leader back from an outage longer than the setting finds every row it kept older than that. The fleet may
    /// have run on the whole time, so none of it is gone until the leader has itself been up that long.
    #[test]
    fn a_leader_forgets_nobody_for_its_own_downtime() {
        let credentials = [
            worker("pod-ran-on", WorkerKind::Ephemeral, false, 0),
            worker("pod-never-heard", WorkerKind::Ephemeral, false, ELEVEN_MINUTES as i64),
        ];
        let snapshot = [
            row("pod-ran-on", "t0", ELEVEN_MINUTES),
            row("unnamed", "t0", ELEVEN_MINUTES),
        ];

        let just_back = gone(&credentials, &snapshot, NOW, TEN_MINUTES, Duration::from_secs(30));
        assert_eq!(just_back, (Vec::<String>::new(), Vec::<String>::new()));

        let back_long_enough = gone(&credentials, &snapshot, NOW, TEN_MINUTES, ELEVEN_MINUTES_UP);
        assert_eq!(
            back_long_enough,
            (
                vec!["pod-ran-on".to_string(), "pod-never-heard".to_string()],
                vec!["unnamed".to_string()]
            )
        );
    }

    const ELEVEN_MINUTES_UP: Duration = Duration::from_millis(ELEVEN_MINUTES);

    #[test]
    fn the_setting_is_held_to_its_bounds() {
        assert!(checked(MIN_FORGET_AFTER).is_ok());
        assert!(checked(MAX_FORGET_AFTER).is_ok());
        assert!(checked(MIN_FORGET_AFTER - Duration::from_secs(1)).is_err());
        assert!(checked(MAX_FORGET_AFTER + Duration::from_secs(1)).is_err());
        assert!((MIN_FORGET_AFTER..=MAX_FORGET_AFTER).contains(&DEFAULT_FORGET_AFTER));
    }
}
