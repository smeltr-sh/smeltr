//! `GET /api/cluster` for the Cluster page: the leader's own snapshots, read in process.
//!
//! The gate let in anyone viewing somewhere, so the snapshots are narrowed to the groups the caller holds
//! ([`ClusterStatus::visible_to`]).

use smeltr::leader::access::{Access, Level};
use smeltr::leader::envelope::Envelope;

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatus {
    /// Set when either read below failed. The two lists are then `None`, not empty: a failed read must not render
    /// as "no worker has ever enrolled".
    error: Option<String>,
    health: Option<smeltr::leader::HealthResponse>,
    /// What the whole fleet runs, one answer for every group. `None` on a leader nothing has been deployed on
    /// yet -- which is why a failed read sets `error` instead of reporting `None`.
    deployed_commit: Option<String>,
    worker_groups: Option<Vec<crate::worker_groups_api::WorkerGroupOut>>,
    /// Every worker that has ever enrolled; the page joins it with `health.threads` by worker id.
    workers: Option<Vec<smeltr::leader::WorkerOut>>,
}

impl ClusterStatus {
    /// Only what `access` holds viewer in. A worker is the caller's when one of its threads heartbeats in a group
    /// they hold; one that has never heartbeated has named no group, and only an administrator sees it.
    fn visible_to(mut self, access: &Access) -> ClusterStatus {
        if access.is_administrator() {
            return self;
        }
        let holds = |group: &str| access.allows(group, Level::Viewer);
        if let Some(health) = &mut self.health {
            health.threads.retain(|t| holds(&t.report.group_id));
            health.groups.retain(|g| holds(&g.group_id));
        }
        let theirs: std::collections::HashSet<String> = self
            .health
            .iter()
            .flat_map(|h| h.threads.iter().map(|t| t.worker_id.clone()))
            .collect();
        if let Some(workers) = &mut self.workers {
            workers.retain(|w| theirs.contains(&w.worker_id));
        }
        if let Some(groups) = &mut self.worker_groups {
            groups.retain(|g| holds(&g.id));
        }
        self
    }
}

pub async fn cluster_status(
    axum::extract::State(state): axum::extract::State<smeltr::leader::AppState>,
    axum::Extension(access): axum::Extension<Access>,
) -> Envelope<ClusterStatus> {
    Envelope::ok(read(state).await.visible_to(&access))
}

async fn read(state: smeltr::leader::AppState) -> ClusterStatus {
    let repo = smeltr::leader::git::repo_path(&state.data_dir);
    // `None` from `deployed` means nothing has been deployed. A panicked read means the leader does not know, and
    // saying "nothing is deployed" to a fleet that is running would be the same lie the store reads refuse to tell.
    let head = smeltr::leader::blocking(move || Ok::<_, String>(smeltr::leader::git::deployed(&repo))).await;
    let (deployed_commit, head_error) = match head {
        Ok(commit) => (commit, None),
        Err(e) => (None, Some(format!("reading what the fleet runs: {e}"))),
    };
    // Both reads or neither -- one is the repository, the other the store, so their errors differ in kind and are
    // stringified alike here.
    let store = match (
        crate::worker_groups_api::worker_groups_snapshot(&state).await,
        smeltr::leader::worker_credentials_snapshot(&state).await,
    ) {
        (Ok(worker_groups), Ok(workers)) => Ok((worker_groups, workers)),
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e.to_string()),
    };
    // After the roster, whose read forgets the gone ephemeral workers first, so the threads agree with it.
    let health = Some(smeltr::leader::health_snapshot(&state));
    match (store, head_error) {
        (Ok((worker_groups, workers)), None) => ClusterStatus {
            error: None,
            health,
            deployed_commit,
            worker_groups: Some(worker_groups),
            workers: Some(workers),
        },
        (Ok((worker_groups, workers)), Some(e)) => ClusterStatus {
            error: Some(e),
            health,
            deployed_commit,
            worker_groups: Some(worker_groups),
            workers: Some(workers),
        },
        (Err(e), _) => ClusterStatus {
            error: Some(e),
            health,
            deployed_commit,
            worker_groups: None,
            workers: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use smeltr::leader::registry::ThreadHealth;
    use smeltr::leader::{GroupHealth, HealthResponse, WorkerOut};

    use super::*;
    use crate::worker_groups_api::WorkerGroupOut;

    fn thread(worker: &str, group: &str) -> ThreadHealth {
        ThreadHealth {
            worker_id: worker.to_string(),
            last_seen_millis_ago: 0,
            stop_after_unreachable_secs: None,
            commit: None,
            smeltr_version: String::new(),
            release: None,
            supervising: None,
            resources: None,
            authority_expires_millis: None,
            stopped_at_millis: None,
            report: smeltr::heartbeat::ThreadHeartbeat {
                thread_id: "reader-1".to_string(),
                group_id: group.to_string(),
                kind: None,
                partitions_owned: vec![],
                metrics: vec![],
            },
        }
    }

    fn group(id: &str) -> GroupHealth {
        GroupHealth {
            group_id: id.to_string(),
            transform_threads: 0,
            readers: 1,
            egress_threads: 0,
            partitions_owned: 0,
        }
    }

    fn worker(id: &str) -> WorkerOut {
        WorkerOut {
            worker_id: id.to_string(),
            revoked: false,
            kind: "permanent".to_string(),
            accepts: Vec::new(),
            asked: None,
        }
    }

    fn worker_group(id: &str) -> WorkerGroupOut {
        WorkerGroupOut {
            id: id.to_string(),
            name: id.to_string(),
            deployed: true,
            hue: None,
        }
    }

    /// Two groups' workers, and one that has never heartbeated.
    fn fleet() -> ClusterStatus {
        ClusterStatus {
            error: None,
            health: Some(HealthResponse {
                threads: vec![thread("w-pay", "payments"), thread("w-search", "search")],
                groups: vec![group("payments"), group("search")],
                silent_after_ms: 1000,
            }),
            deployed_commit: Some("abc".to_string()),
            worker_groups: Some(vec![worker_group("payments"), worker_group("search")]),
            workers: Some(vec![worker("w-pay"), worker("w-search"), worker("w-new")]),
        }
    }

    #[test]
    fn a_viewer_sees_only_the_workers_and_groups_they_hold() {
        let payments = Access::Areas(BTreeMap::from([("payments".to_string(), Level::Viewer)]));
        let seen = fleet().visible_to(&payments);
        let health = seen.health.unwrap();
        assert_eq!(
            health.threads.iter().map(|t| t.worker_id.as_str()).collect::<Vec<_>>(),
            ["w-pay"]
        );
        assert_eq!(
            health.groups.iter().map(|g| g.group_id.as_str()).collect::<Vec<_>>(),
            ["payments"]
        );
        assert_eq!(
            seen.workers
                .unwrap()
                .iter()
                .map(|w| w.worker_id.as_str())
                .collect::<Vec<_>>(),
            ["w-pay"]
        );
        assert_eq!(
            seen.worker_groups
                .unwrap()
                .iter()
                .map(|g| g.id.as_str())
                .collect::<Vec<_>>(),
            ["payments"]
        );
        assert_eq!(seen.deployed_commit.as_deref(), Some("abc"));
    }

    #[test]
    fn an_administrator_sees_the_whole_fleet() {
        let seen = fleet().visible_to(&Access::Administrator);
        assert_eq!(seen.workers.unwrap().len(), 3);
        assert_eq!(seen.health.unwrap().threads.len(), 2);
    }
}
