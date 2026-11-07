//! The leader's own periodic work, due whether or not a worker heartbeats or a person reads anything.

use super::{
    AppState, PRUNE_PROGRESS_EVERY, capture, catalog, cut_over_if_due, forget, pack_repo, prune_source_progress,
};

/// Runs [`tick`] once a heartbeat interval, for as long as the leader runs.
pub async fn run(state: AppState) {
    loop {
        tick(&state, super::warnings::now_millis()).await;
        tokio::time::sleep(crate::startup_config::leader_heartbeat_interval()).await;
    }
}

/// Everything periodic that is due at `now_ms`. Each part keeps its own schedule, so calling this more often costs
/// only the checks.
pub async fn tick(state: &AppState, now_ms: i64) {
    forget::sweep(state, now_ms).await;
    if state.registry.prune_due(now_ms, PRUNE_PROGRESS_EVERY) {
        let state = state.clone();
        tokio::spawn(async move {
            prune_source_progress(&state).await;
            pack_repo(&state).await;
        });
    }
    cut_over_if_due(state, now_ms).await;

    // Every issue begins and ends in the log as it happens, read or not. A catalog that fails here also fails the
    // Issues page, and the page says so.
    if state
        .registry
        .issue_look_due(now_ms, crate::startup_config::leader_heartbeat_interval())
    {
        static SAID: std::sync::Mutex<crate::logs::Repeat> = std::sync::Mutex::new(crate::logs::Repeat::new());
        let looked = catalog(state).await;
        let mut said = SAID.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match looked {
            Ok(_) => said.clear(),
            Err(e) if said.due() => {
                tracing::warn!(name: "issues_unreadable", target: "status", error = %e, "the issues could not be read, so none are logged as beginning or ending")
            }
            Err(_) => {}
        }
    }

    // A finished session that nobody collected would otherwise hold its records until the process restarted.
    state.captures.sweep(capture::HELD_IDLE, capture::HELD_AT_MOST);
}
