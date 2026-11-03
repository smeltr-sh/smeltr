//! The levels the fleet keeps its lines at, set by an operator: one for every area, and any area set apart from
//! it (`crate::logs::levels`). Stored as a leader setting, kept by the leader's own process, and sent to every
//! worker whole on each heartbeat answer.

use axum::Json;
use axum::extract::State;

use crate::heartbeat::LogLevels;
use crate::leader::envelope::{Envelope, Refusal};
use crate::leader::store::{Actor, Store, StoreError};
use crate::leader::{AppState, run_store};

const KEY: &str = "log_levels";

/// What is stored, or nothing set apart when nothing is. A stored value this build cannot read is nothing set
/// apart too, so a worker keeps its own level rather than one nobody can see.
pub fn stored(store: &dyn Store) -> Result<LogLevels, StoreError> {
    Ok(store
        .setting(KEY)?
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

/// The levels as stored, held so a heartbeat answer need not read the store for them.
#[derive(Default)]
pub struct Held(std::sync::RwLock<LogLevels>);

impl Held {
    pub fn get(&self) -> LogLevels {
        self.0.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Hold `levels` and keep the leader's own lines at them.
    fn keep(&self, levels: LogLevels) {
        levels.apply();
        *self.0.write().unwrap_or_else(std::sync::PoisonError::into_inner) = levels;
    }
}

/// Hold what is stored, and keep the leader's own lines at it. Called as the leader starts.
pub async fn keep_stored(state: &AppState) {
    match run_store(state.store.clone(), |s| stored(s)).await {
        Ok(levels) => state.log_levels.keep(levels),
        Err(e) => {
            tracing::warn!(target: "leader.log_levels_unreadable", error = %e, "the saved log levels could not be read, so the leader keeps the levels it has")
        }
    }
}

/// The levels, and what they may name.
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLevelsAnswer {
    levels: Option<LogLevels>,
    areas: &'static [&'static str],
    labels: &'static [&'static str],
}

fn answer(levels: Result<LogLevels, Refusal>) -> Envelope<LogLevelsAnswer> {
    match levels {
        Ok(levels) => Envelope::ok(LogLevelsAnswer {
            levels: Some(levels),
            areas: crate::logs::levels::AREAS,
            labels: crate::logs::LEVEL_LABELS,
        }),
        Err(refusal) => Envelope::refused(refusal),
    }
}

/// The levels, as a person would list them: "error, warn, info, debug or trace".
fn labels() -> String {
    let (last, rest) = crate::logs::LEVEL_LABELS.split_last().expect("there are levels");
    format!("{} or {last}", rest.join(", "))
}

/// Why `levels` cannot be kept, if it cannot. An area is any one a line could name, a library's crate included, so
/// it is held to the shape of one rather than to [`crate::logs::levels::AREAS`].
fn refused(levels: &LogLevels) -> Option<String> {
    let label = |label: &str| crate::logs::level_for_label(label).is_none();
    if let Some(default) = levels.default.as_deref().filter(|d| label(d)) {
        return Some(format!("{default:?} is not a level: use {}", labels()));
    }
    for (area, level) in &levels.areas {
        if area.is_empty()
            || !area
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            return Some(format!(
                "{area:?} is not an area: use lower-case letters, digits and underscores"
            ));
        }
        if label(level) {
            return Some(format!("{level:?} is not a level for {area}: use {}", labels()));
        }
    }
    None
}

/// `GET /api/settings/log-levels`.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<LogLevelsAnswer> {
    answer(run_store(state.store, |s| stored(s)).await.map_err(Refusal::from))
}

/// `PUT /api/settings/log-levels`: whole and replacing. Recorded with who set it, in the transaction that sets it,
/// and kept by the leader at once; a worker keeps it from its next heartbeat.
///
/// `actor` is optional for the reason [`crate::leader::enrollment_codes::mint`]'s is: a router composed without a gate.
pub async fn put_settings(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(levels): Json<LogLevels>,
) -> Envelope<LogLevelsAnswer> {
    if let Some(why) = refused(&levels) {
        return answer(Err(Refusal::invalid(why)));
    }
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let text = serde_json::to_string(&levels).expect("levels are plain strings");
    let event = super::accounts::event_for(&actor, "log-levels-change", &text, "ok");
    let written = run_store(state.store, move |s| s.put_settings(&[(KEY, text)], Some(&event))).await;
    if written.is_ok() {
        state.log_levels.keep(levels.clone());
    }
    answer(written.map(|()| levels).map_err(Refusal::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn levels(default: Option<&str>, areas: &[(&str, &str)]) -> LogLevels {
        LogLevels {
            default: default.map(str::to_string),
            areas: areas.iter().map(|(a, l)| (a.to_string(), l.to_string())).collect(),
        }
    }

    #[test]
    fn levels_are_refused_for_what_is_not_a_level_or_an_area() {
        assert_eq!(
            refused(&levels(Some("info"), &[("kafka", "trace"), ("hyper_util", "debug")])),
            None
        );
        assert_eq!(
            refused(&levels(Some("loud"), &[])).unwrap(),
            r#""loud" is not a level: use error, warn, info, debug or trace"#
        );
        assert!(
            refused(&levels(None, &[("kafka", "3")]))
                .unwrap()
                .contains("not a level")
        );
        assert!(
            refused(&levels(None, &[("Kafka", "trace")]))
                .unwrap()
                .contains("not an area")
        );
        assert!(
            refused(&levels(None, &[("", "trace")]))
                .unwrap()
                .contains("not an area")
        );
    }

    #[test]
    fn levels_name_what_a_process_keeps() {
        use tracing::Level;
        let kept = levels(None, &[("kafka", "trace"), ("store", "nonsense")]).levels();
        assert_eq!(kept.default, Level::INFO);
        assert_eq!(kept.kept_at("kafka.lag_unmeasured"), Level::TRACE);
        assert_eq!(
            kept.kept_at("store.statement"),
            Level::INFO,
            "a label this build does not know is the default"
        );
        assert_eq!(levels(Some("debug"), &[]).levels().default, Level::DEBUG);
    }

    #[test]
    fn levels_travel_whole_and_absent_is_nothing_set_apart() {
        let text = serde_json::to_string(&levels(Some("info"), &[("kafka", "trace")])).unwrap();
        assert_eq!(text, r#"{"default":"info","areas":{"kafka":"trace"}}"#);
        let none: crate::heartbeat::HeartbeatResponse = serde_json::from_str(r#"{"captures":[],"holds":[]}"#).unwrap();
        assert!(none.log_levels.is_empty());
    }

    #[tokio::test]
    async fn levels_set_are_held_for_the_heartbeat_and_held_again_on_a_restart() {
        let state = crate::leader::secrets_delivery_tests::state("log-levels-held");
        let set = levels(None, &[("kafka", "trace")]);
        let put = put_settings(State(state.clone()), None, Json(set.clone())).await;
        assert!(put.succeeded());
        assert_eq!(state.log_levels.get(), set);
        let restarted = AppState {
            log_levels: Default::default(),
            ..state
        };
        keep_stored(&restarted).await;
        assert_eq!(restarted.log_levels.get(), set);
    }
}
