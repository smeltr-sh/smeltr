//! The levels the fleet keeps its lines at, set by an operator: one for every line, and an override for a class, a
//! class and a type, or a service kept at its own (`crate::logs::levels`). Stored as a leader setting, kept by the
//! leader's own process, and sent to every worker whole on each heartbeat answer.

use axum::Json;
use axum::extract::State;

use crate::heartbeat::{LevelOverride, LogLevels, Named};
use crate::leader::envelope::{Envelope, Refusal};
use crate::leader::store::{Actor, Store, StoreError};
use crate::leader::{AppState, run_store};

const KEY: &str = "log_levels";

/// What is stored, or nothing when nothing is: every line then at each process's startup level. A stored value this
/// build cannot read is nothing too.
pub fn stored(store: &Store) -> Result<LogLevels, StoreError> {
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
    match run_store(state.store.clone(), stored).await {
        Ok(levels) => state.log_levels.keep(levels),
        Err(e) => {
            tracing::warn!(name: "levels_unreadable", target: "logs", error = %e, "the saved log levels could not be read, so the leader keeps the levels it has")
        }
    }
}

/// The levels, and what they may name.
#[derive(Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLevelsAnswer {
    levels: Option<LogLevels>,
    classes: &'static [&'static str],
    services: Vec<&'static str>,
    labels: &'static [&'static str],
}

fn answer(levels: Result<LogLevels, Refusal>) -> Envelope<LogLevelsAnswer> {
    match levels {
        Ok(levels) => Envelope::ok(LogLevelsAnswer {
            levels: Some(levels),
            classes: crate::logs::levels::CLASSES,
            services: crate::logs::levels::services(),
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

/// What `named` names, as a person would say it, or why it names nothing a line can have.
fn named(named: &LevelOverride) -> Result<String, String> {
    let class = |class: &str| {
        if crate::logs::levels::CLASSES.contains(&class) {
            Ok(())
        } else {
            let service = crate::logs::levels::services().contains(&class);
            Err(format!(
                "{class:?} is not a class: use one of the classes GET /api/settings/log-levels lists{}",
                if service {
                    format!(" ({class} is a service)")
                } else {
                    String::new()
                }
            ))
        }
    };
    match named.names() {
        Some(Named::Class(c)) => class(c).map(|()| format!("class {c}")),
        Some(Named::Type { class: c, kind }) => {
            class(c)?;
            if crate::logs::levels::is_type(kind) {
                Ok(format!("class {c}, type {kind}"))
            } else {
                Err(format!(
                    "{kind:?} is not a type: use lower-case letters, digits and underscores"
                ))
            }
        }
        Some(Named::Service(service)) if crate::logs::levels::services().contains(&service) => {
            Ok(format!("service {service}"))
        }
        Some(Named::Service(service)) => Err(format!(
            "{service:?} is not a service: use one of the services GET /api/settings/log-levels lists"
        )),
        None => Err("An override must name a class, a class and a type, or a service.".to_string()),
    }
}

/// Why `levels` cannot be kept, if it cannot.
fn refused(levels: &LogLevels) -> Option<String> {
    let label = |label: &str| crate::logs::level_for_label(label).is_none();
    if let Some(default) = levels.default.as_deref().filter(|d| label(d)) {
        return Some(format!("{default:?} is not a level: use {}", labels()));
    }
    let mut seen = std::collections::BTreeSet::new();
    for each in &levels.overrides {
        let what = match named(each) {
            Ok(what) => what,
            Err(why) => return Some(why),
        };
        if label(&each.level) {
            return Some(format!("{:?} is not a level for {what}: use {}", each.level, labels()));
        }
        if !seen.insert(what.clone()) {
            return Some(format!("{what} is named by two overrides: keep one"));
        }
    }
    None
}

/// `GET /api/settings/log-levels`.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<LogLevelsAnswer> {
    answer(run_store(state.store, stored).await.map_err(Refusal::from))
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

    /// An override for each of `named`: `class`, `class type`, or `@service`, at its level.
    fn levels(default: Option<&str>, named: &[(&str, &str)]) -> LogLevels {
        let overrides = named
            .iter()
            .map(|(what, level)| {
                let mut o = LevelOverride {
                    level: level.to_string(),
                    ..Default::default()
                };
                match (what.strip_prefix('@'), what.split_once(' ')) {
                    (Some(service), _) => o.service = Some(service.to_string()),
                    (None, Some((class, kind))) => {
                        o.class = Some(class.to_string());
                        o.r#type = Some(kind.to_string());
                    }
                    (None, None) => o.class = Some(what.to_string()),
                }
                o
            })
            .collect();
        LogLevels {
            default: default.map(str::to_string),
            overrides,
        }
    }

    #[test]
    fn levels_are_refused_for_what_is_not_a_level_or_a_line() {
        assert_eq!(
            refused(&levels(
                Some("info"),
                &[
                    ("sources", "trace"),
                    ("store statement_ran", "debug"),
                    ("@kafka", "debug")
                ]
            )),
            None
        );
        assert_eq!(
            refused(&levels(Some("loud"), &[])).unwrap(),
            r#""loud" is not a level: use error, warn, info, debug or trace"#
        );
        assert_eq!(
            refused(&levels(None, &[("sources", "3")])).unwrap(),
            r#""3" is not a level for class sources: use error, warn, info, debug or trace"#
        );
        assert_eq!(
            refused(&levels(None, &[("kafka", "trace")])).unwrap(),
            r#""kafka" is not a class: use one of the classes GET /api/settings/log-levels lists (kafka is a service)"#
        );
        assert_eq!(
            refused(&levels(None, &[("store_x", "trace")])).unwrap(),
            r#""store_x" is not a class: use one of the classes GET /api/settings/log-levels lists"#
        );
        assert_eq!(
            refused(&levels(None, &[("@mongo", "trace")])).unwrap(),
            r#""mongo" is not a service: use one of the services GET /api/settings/log-levels lists"#
        );
        assert!(
            refused(&levels(None, &[("sources Assigned", "trace")]))
                .unwrap()
                .contains("not a type")
        );
        assert_eq!(
            refused(&levels(None, &[("sources", "trace"), ("sources", "debug")])).unwrap(),
            "class sources is named by two overrides: keep one"
        );
        let both = LogLevels {
            default: None,
            overrides: vec![LevelOverride {
                class: Some("sources".into()),
                service: Some("kafka".into()),
                level: "trace".into(),
                ..Default::default()
            }],
        };
        assert_eq!(
            refused(&both).unwrap(),
            "An override must name a class, a class and a type, or a service."
        );
    }

    #[test]
    fn levels_name_what_a_process_keeps() {
        use tracing::Level;
        let kept = levels(
            None,
            &[
                ("sources", "trace"),
                ("@kafka", "debug"),
                ("store statement_ran", "nonsense"),
            ],
        )
        .levels(Level::WARN);
        assert_eq!(kept.default, Level::WARN);
        assert_eq!(kept.kept_at(Some("sources"), "poll_failed", None), Level::TRACE);
        assert_eq!(kept.kept_at(Some("sources"), "assigned", Some("kafka")), Level::DEBUG);
        assert_eq!(
            kept.kept_at(Some("store"), "statement_ran", None),
            Level::WARN,
            "a label this build does not know is the default"
        );
        assert_eq!(levels(Some("debug"), &[]).levels(Level::WARN).default, Level::DEBUG);
    }

    #[test]
    fn levels_travel_whole_and_absent_is_the_startup_level() {
        let text = serde_json::to_string(&levels(Some("info"), &[("sources", "trace"), ("@kafka", "debug")])).unwrap();
        assert_eq!(
            text,
            r#"{"default":"info","overrides":[{"class":"sources","level":"trace"},{"service":"kafka","level":"debug"}]}"#
        );
        let none: crate::heartbeat::HeartbeatResponse = serde_json::from_str(r#"{"captures":[],"holds":[]}"#).unwrap();
        assert!(none.log_levels.is_empty());
    }

    #[tokio::test]
    async fn levels_set_are_held_for_the_heartbeat_and_held_again_on_a_restart() {
        let state = crate::leader::secrets_delivery_tests::state("log-levels-held");
        let set = levels(None, &[("sources", "trace")]);
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
