//! What an editor said about a source's rows read against its normal (`throughput`): lower the warning that holds to
//! info, reset the source's history, so its normal is learned again from now, or say a past period of it was normal
//! or an outage.
//!
//! A lowered warning stays on the Issues page for everyone, marked as info, and the views that judge health leave
//! it out. It is named by when it began, so it stays lowered until it ends, and the next one is a warning again.
//! Nothing is learned from the time it held.
//!
//! A source whose history was reset learns its normal only from what it read after the reset, so it has none, and
//! does not warn, until it has [`super::throughput::MIN_DAYS`] days of that.
//!
//! A period said to be normal is learned from though it warned, and one said to be an outage is not learned from though
//! it did not warn. The count widths and the gap learn from the same periods.
//!
//! All are kept in one setting, with who said each.

use axum::Json;
use axum::extract::{Path, State};
use serde::{Deserialize, Serialize};

use super::AppState;
use super::envelope::{Empty, Envelope, Refusal};
use super::series::YEAR_MS;
use super::store::{Actor, Store};
use super::throughput::{self, Learning};
use super::warnings::{self, Condition, SubjectKind};

/// Where the marks are kept, as JSON.
pub const MARKS_KEY: &str = "warnings.source_rows_marks";

/// Every mark that still says something.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marks {
    #[serde(default)]
    pub lowered: Vec<Lowered>,
    #[serde(default)]
    pub reset: Vec<Reset>,
    #[serde(default)]
    pub periods: Vec<Period>,
}

/// A source's rows warning that began at `since_millis`, lowered to info by `by`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lowered {
    pub group: String,
    pub source: String,
    pub since_millis: i64,
    pub by: String,
}

/// A source whose normal `by` said to learn again from `from_millis` on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reset {
    pub group: String,
    pub source: String,
    pub from_millis: i64,
    pub by: String,
}

/// A past period of a source an editor said was normal or an outage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Period {
    pub group: String,
    pub source: String,
    pub from_millis: i64,
    pub to_millis: i64,
    pub was: Was,
    pub by: String,
}

/// What an editor said a past period of a source was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Was {
    Normal,
    Outage,
}

impl Marks {
    /// The marks as stored. None when they cannot be read, which is logged: every warning is then a warning, and
    /// every normal learned from all it can be.
    pub fn read(store: &Store) -> Marks {
        let text = match store.setting(MARKS_KEY) {
            Ok(text) => text,
            Err(e) => {
                tracing::warn!(name: "marks_unreadable", target: "status", error = %e, "could not read which rows warnings were lowered or had their history reset, so none is");
                return Marks::default();
            }
        };
        text.and_then(|text| {
            serde_json::from_str(&text)
                .inspect_err(|e| {
                    tracing::warn!(name: "marks_unreadable", target: "status", error = %e, "the stored marks on rows warnings are not readable, so none is applied");
                })
                .ok()
        })
        .unwrap_or_default()
    }

    /// Who lowered `group`'s `source`'s rows warning that began at `since_millis`, if anyone did.
    pub fn lowered_by(&self, group: &str, source: &str, since_millis: Option<i64>) -> Option<&str> {
        self.lowered
            .iter()
            .find(|l| l.group == group && l.source == source && Some(l.since_millis) == since_millis)
            .map(|l| l.by.as_str())
    }
}

/// When `group`'s `source`'s rows warning that holds began, if one holds.
fn holding_since(state: &AppState, group: &str, source: &str, now_ms: i64) -> Result<Option<i64>, Refusal> {
    let Some(store) = state.registry.series() else {
        return Ok(None);
    };
    let spans = warnings::spans(store, SubjectKind::Source, group, source, now_ms)?;
    Ok(spans
        .into_iter()
        .find(|s| s.ended_millis.is_none() && s.condition == Condition::SourceRowsBelow)
        .map(|s| s.started_millis))
}

/// Held while the marks are read, changed and written, so two marks made at once are both kept.
static CHANGING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// One source an editor marks, by the route that marks it.
struct Marking {
    group: String,
    source: String,
    /// The audit's name for what was done.
    action: &'static str,
}

impl Marking {
    /// Makes `change` to the stored marks, given when the source's rows warning that holds began, and writes them
    /// with who made it, leaving out every lowered warning that no longer holds. Off the async runtime.
    async fn write(
        self,
        state: AppState,
        actor: Option<axum::Extension<Actor>>,
        change: impl FnOnce(&mut Marks, &Marking, Option<i64>, &Actor, i64) -> Result<(), Refusal> + Send + 'static,
    ) -> Envelope {
        let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
        let written = super::blocking(move || -> Result<(), Refusal> {
            let _changing = CHANGING.lock().unwrap_or_else(|e| e.into_inner());
            let now_ms = warnings::now_millis();
            let mut marks = Marks::read(&state.store);
            let since = holding_since(&state, &self.group, &self.source, now_ms)?;
            change(&mut marks, &self, since, &actor, now_ms)?;
            let mut kept = Vec::new();
            for l in marks.lowered {
                if holding_since(&state, &l.group, &l.source, now_ms)? == Some(l.since_millis) {
                    kept.push(l);
                }
            }
            marks.lowered = kept;
            let target = format!("{}/{}", self.group, self.source);
            let event = super::accounts::event_for(&actor, self.action, &target, "ok");
            let text = serde_json::to_string(&marks).expect("marks always serialize");
            Ok(state.store.put_settings(&[(MARKS_KEY, text)], Some(&event))?)
        })
        .await;
        match written {
            Ok(()) => Envelope::done(),
            Err(refusal) => Envelope::refused(refusal),
        }
    }
}

/// `POST /api/areas/:area/sources/:id/lower`: lowers the rows warning that holds for the source to info until it
/// ends. Refused when none holds.
pub async fn lower(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        action: "source-rows-lower",
    };
    marking
        .write(state, actor, |marks, m, since, actor, _| {
            let Some(since) = since else {
                return Err(Refusal::not_found(format!(
                    "source {} in {} has no warning to lower",
                    m.source, m.group
                )));
            };
            if marks.lowered_by(&m.group, &m.source, Some(since)).is_none() {
                marks.lowered.push(Lowered {
                    group: m.group.clone(),
                    source: m.source.clone(),
                    since_millis: since,
                    by: actor.display_name.clone(),
                });
            }
            Ok(())
        })
        .await
}

/// `POST /api/areas/:area/sources/:id/reset-history`: resets the source's history, so its normal is learned again
/// from now.
pub async fn reset_history(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        action: "source-rows-reset-history",
    };
    marking
        .write(state, actor, |marks, m, _, actor, now_ms| {
            // Only the last reset says anything: it forgets more than every one before it.
            marks.reset.retain(|r| (&r.group, &r.source) != (&m.group, &m.source));
            marks.reset.push(Reset {
                group: m.group.clone(),
                source: m.source.clone(),
                from_millis: now_ms,
                by: actor.display_name.clone(),
            });
            Ok(())
        })
        .await
}

/// A past period of a source, and what to mark it.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Marked {
    pub from_millis: i64,
    pub to_millis: i64,
    pub was: Was,
}

/// `POST /api/areas/:area/sources/:id/mark`: says a past period of the source was normal or an outage, in place of
/// what was said about that same period. Refused for a period that is empty or not over.
pub async fn mark(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Path((group, source)): Path<(String, String)>,
    Json(marked): Json<Marked>,
) -> Envelope<Empty> {
    let marking = Marking {
        group,
        source,
        action: "source-rows-mark",
    };
    marking
        .write(state, actor, move |marks, m, _, actor, now_ms| {
            if marked.from_millis >= marked.to_millis || marked.to_millis > now_ms {
                return Err(Refusal::invalid(format!(
                    "source {} in {}: a period to mark must end after it begins, and must be over",
                    m.source, m.group
                )));
            }
            // A period past a year is past what any normal learns from.
            marks
                .periods
                .retain(|p| p.to_millis >= now_ms - YEAR_MS && !p.is(&m.group, &m.source, &marked));
            marks.periods.push(Period {
                group: m.group.clone(),
                source: m.source.clone(),
                from_millis: marked.from_millis,
                to_millis: marked.to_millis,
                was: marked.was,
                by: actor.display_name.clone(),
            });
            Ok(())
        })
        .await
}

impl Period {
    /// Whether this is `group`'s `source`'s period `marked` names.
    fn is(&self, group: &str, source: &str, marked: &Marked) -> bool {
        (
            self.group.as_str(),
            self.source.as_str(),
            self.from_millis,
            self.to_millis,
        ) == (group, source, marked.from_millis, marked.to_millis)
    }
}

/// `GET /api/areas/:area/sources/:id/past`: the source's warnings and quiet spells of the past year, for an editor to
/// mark ([`throughput::past`]).
pub async fn past(
    State(state): State<AppState>,
    Path((group, source)): Path<(String, String)>,
) -> Envelope<throughput::Past> {
    super::blocking_answer(move || {
        let Some(store) = state.registry.series() else {
            return Envelope::ok(throughput::Past::default());
        };
        let learning = Learning::read(&state.store);
        throughput::past(store, &group, &source, warnings::now_millis(), &learning).into()
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lowered_warning_is_named_by_when_it_began() {
        let marks = Marks {
            lowered: vec![Lowered {
                group: "g".into(),
                source: "s".into(),
                since_millis: 5,
                by: "John".into(),
            }],
            ..Marks::default()
        };
        assert_eq!(marks.lowered_by("g", "s", Some(5)), Some("John"));
        assert_eq!(
            marks.lowered_by("g", "s", Some(6)),
            None,
            "the next one is a warning again"
        );
        assert_eq!(marks.lowered_by("g", "other", Some(5)), None);
        assert_eq!(marks.lowered_by("g", "s", None), None);
    }

    #[test]
    fn marks_read_back_as_written_and_unreadable_ones_as_none() {
        let dir = std::env::temp_dir().join(format!("smeltr-marks-{}", std::process::id()));
        let store = crate::leader::store::Store::open(std::path::Path::new(":memory:"), &dir.join("k")).unwrap();
        assert_eq!(Marks::read(&store), Marks::default());
        let marks = Marks {
            reset: vec![Reset {
                group: "g".into(),
                source: "s".into(),
                from_millis: 7,
                by: "John".into(),
            }],
            periods: vec![Period {
                group: "g".into(),
                source: "s".into(),
                from_millis: 3,
                to_millis: 5,
                was: Was::Outage,
                by: "John".into(),
            }],
            ..Marks::default()
        };
        store
            .put_settings(&[(MARKS_KEY, serde_json::to_string(&marks).unwrap())], None)
            .unwrap();
        assert_eq!(Marks::read(&store), marks);
        store.put_settings(&[(MARKS_KEY, "not json".into())], None).unwrap();
        assert_eq!(Marks::read(&store), Marks::default());
    }
}
