//! The look for a newer smeltr: at startup and once a day after, the leader reads the releases published on GitHub,
//! through smeltr.sh, and keeps the newest that is newer than its own. The request names this leader's release in its
//! `User-Agent` and nothing else; smeltr.sh counts leaders by release from it (`website/worker`). A look that fails
//! says why on Admin -> System -> General and raises nothing, so a leader that cannot reach smeltr.sh is never noisy.
//! A build that is no release has nothing to compare and never looks.

use std::time::Duration;

use axum::Json;
use axum::extract::State;

use super::envelope::{Envelope, Refusal};
use super::store::Actor;
use super::{AppState, run_store};

/// Where releases are published: GitHub's list as smeltr.sh serves it, not its "latest". That marker follows the order
/// releases were published in, so a patch to an older line published after a newer major would take it.
const RELEASES: &str = "https://smeltr.sh/api/releases";

/// A release's page on smeltr.sh. `just cut-release` refuses a release without one.
fn page(release: &str) -> String {
    format!("https://smeltr.sh/releases/{release}")
}

/// How often the leader looks.
const EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// How long one look may take.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The most a list of releases may hold, notes and all.
const MAX_LIST: usize = 8 * 1024 * 1024;

/// Whether the leader looks; absent is on.
pub const CHECK_KEY: &str = "updates.check";

/// A published release newer than this leader's.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Newer {
    pub release: String,
    /// Its page on smeltr.sh, which says what it changed.
    pub url: String,
    /// When this leader first saw it, unix millis.
    #[serde(skip)]
    pub since: i64,
}

/// What the last look found.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Looked {
    /// Unix millis.
    pub checked_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// The leader's looks: what the last found, and what wakes the timer.
#[derive(Default)]
pub struct Updates {
    state: std::sync::Mutex<(Option<Newer>, Option<Looked>)>,
    /// Wakes the timer when the setting changes, so turning it on looks at once.
    pub settings_changed: tokio::sync::Notify,
}

impl Updates {
    fn held(&self) -> std::sync::MutexGuard<'_, (Option<Newer>, Option<Looked>)> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The newer release, if the last look that reached GitHub found one.
    pub fn newer(&self) -> Option<Newer> {
        self.held().0.clone()
    }

    /// When the last look was, and why it failed if it did.
    pub fn looked(&self) -> Option<Looked> {
        self.held().1.clone()
    }

    /// A look that read the list. A newer release seen before keeps the time it was first seen.
    fn found(&self, newest: Option<(String, String)>, now: i64) {
        let mut held = self.held();
        let since = |release: &str| {
            held.0
                .as_ref()
                .filter(|n| n.release == release)
                .map_or(now, |n| n.since)
        };
        held.0 = newest.map(|(release, url)| Newer {
            since: since(&release),
            release,
            url,
        });
        held.1 = Some(Looked {
            checked_at: now,
            failure: None,
        });
    }

    /// A look that did not read the list. What an earlier look found stands: the release is out whether or not
    /// GitHub answered this time.
    fn failed(&self, why: String, now: i64) {
        self.held().1 = Some(Looked {
            checked_at: now,
            failure: Some(why),
        });
    }

    /// Turned off: nothing found stays, since nothing keeps it current.
    fn forget(&self) {
        *self.held() = (None, None);
    }
}

/// Whether this leader looks.
fn checking(store: &dyn super::store::Store) -> Result<bool, super::store::StoreError> {
    Ok(store.setting(CHECK_KEY)?.as_deref() != Some("false"))
}

/// Look on the timer for as long as the leader runs. Turned off, it waits for the setting to change.
pub async fn poll(state: AppState) {
    let Some(own) = crate::version::RELEASE else {
        return;
    };
    loop {
        if !run_store(state.store.clone(), checking).await.unwrap_or(true) {
            state.updates.forget();
            state.updates.settings_changed.notified().await;
            continue;
        }
        let now = super::warnings::now_millis();
        match fetch(own).await {
            Ok(newest) => state.updates.found(newest, now),
            Err(why) => state.updates.failed(why, now),
        }
        tokio::select! {
            _ = tokio::time::sleep(EVERY) => {}
            _ = state.updates.settings_changed.notified() => {}
        }
    }
}

/// The newest published release newer than `own`, with its page.
async fn fetch(own: &str) -> Result<Option<(String, String)>, String> {
    let client = super::outbound::client(RELEASES, "smeltr.sh", TIMEOUT, reqwest::redirect::Policy::limited(3))?;
    let response = client
        .get(RELEASES)
        .header(reqwest::header::USER_AGENT, format!("smeltr/{own}"))
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| format!("could not reach smeltr.sh: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("smeltr.sh answered {}", response.status()));
    }
    let body = super::outbound::read(response, MAX_LIST).await?;
    newest(&body, own)
}

/// One entry of GitHub's list, as far as this reads it.
#[derive(serde::Deserialize)]
struct Published {
    tag_name: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    prerelease: bool,
}

/// The highest `v<major>.<minor>.<patch>` in `list` that is no draft or pre-release, when it is newer than `own`.
fn newest(list: &[u8], own: &str) -> Result<Option<(String, String)>, String> {
    let list: Vec<Published> =
        serde_json::from_slice(list).map_err(|e| format!("could not read the list of releases: {e}"))?;
    let own = parse(own).ok_or_else(|| format!("{own} is not a release"))?;
    Ok(list
        .into_iter()
        .filter(|p| !p.draft && !p.prerelease)
        .filter_map(|p| {
            let name = p.tag_name.strip_prefix('v')?.to_string();
            let (core, pre) = parse(&name)?;
            pre.is_none().then(|| (core, page(&name), name))
        })
        .max_by_key(|(core, ..)| *core)
        .filter(|(core, ..)| newer(*core, own))
        .map(|(_, url, name)| (name, url)))
}

type Core = (u64, u64, u64);

/// `1.2.0` as its numbers, and `1.2.0-rc1`'s pre-release part beside them.
fn parse(release: &str) -> Option<(Core, Option<&str>)> {
    let (core, pre) = match release.split_once('-') {
        Some((core, pre)) => (core, Some(pre)),
        None => (release, None),
    };
    let mut parts = core.split('.').map(|p| p.parse::<u64>().ok());
    let core = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some((core, pre))
}

/// Whether a release is newer than `own`. A release is newer than its own pre-releases: `1.0.0` than `1.0.0-rc3`.
fn newer(release: Core, (own, pre): (Core, Option<&str>)) -> bool {
    release > own || (release == own && pre.is_some())
}

/// What Admin -> System -> General reads and saves.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct UpdateSettingsDto {
    check: bool,
}

#[derive(Default, serde::Serialize)]
pub struct UpdateSettings {
    settings: Option<UpdateSettingsDto>,
}

impl UpdateSettings {
    fn answer(check: Result<bool, Refusal>) -> Envelope<UpdateSettings> {
        match check {
            Ok(check) => Envelope::ok(UpdateSettings {
                settings: Some(UpdateSettingsDto { check }),
            }),
            Err(refusal) => Envelope::refused(refusal),
        }
    }
}

/// `GET /api/settings/updates`.
pub async fn get_settings(State(state): State<AppState>) -> Envelope<UpdateSettings> {
    UpdateSettings::answer(run_store(state.store, checking).await.map_err(Refusal::from))
}

/// `PUT /api/settings/updates`. Recorded with who set it, in the transaction that sets it; the timer looks at once
/// when it is turned on.
pub async fn put_settings(
    State(state): State<AppState>,
    actor: Option<axum::Extension<Actor>>,
    Json(request): Json<UpdateSettingsDto>,
) -> Envelope<UpdateSettings> {
    let actor = actor.map(|a| a.0).unwrap_or_else(Actor::system);
    let check = request.check;
    let event = super::accounts::event_for(&actor, "update-settings-change", "", "ok");
    let written = run_store(state.store.clone(), move |s| {
        s.put_settings(&[(CHECK_KEY, check.to_string())], Some(&event))
    })
    .await;
    state.updates.settings_changed.notify_one();
    UpdateSettings::answer(written.map(|()| check).map_err(Refusal::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(entries: &[(&str, bool, bool)]) -> Vec<u8> {
        let entries: Vec<_> = entries
            .iter()
            .map(|(tag, draft, prerelease)| {
                serde_json::json!({
                    "tag_name": tag,
                    "html_url": format!("https://github.com/smeltr-sh/smeltr/releases/tag/{tag}"),
                    "draft": draft,
                    "prerelease": prerelease,
                    "body": "notes",
                })
            })
            .collect();
        serde_json::to_vec(&entries).unwrap()
    }

    #[test]
    fn the_newest_is_the_highest_version_not_the_last_published() {
        let published = list(&[
            ("v1.2.4", false, false),
            ("v2.0.0", false, false),
            ("v1.3.0", false, false),
        ]);
        assert_eq!(
            newest(&published, "1.2.0").unwrap(),
            Some(("2.0.0".into(), "https://smeltr.sh/releases/2.0.0".into()))
        );
        assert_eq!(newest(&published, "2.0.0").unwrap(), None, "nothing newer than its own");
        assert_eq!(
            newest(&published, "2.1.0").unwrap(),
            None,
            "nor than a release GitHub no longer lists"
        );
    }

    #[test]
    fn drafts_pre_releases_and_other_tags_are_not_releases() {
        let published = list(&[
            ("v1.3.0", true, false),
            ("v1.4.0-rc1", false, true),
            ("v1.5.0-rc1", false, false),
            ("nightly", false, false),
            ("v1.6", false, false),
            ("v1.2.1", false, false),
        ]);
        assert_eq!(
            newest(&published, "1.2.0").unwrap().map(|n| n.0).as_deref(),
            Some("1.2.1")
        );
    }

    #[test]
    fn a_release_is_newer_than_its_own_pre_releases() {
        let published = list(&[("v1.0.0", false, false)]);
        assert_eq!(
            newest(&published, "1.0.0-rc3").unwrap().map(|n| n.0).as_deref(),
            Some("1.0.0")
        );
        assert_eq!(newest(&published, "1.0.1-rc1").unwrap(), None);
    }

    #[test]
    fn an_answer_that_is_not_a_list_is_a_failure() {
        assert!(newest(br#"{"message":"API rate limit exceeded"}"#, "1.2.0").is_err());
    }

    #[test]
    fn a_newer_release_keeps_when_it_was_first_seen_and_a_failure_keeps_it() {
        let updates = Updates::default();
        let v = |r: &str| Some((r.to_string(), format!("u/{r}")));
        updates.found(v("1.3.0"), 10);
        updates.found(v("1.3.0"), 20);
        assert_eq!(updates.newer().unwrap().since, 10);
        updates.failed("down".into(), 30);
        assert_eq!(updates.newer().unwrap().release, "1.3.0", "the release is still out");
        assert_eq!(
            updates.looked().unwrap(),
            Looked {
                checked_at: 30,
                failure: Some("down".into())
            }
        );
        updates.found(v("1.4.0"), 40);
        assert_eq!(updates.newer().unwrap().since, 40, "another release is news again");
        updates.found(None, 50);
        assert_eq!(updates.newer(), None, "a release GitHub no longer lists");
    }
}
