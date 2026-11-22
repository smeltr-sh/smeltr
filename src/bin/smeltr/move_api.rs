//! Moving objects from one area to another, the way an export and an import move them between leaders: the mover
//! sees each object the move would take or break and ticks what goes, and the move is one commit. What goes replaces
//! whatever the other area holds under its id. Asked under `preview/moves`, it writes nothing and answers what the ticks
//! would do.

use std::collections::BTreeSet;

use axum::Json;
use axum::extract::Extension;
use axum::http::StatusCode;

use smeltr::config_file::{self, MoveRow, Moving, Objects, Part, PartId, describe};
use smeltr::leader::envelope::{ApiError, Envelope, Refusal};
use smeltr::leader::store::Actor;

use crate::config_api::{Addressed, NotWritten, Writing};
use crate::import_api::{self, scope_objects};

/// What the mover asks: the object, where it goes, and once the mover has chosen, what goes with it.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MoveAsk {
    pub(crate) part: Part,
    pub(crate) id: String,
    /// `shared`, or a group id.
    pub(crate) to: String,
    /// Further objects asked for beside the one moved, each with the least it takes, which the move takes too: a
    /// route card's other routes.
    #[serde(default)]
    pub(crate) also: Vec<PartId>,
    /// What goes. Absent, the least the move takes.
    #[serde(default)]
    pub(crate) ticked: Option<Vec<PartId>>,
}

/// Each object the move lists, what the ticks leave broken, the commit both areas were read at, and once written,
/// the commit the move made.
#[derive(Default, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveAnswer {
    pub(crate) rows: Vec<MoveRow>,
    /// Why the ticks cannot move as they are. Empty when they can.
    exceptions: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// Moves what `ask` names into the other area, in one commit.
pub async fn move_objects(
    actor: Extension<Actor>,
    access: Extension<smeltr::leader::access::Access>,
    at: Addressed,
    ask: Json<MoveAsk>,
) -> Envelope<MoveAnswer> {
    asked(actor, access, at, ask, Go::Write).await
}

/// What moving `ask` would take, and why any tick cannot move. Writes nothing: a path under `preview` changes
/// nothing (`policy::changes_nothing`), so the gate holds it to that.
pub async fn preview_move(
    actor: Extension<Actor>,
    access: Extension<smeltr::leader::access::Access>,
    at: Addressed,
    ask: Json<MoveAsk>,
) -> Envelope<MoveAnswer> {
    asked(actor, access, at, ask, Go::List).await
}

/// Whether a move or an import is written, or only listed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Go {
    Write,
    List,
}

async fn asked(
    Extension(actor): Extension<Actor>,
    Extension(access): Extension<smeltr::leader::access::Access>,
    at: Addressed,
    Json(ask): Json<MoveAsk>,
    go: Go,
) -> Envelope<MoveAnswer> {
    // The gate judged the area moved from, which the path names. The one moved to is in the body.
    let need = crate::policy::Need::Area {
        area: ask.to.clone(),
        level: smeltr::leader::access::Level::Editor,
    };
    if !need.met_by(&access) {
        return Envelope::refused(Refusal::from(need));
    }
    smeltr::leader::blocking_answer(move || {
        let opened = at
            .open()
            .and_then(|from| Ok((from, Writing::open(&at.branch, at.base.as_deref(), &ask.to)?)));
        match opened {
            Ok((from, to)) => move_between(from, to, ask, go, &actor),
            Err(e) => not_written(e),
        }
    })
    .await
}

fn not_written(e: NotWritten) -> Envelope<MoveAnswer> {
    Envelope::refused(e.refusal)
}

/// [`move_objects`] once both areas are open. Blocking.
pub(crate) fn move_between(mut from: Writing, mut to: Writing, ask: MoveAsk, go: Go, author: &Actor) -> Envelope<MoveAnswer> {
    let what = format!("{} {:?}", describe(ask.part), ask.id);
    if to.area() == from.area() {
        return Envelope::refused(Refusal::invalid(format!("{what} is in {} already", to.area())));
    }
    if let Err(e) = from.can_move_to(&to) {
        return not_written(e);
    }
    let here = match all_objects(&from) {
        Ok(here) => here,
        Err(e) => return Envelope::refused(e),
    };
    let asked = (ask.part, ask.id.clone());
    if !here.ids().contains(&asked) {
        return Envelope::refused(Refusal::not_found(format!("{} has no {what}", from.area())));
    }
    // Out of `shared/`, the group moved to sees what moves where it is now: compared with the group's own alone.
    let found = match from.area() == config_file::SHARED_DIR {
        true => scope_of(&to, &here),
        false => import_api::here(&to, &here).map(|(there, _)| there),
    };
    let there = match found {
        Ok(found) => found,
        Err(e) => return Envelope::refused(e),
    };

    let (from_area, to_area) = (from.area().to_string(), to.area().to_string());
    let also = (ask.also.iter()).map(|o| (o.part, o.id.clone())).collect();
    let moving = Moving::new(&here, &there, asked, &from_area, &to_area).also(also);
    let ticked: Option<BTreeSet<(Part, String)>> =
        (ask.ticked.as_ref()).map(|t| t.iter().map(|o| (o.part, o.id.clone())).collect());
    let (rows, mut exceptions) = moving.list(ticked.as_ref());
    let going: BTreeSet<(Part, String)> = (rows.iter())
        .filter(|r| r.ticked)
        .map(|r| (r.listed.part, r.listed.id.clone()))
        .collect();

    // What moves replaces what the other area holds under the same id, so there is nothing to answer.
    let mut edits = None;
    if exceptions.is_empty() {
        match from.move_edits(&mut to, moving.going(&going)) {
            Ok(e) => edits = Some(e),
            Err(refused) => exceptions.extend(refused.errors.into_iter().map(|e| e.message)),
        }
    }

    let answer = MoveAnswer {
        rows,
        exceptions,
        at: from.parent.clone(),
        commit: None,
    };
    if go == Go::List {
        return Envelope::ok(answer);
    }
    let Some(edits) = edits else {
        let errors = answer.exceptions.iter().map(ApiError::new).collect();
        return Envelope::with(answer, StatusCode::CONFLICT, errors);
    };
    let message = format!("move {} {} to {to_area}", describe(ask.part), ask.id);
    let saved = from.commit_move(edits, &message, author);
    match saved.body.commit() {
        Some(commit) => Envelope::ok(MoveAnswer {
            commit: Some(commit.to_string()),
            ..answer
        }),
        None => Envelope::with(answer, saved.status, saved.errors),
    }
}

/// Every object `writing`'s area holds, its samples and tests among them.
fn all_objects(writing: &Writing) -> Result<Objects, Refusal> {
    let mut files = writing.files();
    let samples = config_file::list_samples(&mut files).0;
    let tests = config_file::list_tests(&mut files).0;
    let samples: Vec<&str> = samples.iter().map(|s| s.id.as_str()).collect();
    let tests: Vec<&str> = tests.iter().map(|t| t.id.as_str()).collect();
    scope_objects(writing, writing.prefix(), writing.read().file, &samples, &tests)
}

/// What `to`'s own area holds of the samples and tests `here` holds, beside its bundle.
fn scope_of(to: &Writing, here: &Objects) -> Result<Objects, Refusal> {
    let samples: Vec<&str> = here.samples.iter().map(|s| s.id.as_str()).collect();
    let tests: Vec<&str> = here.tests.iter().map(|t| t.id.as_str()).collect();
    scope_objects(to, to.prefix(), to.read().file, &samples, &tests)
}
