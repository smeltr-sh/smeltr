//! What an operator asks of one worker: restart its deployment, or shut down.
//!
//! The leader reaches a worker only on the worker's own heartbeat, so a request waits here until the next one and
//! rides every answer after it until a beat names it taken. Held in memory: a leader that restarts forgets what it
//! had not yet handed over, and the operator asks again. A request no beat takes within [`untaken_for`] is dropped,
//! so a worker that went away just after it was asked is not shut down whenever it comes back.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::heartbeat::{Directive, DirectiveAction};

/// Every worker's standing request, and what each said it does when asked.
#[derive(Default)]
pub struct Directives(Mutex<HashMap<String, Held>>);

/// How long a request waits for a beat to take it: a minute, and never less than the window a worker counts as
/// heartbeating, so only a worker that has gone misses it.
pub fn untaken_for() -> Duration {
    Duration::from_secs(60).max(super::heartbeat_window())
}

#[derive(Default)]
struct Held {
    accepts: Vec<DirectiveAction>,
    pending: Option<(Directive, Instant)>,
}

impl Held {
    fn current(&mut self) -> Option<&Directive> {
        if self
            .pending
            .as_ref()
            .is_some_and(|(_, at)| at.elapsed() > untaken_for())
        {
            self.pending = None;
        }
        self.pending.as_ref().map(|(d, _)| d)
    }
}

impl Directives {
    /// Records what `worker_id`'s heartbeat said, and returns the request its answer carries.
    ///
    /// A beat that names the pending request as taken ends it. So does a worker saying it is stopping: whatever
    /// was pending, a worker on its way out acts on nothing more.
    pub fn heard(
        &self,
        worker_id: &str,
        accepts: Vec<DirectiveAction>,
        took: Option<&str>,
        stopping: bool,
    ) -> Option<Directive> {
        let mut workers = self.0.lock().unwrap();
        let held = workers.entry(worker_id.to_string()).or_default();
        held.accepts = accepts;
        if stopping || held.current().is_some_and(|d| Some(d.id.as_str()) == took) {
            held.pending = None;
        }
        held.current().cloned()
    }

    /// Asks `worker_id` for `action` on its next heartbeat, in place of anything it has not yet taken. Refused for
    /// a worker that has not said it does that.
    pub fn ask(&self, worker_id: &str, action: DirectiveAction) -> Result<(), String> {
        let mut workers = self.0.lock().unwrap();
        let Some(held) = workers.get_mut(worker_id).filter(|h| h.accepts.contains(&action)) else {
            return Err(format!(
                "worker {worker_id:?} has not said it can {} since this leader started",
                verb(action)
            ));
        };
        let directive = Directive {
            // Unique across leader restarts: a worker acts on each id once, and remembers the last.
            id: crate::leader::auth::generate_secret(),
            action,
        };
        held.pending = Some((directive, Instant::now()));
        Ok(())
    }

    /// What `worker_id` does when asked, as of its last heartbeat.
    pub fn accepts(&self, worker_id: &str) -> Vec<DirectiveAction> {
        self.0
            .lock()
            .unwrap()
            .get(worker_id)
            .map(|h| h.accepts.clone())
            .unwrap_or_default()
    }

    /// What `worker_id` has been asked and has not yet taken.
    pub fn pending(&self, worker_id: &str) -> Option<DirectiveAction> {
        self.0
            .lock()
            .unwrap()
            .get_mut(worker_id)
            .and_then(|h| h.current().map(|d| d.action))
    }

    /// Drops everything held about `worker_id`.
    pub fn forget(&self, worker_id: &str) {
        self.0.lock().unwrap().remove(worker_id);
    }
}

fn verb(action: DirectiveAction) -> &'static str {
    match action {
        DirectiveAction::Restart => "restart",
        DirectiveAction::Shutdown => "shut down",
        DirectiveAction::Unknown => "do that",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOTH: [DirectiveAction; 2] = [DirectiveAction::Restart, DirectiveAction::Shutdown];

    #[test]
    fn a_request_rides_every_answer_until_a_beat_names_it_taken() {
        let d = Directives::default();
        assert_eq!(d.heard("w1", BOTH.to_vec(), None, false), None);
        d.ask("w1", DirectiveAction::Restart).unwrap();
        let sent = d.heard("w1", BOTH.to_vec(), None, false).expect("sent");
        assert_eq!(sent.action, DirectiveAction::Restart);
        assert_eq!(d.heard("w1", BOTH.to_vec(), Some("older"), false), Some(sent.clone()));
        assert_eq!(d.heard("w1", BOTH.to_vec(), Some(&sent.id), false), None);
        assert_eq!(d.pending("w1"), None);
    }

    #[test]
    fn a_worker_that_says_it_is_stopping_is_asked_nothing_more() {
        let d = Directives::default();
        d.heard("w1", BOTH.to_vec(), None, false);
        d.ask("w1", DirectiveAction::Shutdown).unwrap();
        assert_eq!(d.heard("w1", BOTH.to_vec(), None, true), None);
    }

    #[test]
    fn a_worker_is_asked_only_what_it_said_it_does() {
        let d = Directives::default();
        assert!(d.ask("w1", DirectiveAction::Restart).is_err(), "never heard from");
        d.heard("w1", vec![DirectiveAction::Restart], None, false);
        assert!(
            d.ask("w1", DirectiveAction::Shutdown).is_err(),
            "a worker inside the leader"
        );
        assert!(d.ask("w1", DirectiveAction::Restart).is_ok());
    }

    #[test]
    fn a_new_request_replaces_one_not_yet_taken() {
        let d = Directives::default();
        d.heard("w1", BOTH.to_vec(), None, false);
        d.ask("w1", DirectiveAction::Restart).unwrap();
        d.ask("w1", DirectiveAction::Shutdown).unwrap();
        assert_eq!(d.pending("w1"), Some(DirectiveAction::Shutdown));
    }
}
