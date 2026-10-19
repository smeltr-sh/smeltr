//! How a source that reads on its own clock waits after a read fails: a wait that doubles to a ceiling, or the one
//! the service asked for, with the failure kept to show and to log again while it is waited out.

use std::time::{Duration, Instant};

use crate::logs::Relog;

/// The wait after the first failure, and the least any asked-for wait is honoured at, so a service that asks for no
/// wait is not asked again in a tight loop.
pub const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The most a backoff grows to.
pub const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// The pace of a source that reads on its own clock. A failure sets when it may ask again; a read that lands puts
/// it back to the first backoff.
#[derive(Debug)]
pub struct Pacer {
    /// The kind of source, as its warning names it.
    kind: &'static str,
    /// Asked again no earlier than this, after a failure.
    retry_at: Option<Instant>,
    backoff: Duration,
    failing: Relog,
}

impl Pacer {
    pub fn new(kind: &'static str) -> Self {
        Pacer {
            kind,
            retry_at: None,
            backoff: FIRST_BACKOFF,
            failing: Relog::default(),
        }
    }

    /// Sets when the source may ask again, after `wait` or the next backoff, keeps `why` to show, and logs it when
    /// it is due.
    pub fn failed(&mut self, why: String, wait: Option<Duration>) {
        let wait = wait.map_or(self.backoff, |w| w.max(FIRST_BACKOFF));
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        self.retry_at = Some(Instant::now() + wait);
        self.failing.fail(why);
        self.relog();
    }

    /// Logs the failure under way when it is due: on its first failure, when its message changes, and again every
    /// [`crate::logs::RELOG_EVERY`] while it lasts, so its warning stands as long as it does.
    pub fn relog(&mut self) {
        let kind = self.kind;
        if let Some(why) = self.failing.due() {
            tracing::warn!(target: "source.poll", source = kind, error = %why, "a source could not read; asking again");
        }
    }

    /// A read landed: the failure is over, and the next one waits the first backoff.
    pub fn landed(&mut self) {
        self.retry_at = None;
        self.backoff = FIRST_BACKOFF;
        self.failing.clear();
    }

    /// The failure under way.
    pub fn error(&self) -> Option<&str> {
        self.failing.why()
    }

    /// Whether a failure's wait is still being waited out.
    pub fn retrying(&self) -> bool {
        self.retry_at.is_some_and(|at| at > Instant::now())
    }

    /// Sleeps out the failure's wait, until `deadline` at most.
    pub fn wait(&self, deadline: Instant) {
        if let Some(at) = self.retry_at {
            std::thread::sleep(at.min(deadline).saturating_duration_since(Instant::now()));
        }
    }

    #[cfg(test)]
    pub fn retry_at(&self) -> Option<Instant> {
        self.retry_at
    }

    #[cfg(test)]
    pub fn backoff(&self) -> Duration {
        self.backoff
    }

    /// Ends the wait, so a test asks again at once.
    #[cfg(test)]
    pub fn retry_now(&mut self) {
        self.retry_at = None;
    }

    /// The failure under way, when it is due its log, counted as logged.
    #[cfg(test)]
    fn due(&mut self) -> Option<&str> {
        self.failing.due()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_asked_wait_is_honoured_and_never_less_than_the_first_backoff() {
        let mut p = Pacer::new("test");
        let before = Instant::now();
        p.failed("busy".into(), Some(Duration::from_secs(60)));
        assert!(p.retry_at().unwrap() >= before + Duration::from_secs(60));
        p.failed("busy".into(), Some(Duration::ZERO));
        assert!(p.retry_at().unwrap() >= before + FIRST_BACKOFF);
        assert!(p.retrying());
    }

    #[test]
    fn the_backoff_doubles_to_its_ceiling_and_a_landed_read_resets_it() {
        let mut p = Pacer::new("test");
        for _ in 0..20 {
            p.failed("down".into(), None);
        }
        assert_eq!(p.backoff(), MAX_BACKOFF);
        assert_eq!(p.error(), Some("down"));
        p.landed();
        assert_eq!(p.backoff(), FIRST_BACKOFF);
        assert_eq!(p.error(), None);
        assert!(!p.retrying());
    }

    #[test]
    fn a_failure_is_due_its_log_once_until_its_message_changes() {
        let mut p = Pacer::new("test");
        p.failing.fail("down".into());
        assert_eq!(p.due(), Some("down"));
        assert_eq!(p.due(), None, "logged already");
        p.failing.fail("down".into());
        assert_eq!(p.due(), None, "the same failure again");
        p.failing.fail("refused".into());
        assert_eq!(p.due(), Some("refused"), "a changed message");
        p.landed();
        p.failing.fail("refused".into());
        assert_eq!(p.due(), Some("refused"), "a fault that returns after a landed read");
    }
}
