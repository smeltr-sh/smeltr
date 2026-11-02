//! The `tracing` end of the buffer: what turns an event into something that can cross a thread.

use std::borrow::Cow;
use std::time::SystemTime;

use tracing::{Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

use super::event::{Collect, Event};
use super::{hand_off, max_level};

/// Every `tracing` event in the process arrives here -- our own call sites, and anything a library reports
/// through the `log` facade.
pub struct LogLayer;

impl<S: Subscriber> Layer<S> for LogLayer {
    /// Asked once per callsite and cached, so a site below the threshold costs its caller nothing at all: the
    /// field expressions and the message are never evaluated.
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        as_said_here(metadata.target(), *metadata.level()) <= max_level()
    }

    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut collected = Collect::default();
        event.record(&mut collected);

        // `tracing-log` dispatches every record from one callsite per level, whose target is the literal "log",
        // and carries the originating library in a field instead. Our own events have it in their metadata.
        let site = match collected.log_target.take() {
            Some(target) => Cow::Owned(target),
            None => Cow::Borrowed(event.metadata().target()),
        };

        hand_off(Event {
            timestamp: SystemTime::now(),
            level: as_said_here(event.metadata().target(), *event.metadata().level()),
            site,
            message: collected.message,
            fields: collected.fields,
            worker: super::worker(),
            thread: std::thread::current().name().map(str::to_string),
            local_only: super::delivering(),
            seq: super::next_seq(),
        });
    }
}

/// `level` as a line from `target` is kept: a library's info is debug here and its debug is trace, since what a
/// library says at those levels Smeltr says better itself. Its warn and error are what they say. A library's
/// target is a module path; `tracing-log` relays the `log` facade from a callsite whose target is `log`.
pub(crate) fn as_said_here(target: &str, level: Level) -> Level {
    if !(target == "log" || target.contains("::")) {
        return level;
    }
    match level {
        Level::INFO => Level::DEBUG,
        Level::DEBUG | Level::TRACE => Level::TRACE,
        severe => severe,
    }
}

/// The `log` facade's own ceiling for `max`, the threshold the lines are kept at: one step above it, since a
/// library's line is kept one step below what it says.
pub(crate) fn facade_ceiling(max: Level) -> Level {
    match max {
        Level::INFO => Level::WARN,
        Level::DEBUG => Level::INFO,
        same => same,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_library_line_is_kept_one_step_below_what_it_says() {
        assert_eq!(as_said_here("ureq::pool", Level::DEBUG), Level::TRACE);
        assert_eq!(as_said_here("log", Level::INFO), Level::DEBUG);
        assert_eq!(as_said_here("hyper_util::client", Level::WARN), Level::WARN);
        assert_eq!(as_said_here("egress.destination_stalled", Level::DEBUG), Level::DEBUG);
        assert_eq!(as_said_here("worker.heartbeat_answered", Level::INFO), Level::INFO);
    }

    #[test]
    fn the_facade_ceiling_admits_what_is_kept_and_no_more() {
        for max in [Level::ERROR, Level::WARN, Level::INFO, Level::DEBUG, Level::TRACE] {
            let ceiling = facade_ceiling(max);
            for said in [Level::ERROR, Level::WARN, Level::INFO, Level::DEBUG, Level::TRACE] {
                let kept = as_said_here("log", said) <= max;
                assert!(
                    !kept || said <= ceiling,
                    "at {max} a library's {said} is kept but the facade drops it"
                );
            }
            assert!(
                as_said_here("log", ceiling) <= max,
                "at {max} the facade admits {ceiling}, which is dropped"
            );
        }
    }
}
