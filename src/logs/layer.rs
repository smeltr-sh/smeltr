//! The `tracing` end of the buffer: what turns an event into something that can cross a thread.

use std::borrow::Cow;
use std::time::SystemTime;

use tracing::Subscriber;
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
        *metadata.level() <= max_level()
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
            level: *event.metadata().level(),
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
