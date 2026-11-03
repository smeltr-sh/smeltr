//! The `tracing` end of the buffer: what turns an event into something that can cross a thread.

use std::borrow::Cow;
use std::time::{Instant, SystemTime};

use tracing::span::{Attributes, Id, Record};
use tracing::{Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use super::event::{Collect, Event};
use super::hand_off;

/// Every `tracing` event in the process arrives here -- our own call sites, and anything a library reports
/// through the `log` facade.
///
/// A span of ours lends its fields to every line raised inside it, so a line deep in a commit says which source
/// and batch it was about without its call site knowing. Only while the span is enabled: the data path opens its
/// spans at trace, so below trace a line names what it is about itself. When one closes it is said, at its own
/// level, with how long it was open. A library's spans are neither: what it names in them is its own business, and
/// it opens far too many to say.
pub struct LogLayer;

/// What a span of ours knew, kept on it for the lines raised inside it and for its own closing line.
struct Known {
    fields: Vec<(Cow<'static, str>, super::event::Value)>,
    opened: Instant,
}

impl<S> Layer<S> for LogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// Asked once per callsite and cached until the levels change, so a site below its area's level costs its
    /// caller nothing at all: the field expressions and the message are never evaluated. A library's line through
    /// the `log` facade is asked each time, with the library's own target.
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        let target = metadata.target();
        let level = as_said_here(target, *metadata.level());
        // The highest any area keeps first, which needs no lock: a library's line through the facade asks every time.
        level <= super::max_level() && level <= super::levels::kept_at(target)
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if is_library(attrs.metadata().target()) {
            return;
        }
        let mut collected = Collect::default();
        attrs.record(&mut collected);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(Known {
                fields: collected.fields,
                opened: Instant::now(),
            });
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut extensions = span.extensions_mut();
        let Some(known) = extensions.get_mut::<Known>() else {
            return;
        };
        let mut collected = Collect::default();
        values.record(&mut collected);
        for (name, value) in collected.fields {
            match known.fields.iter_mut().find(|(n, _)| *n == name) {
                Some(field) => field.1 = value,
                None => known.fields.push((name, value)),
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut collected = Collect::default();
        event.record(&mut collected);

        // `tracing-log` dispatches every record from one callsite per level, whose target is the literal "log",
        // and carries the originating library in a field instead. Our own events have it in their metadata.
        let site = match collected.log_target.take() {
            Some(target) => Cow::Owned(target),
            None => Cow::Borrowed(event.metadata().target()),
        };
        // Innermost first, so a field the line or a nearer span names wins over the same name further out.
        let mut fields = collected.fields;
        for span in ctx.event_scope(event).into_iter().flatten() {
            lend(&span.extensions(), &mut fields);
        }
        let level = as_said_here(event.metadata().target(), *event.metadata().level());
        hand_off(shaped(level, site, collected.message, fields));
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let metadata = span.metadata();
        let (mut fields, took) = {
            let extensions = span.extensions();
            let Some(known) = extensions.get::<Known>() else { return };
            (known.fields.clone(), known.opened.elapsed())
        };
        for outer in span.scope().skip(1) {
            lend(&outer.extensions(), &mut fields);
        }
        let ms = super::millis(took);
        // The duration wins over a span field of the same name, so `ms` always means how long it was open.
        fields.retain(|(name, _)| name != "ms");
        fields.push(("ms".into(), super::event::Value::Uint(ms)));
        let message = format!("{} finished", metadata.name());
        hand_off(shaped(
            *metadata.level(),
            Cow::Borrowed(metadata.target()),
            message,
            fields,
        ));
    }
}

/// `known`'s fields, onto `fields` where they do not already name the same thing.
fn lend(
    extensions: &tracing_subscriber::registry::Extensions<'_>,
    fields: &mut Vec<(Cow<'static, str>, super::event::Value)>,
) {
    let Some(known) = extensions.get::<Known>() else { return };
    for (name, value) in &known.fields {
        if !fields.iter().any(|(n, _)| n == name) {
            fields.push((name.clone(), value.clone()));
        }
    }
}

/// One line, with what the layer attaches to every line: who and where said it, and its place in this process's
/// life.
fn shaped(
    level: Level,
    site: Cow<'static, str>,
    message: String,
    fields: Vec<(Cow<'static, str>, super::event::Value)>,
) -> Event {
    Event {
        timestamp: SystemTime::now(),
        level,
        site,
        message,
        fields,
        worker: super::worker(),
        thread: std::thread::current().name().map(str::to_string),
        local_only: super::delivering(),
        seq: super::next_seq(),
    }
}

/// Whether `target` is a library's: a module path, the `log` facade's relay, or librdkafka, which names itself.
fn is_library(target: &str) -> bool {
    target == "log" || target == "librdkafka" || target.contains("::")
}

/// `level` as a line from `target` is kept: a library's info is debug here and its debug is trace, since what a
/// library says at those levels Smeltr says better itself. Its warn and error are what they say. A library's
/// target is a module path; `tracing-log` relays the `log` facade from a callsite whose target is `log`.
pub(crate) fn as_said_here(target: &str, level: Level) -> Level {
    if !is_library(target) {
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

    /// What the layer hands on while `f` runs, everything kept.
    fn handed(f: impl FnOnce()) -> Vec<Event> {
        use tracing_subscriber::layer::SubscriberExt;
        super::super::levels::set(super::super::levels::Levels::at(Level::TRACE));
        let tap = super::super::tap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(LogLayer), f);
        tap.drain(100)
    }

    fn field<'a>(event: &'a Event, name: &str) -> Option<&'a super::super::event::Value> {
        event.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    /// A line inside a span of ours carries the span's fields, a nearer one's and its own winning, and the span's
    /// end is a line of its own with how long it was open.
    #[test]
    fn a_span_lends_its_fields_and_says_when_it_ends() {
        use super::super::event::Value;
        let events = handed(|| {
            let outer = tracing::trace_span!(target: "runtime.read", "a read", subject = "orders", batch = 7_u64);
            let _outer = outer.enter();
            let inner = tracing::trace_span!(target: "runtime.commit", "a commit", batch = 8_u64);
            let _inner = inner.enter();
            tracing::trace!(target: "runtime.committed", partitions = 2_u64, "offsets were committed");
        });
        let line = events.iter().find(|e| e.site == "runtime.committed").expect("the line");
        assert_eq!(field(line, "subject"), Some(&Value::Str("orders".into())));
        assert_eq!(field(line, "batch"), Some(&Value::Uint(8)), "the nearer span's");
        assert_eq!(field(line, "partitions"), Some(&Value::Uint(2)));
        let ends: Vec<&str> = events
            .iter()
            .filter(|e| field(e, "ms").is_some())
            .map(|e| e.site.as_ref())
            .collect();
        assert_eq!(ends, vec!["runtime.commit", "runtime.read"]);
        let commit = events.iter().find(|e| e.site == "runtime.commit").unwrap();
        assert_eq!(commit.message, "a commit finished");
        assert_eq!(
            field(commit, "subject"),
            Some(&Value::Str("orders".into())),
            "an end carries its outer spans' fields too"
        );
    }

    /// A span records only the fields its site names. `#[instrument]` would otherwise record every argument,
    /// and an argument can be a credential or a record's values, so every use of it says `skip_all`.
    #[test]
    fn every_instrumented_function_records_only_what_it_names() {
        fn walk(dir: &std::path::Path, found: &mut Vec<String>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    for (at, _) in text.match_indices("instrument(") {
                        let before = &text[..at];
                        if !(before.ends_with("#[") || before.ends_with("#[tracing::")) {
                            continue;
                        }
                        let attribute = &text[at..at + text[at..].find(")]").unwrap_or(text.len() - at)];
                        if !attribute.contains("skip_all") {
                            found.push(format!("{}: {attribute}", path.display()));
                        }
                    }
                }
            }
        }
        let mut found = Vec::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut found,
        );
        assert!(found.is_empty(), "instrumented without skip_all:\n{}", found.join("\n"));
    }

    /// Turning an area up opens its call sites at once, the same sites that were closed, and leaves the rest shut.
    #[test]
    fn an_area_turned_up_is_said_from_then_on() {
        use super::super::levels::{self, Levels};
        use tracing_subscriber::layer::SubscriberExt;
        let tap = super::super::tap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(LogLayer), || {
            let say = || {
                tracing::trace!(target: "kafka.lag_unmeasured", "lag cannot be measured");
                tracing::trace!(target: "store.statement", "a store ran a statement");
            };
            levels::set(Levels::at(Level::INFO));
            say();
            let mut up = Levels::at(Level::INFO);
            up.areas.insert("kafka".into(), Level::TRACE);
            levels::set(up);
            say();
            levels::set(Levels::at(Level::INFO));
            say();
        });
        let sites: Vec<String> = tap.drain(100).into_iter().map(|e| e.site.to_string()).collect();
        assert_eq!(sites, ["kafka.lag_unmeasured"]);
    }

    /// A span held and never entered, as a background pass holds one across its awaits, still says when it ends.
    #[test]
    fn a_span_never_entered_says_when_it_ends() {
        let events = handed(|| {
            let pass = tracing::trace_span!(target: "background.pass", "a background pass", task = "check alerts");
            drop(pass);
        });
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "a background pass finished");
        assert!(field(&events[0], "task").is_some() && field(&events[0], "ms").is_some());
    }

    /// A library's span lends nothing and says nothing.
    #[test]
    fn a_library_span_is_not_said() {
        let events = handed(|| {
            let span = tracing::trace_span!(target: "hyper::proto", "parse", token = "abc");
            let _entered = span.enter();
            tracing::trace!(target: "runtime.polled", "a source was polled");
        });
        assert_eq!(events.len(), 1);
        assert!(field(&events[0], "token").is_none());
    }

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
