//! What a log is once it has left its call site: when, how bad, where from, a sentence, and whatever that
//! site knew at the time.

use std::borrow::Cow;
use std::time::SystemTime;

use tracing::field::{Field, Visit};

/// A detail field's value. Scalars only: a SIEM indexes these without being told a schema, and deep nesting is
/// what makes those mappings painful.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
}

/// One log, owned, on its way to the consumers.
#[derive(Debug, Clone)]
pub struct Event {
    pub timestamp: SystemTime,
    pub level: tracing::Level,
    /// Where it came from, and what a filter names. Our own sites are `tracing` targets and cost nothing to
    /// carry; one that arrived through the `log` facade owns a copy of its library's target.
    pub site: Cow<'static, str>,
    /// The constant sentence for this site. Anything that varies belongs in `fields`.
    pub message: String,
    /// What varied this time. `subject` by convention names the identity the event is about -- a destination, a
    /// source, a commit -- and never a position.
    pub fields: Vec<(Cow<'static, str>, Value)>,
    /// Attached by the layer. A call site does not know which worker it is running in and should not have to say.
    pub worker: Option<&'static str>,
    pub thread: Option<String>,
    /// Raised while this process was delivering logs, so it goes to the console and no further.
    pub local_only: bool,
    /// This event's number in this process's life. With the process's boot id it is what lets the leader take the
    /// same batch twice -- which a retried heartbeat does -- without writing it twice.
    pub seq: u64,
}

/// Takes an event's fields out of `tracing`'s borrowed form into one that can cross a thread.
///
/// `message` is a field like any other to `tracing`; it is the sentence to us. `log.target` is the originating
/// library of a record that came through the facade -- needed because `tracing-log` dispatches every record from
/// one callsite per level, so the event's own metadata says only `"log"`.
#[derive(Default)]
pub struct Collect {
    pub message: String,
    pub fields: Vec<(Cow<'static, str>, Value)>,
    pub log_target: Option<String>,
}

impl Collect {
    fn put(&mut self, field: &Field, value: Value) {
        match (field.name(), value) {
            ("message", Value::Str(s)) => self.message = s,
            ("log.target", Value::Str(s)) => self.log_target = Some(s),
            // `tracing-log` sets `log.module_path`, `log.file` and `log.line` beside the target on every bridged
            // record. The site already says which library spoke; the rest is a path on whatever machine built the
            // crate, and would be three junk keys in whatever reads these fields.
            (name, _) if name.starts_with("log.") => {}
            (name, value) => self.fields.push((name.into(), value)),
        }
    }
}

impl Visit for Collect {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::Str(value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, Value::Int(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, Value::Uint(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, Value::Bool(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, Value::Float(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::Str(value.to_string()));
    }

    /// Everything `tracing` cannot hand over as a scalar, the message included: its macros record a literal as
    /// formatting arguments, whose `Debug` is the sentence itself.
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.put(field, Value::Str(format!("{value:?}")));
    }
}
