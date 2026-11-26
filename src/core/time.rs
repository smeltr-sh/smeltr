//! Time as the language holds it, a type of its own so a time is never mistaken for another number.

/// An instant: microseconds since the Unix epoch, as the language's `datetime` holds it.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct DateTime(i64);

impl DateTime {
    /// The wall clock now.
    pub fn now() -> DateTime {
        let since_epoch = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
        // A clock set before the epoch reads as the epoch.
        DateTime(since_epoch.map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX)))
    }

    pub const fn from_micros(micros: i64) -> DateTime {
        DateTime(micros)
    }

    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Whole milliseconds since the epoch, for what is stored or sent in milliseconds.
    pub const fn as_millis(self) -> i64 {
        self.0.div_euclid(1000)
    }
}

/// `span` in whole milliseconds, for what is stored or sent in milliseconds. A span longer than `i64::MAX` milliseconds
/// gives `i64::MAX`.
pub fn millis(span: std::time::Duration) -> i64 {
    i64::try_from(span.as_millis()).unwrap_or(i64::MAX)
}
