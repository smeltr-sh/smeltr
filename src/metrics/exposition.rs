//! The registry as Prometheus scrapes it: the text exposition format, version 0.0.4, one family per metric name, every
//! counter a total and every gauge its current value (`worker::metrics_endpoint` serves it).
//!
//! Prometheus refuses a whole scrape over one duplicate series, and two label sets that differ only in order, or only
//! in a label whose value is empty, are one series to it. So each sample's labels are put in its own terms before
//! anything is written: sorted, the first of a repeated name kept, an empty value dropped. A series given twice is
//! written once: two counters add, since each process counted its own share, and otherwise the first sample given
//! stands.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use super::{MetricSample, MetricValue};

/// What a scrape is answered with.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// `samples` in the text format, families by name. A series given twice sums its counters and otherwise keeps the
/// earlier sample, so a caller puts the samples it trusts most first. A sample whose metric or label name Prometheus
/// would refuse is left out, since one would cost every other series in the scrape.
pub fn render(samples: impl IntoIterator<Item = MetricSample>) -> String {
    let mut families: BTreeMap<&'static str, Family> = BTreeMap::new();
    // Where each series already written sits in its family.
    let mut seen: HashMap<(&'static str, Labels), usize> = HashMap::new();
    for sample in samples {
        let name = sample.key.name;
        if !is_metric_name(name) {
            continue;
        }
        let Some(labels) = series_labels(sample.key.labels) else {
            continue;
        };
        let family = families.entry(name).or_default();
        if let Some(&at) = seen.get(&(name, labels.clone())) {
            match (&mut family.series[at].1, sample.value) {
                (MetricValue::Counter(held), MetricValue::Counter(more)) => *held = held.saturating_add(more),
                (MetricValue::FloatCounter(held), MetricValue::FloatCounter(more)) => *held += more,
                _ => {}
            }
            continue;
        }
        seen.insert((name, labels.clone()), family.series.len());
        family.kind = Some(match (family.kind, sample.value) {
            (None | Some(Kind::Counter), MetricValue::Counter(_) | MetricValue::FloatCounter(_)) => Kind::Counter,
            (None | Some(Kind::Gauge), MetricValue::Gauge(_)) => Kind::Gauge,
            _ => Kind::Untyped,
        });
        family.series.push((labels, sample.value));
    }

    let mut out = String::new();
    for (name, mut family) in families {
        let kind = match family.kind {
            Some(Kind::Counter) => "counter",
            Some(Kind::Gauge) => "gauge",
            _ => "untyped",
        };
        writeln!(out, "# TYPE {name} {kind}").unwrap();
        // Sorted, so two scrapes of the same registry read the same.
        family.series.sort_by(|a, b| a.0.cmp(&b.0));
        for (labels, value) in family.series {
            out.push_str(name);
            if !labels.is_empty() {
                out.push('{');
                for (i, (k, v)) in labels.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(k);
                    out.push_str("=\"");
                    escape_label_value(v, &mut out);
                    out.push('"');
                }
                out.push('}');
            }
            out.push(' ');
            match value {
                MetricValue::Counter(v) => write!(out, "{v}").unwrap(),
                MetricValue::FloatCounter(v) | MetricValue::Gauge(v) => write_float(v, &mut out),
            }
            out.push('\n');
        }
    }
    out
}

/// A series' labels, in the order and spelling it is written with.
type Labels = Vec<(&'static str, String)>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    /// A name written as both, which the registry refuses per series but not per name.
    Untyped,
}

#[derive(Default)]
struct Family {
    kind: Option<Kind>,
    series: Vec<(Labels, MetricValue)>,
}

/// `labels` as the one series Prometheus reads them as, or `None` when a name is one it would refuse.
fn series_labels(mut labels: Labels) -> Option<Labels> {
    if !labels.iter().all(|(k, _)| is_label_name(k)) {
        return None;
    }
    labels.retain(|(_, v)| !v.is_empty());
    // Stable, so the first of a repeated name is the one kept.
    labels.sort_by_key(|(k, _)| *k);
    labels.dedup_by_key(|(k, _)| *k);
    Some(labels)
}

/// `[a-zA-Z_:][a-zA-Z0-9_:]*`.
fn is_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// `[a-zA-Z_][a-zA-Z0-9_]*`, and not one of the `__` names Prometheus keeps for itself.
fn is_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    !name.starts_with("__")
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A label value's backslash, double quote and line feed, escaped as the format asks.
fn escape_label_value(value: &str, out: &mut String) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// The format's own spellings for what Rust writes as `NaN`, `inf` and `-inf`.
fn write_float(v: f64, out: &mut String) {
    if v.is_nan() {
        out.push_str("NaN");
    } else if v == f64::INFINITY {
        out.push_str("+Inf");
    } else if v == f64::NEG_INFINITY {
        out.push_str("-Inf");
    } else {
        write!(out, "{v}").unwrap();
    }
}

#[cfg(test)]
mod fuzz;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricKey;

    fn sample(name: &'static str, labels: &[(&'static str, &str)], value: MetricValue) -> MetricSample {
        MetricSample {
            key: MetricKey::new(name, labels),
            value,
        }
    }

    #[test]
    fn counters_and_gauges_are_families_with_their_types() {
        let text = render([
            sample(
                "smeltr_rows_in_total",
                &[("source", "web"), ("thread", "reader-0")],
                MetricValue::Counter(12),
            ),
            sample("smeltr_queue_depth", &[("thread", "egress-0")], MetricValue::Gauge(3.5)),
            sample(
                "smeltr_rows_in_total",
                &[("source", "app"), ("thread", "reader-1")],
                MetricValue::Counter(0),
            ),
        ]);
        assert_eq!(
            text,
            "# TYPE smeltr_queue_depth gauge\n\
             smeltr_queue_depth{thread=\"egress-0\"} 3.5\n\
             # TYPE smeltr_rows_in_total counter\n\
             smeltr_rows_in_total{source=\"app\",thread=\"reader-1\"} 0\n\
             smeltr_rows_in_total{source=\"web\",thread=\"reader-0\"} 12\n"
        );
    }

    #[test]
    fn a_label_value_is_escaped() {
        let text = render([sample(
            "smeltr_rows_in_total",
            &[("source", "a\\b\"c\nd")],
            MetricValue::Counter(1),
        )]);
        assert!(
            text.contains(r#"smeltr_rows_in_total{source="a\\b\"c\nd"} 1"#),
            "{text}"
        );
    }

    #[test]
    fn one_series_is_written_once_however_its_labels_are_spelled() {
        let text = render([
            sample(
                "smeltr_rows_in_total",
                &[("thread", "t"), ("source", "web")],
                MetricValue::Counter(5),
            ),
            sample(
                "smeltr_rows_in_total",
                &[("source", "web"), ("thread", "t")],
                MetricValue::Counter(9),
            ),
            sample(
                "smeltr_rows_in_total",
                &[("source", "web"), ("route", ""), ("thread", "t")],
                MetricValue::Counter(7),
            ),
        ]);
        assert_eq!(
            text,
            "# TYPE smeltr_rows_in_total counter\nsmeltr_rows_in_total{source=\"web\",thread=\"t\"} 21\n"
        );
    }

    /// Each process counts its own share of a series both write, such as a capture's, so the scrape reads their sum.
    /// A gauge says one thing now, and the first given stands.
    #[test]
    fn a_series_given_twice_sums_its_counters_and_keeps_its_first_gauge() {
        let text = render([
            sample(
                "smeltr_capture_records_total",
                &[("thread", "capture")],
                MetricValue::Counter(4),
            ),
            sample("smeltr_queue_depth", &[], MetricValue::Gauge(1.0)),
            sample(
                "smeltr_capture_records_total",
                &[("thread", "capture")],
                MetricValue::Counter(100),
            ),
            sample("smeltr_queue_depth", &[], MetricValue::Gauge(7.0)),
        ]);
        assert!(
            text.contains("smeltr_capture_records_total{thread=\"capture\"} 104\n"),
            "{text}"
        );
        assert!(text.contains("smeltr_queue_depth 1\n"), "{text}");
    }

    #[test]
    fn a_name_written_as_both_kinds_is_untyped() {
        let text = render([
            sample("smeltr_lag", &[("a", "1")], MetricValue::Counter(1)),
            sample("smeltr_lag", &[("a", "2")], MetricValue::Gauge(2.0)),
        ]);
        assert!(text.starts_with("# TYPE smeltr_lag untyped\n"), "{text}");
    }

    #[test]
    fn a_name_prometheus_would_refuse_is_left_out() {
        let text = render([
            sample("smeltr-bad", &[], MetricValue::Counter(1)),
            sample("smeltr_ok", &[("bad-label", "x")], MetricValue::Counter(1)),
            sample("smeltr_ok", &[("__name__", "x")], MetricValue::Counter(1)),
            sample("smeltr_ok", &[], MetricValue::Counter(2)),
        ]);
        assert_eq!(text, "# TYPE smeltr_ok counter\nsmeltr_ok 2\n");
    }

    #[test]
    fn gauges_that_are_not_numbers_take_the_formats_spellings() {
        let text = render([
            sample("a", &[], MetricValue::Gauge(f64::NAN)),
            sample("b", &[], MetricValue::Gauge(f64::INFINITY)),
            sample("c", &[], MetricValue::Gauge(f64::NEG_INFINITY)),
        ]);
        assert!(
            text.contains("a NaN\n") && text.contains("b +Inf\n") && text.contains("c -Inf\n"),
            "{text}"
        );
    }
}
