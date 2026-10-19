//! The exposition's offline fuzzer. Threads of this process count and set series in one registry, the worker's other
//! process mirrors its own, and the process that set the mirrored gauges goes; names and labels come spelled every way
//! a registry could hold them, Prometheus's refused names among them. However it goes, what a scrape reads back
//! parses as the text format, names each series once under one `# TYPE` for its family, and holds every series it may:
//! each counter's total, summed over every process that counted it, and each gauge's current value, this process's
//! own over a mirrored one.

use std::collections::{HashMap, HashSet};

use super::super::*;
use super::*;
use crate::fuzz::{self, Coverage, Rng};

/// Metric names, one Prometheus refuses among them. None names a metric smeltr writes, so the website's table of
/// them leaves these out.
const NAMES: [&str; 5] = ["rows_in", "queue_depth", "lag", ":colon_name", "refused-name"];

/// Label names, two Prometheus refuses among them.
const KEYS: [&str; 6] = ["source", "thread", "process", "_under", "bad-label", "__reserved"];

/// Label values, the escaped characters and the empty one among them.
const VALUES: [&str; 6] = ["web", "reader-0", "été", "a\\b", "say \"hi\"\nthere", ""];

/// A series as Prometheus reads it.
type Series = (String, Vec<(String, String)>);

#[test]
fn a_scrape_names_each_series_once_with_its_total_or_current_value() {
    let mut cov = Coverage::new(
        [
            "a counter",
            "a counter of seconds",
            "a gauge",
            "a gauge that is not a number",
            "a counter on a gauge's name",
            "a mirrored counter",
            "a mirrored gauge",
            "the mirrored gauges forgotten",
            "a series both processes hold",
            "labels in another order",
            "a label repeated",
            "an empty label value",
            "an escaped label value",
            "a refused metric name",
            "a refused label name",
            "an untyped family",
            "an empty registry",
        ]
        .map(String::from),
    );
    fuzz::run(1..301, |seed| {
        let mut rng = Rng::seeded(seed);
        let registry = Metrics::new();
        // What the registry should hold, as it holds it.
        let mut own: HashMap<MetricKey, MetricValue> = HashMap::new();
        let mut mirrored: HashMap<MetricKey, MetricValue> = HashMap::new();
        for _ in 0..rng.below(40) {
            let key = random_key(&mut rng, &mut cov);
            let labels: Vec<(&'static str, &str)> = key.labels.iter().map(|(k, v)| (*k, v.as_str())).collect();
            match rng.below(5) {
                0 => {
                    let delta = 1 + rng.below(1000) as u64;
                    registry.incr(key.name, &labels, delta);
                    match own.entry(key).or_insert(MetricValue::Counter(0)) {
                        MetricValue::Counter(v) => *v += delta,
                        _ => cov.reach("a counter on a gauge's name"),
                    }
                    cov.reach("a counter");
                }
                1 => {
                    let seconds = (1 + rng.below(1_000_000)) as f64 / 1e6;
                    registry.incr_seconds(key.name, &labels, seconds);
                    match own.entry(key).or_insert(MetricValue::FloatCounter(0.0)) {
                        MetricValue::FloatCounter(v) => *v += seconds,
                        _ => cov.reach("a counter on a gauge's name"),
                    }
                    cov.reach("a counter of seconds");
                }
                2 => {
                    let value = random_float(&mut rng, &mut cov);
                    registry.gauge(key.name, &labels, value);
                    own.insert(key, MetricValue::Gauge(value));
                    cov.reach("a gauge");
                }
                3 => {
                    let value = if rng.chance(2) {
                        cov.reach("a mirrored counter");
                        MetricValue::Counter(rng.below(1000) as u64)
                    } else {
                        cov.reach("a mirrored gauge");
                        MetricValue::Gauge(random_float(&mut rng, &mut cov))
                    };
                    registry.mirror([MetricSample {
                        key: key.clone(),
                        value,
                    }]);
                    let held = mirrored.entry(key).or_insert(MetricValue::Counter(0));
                    match (held, value) {
                        (MetricValue::Counter(total), MetricValue::Counter(more)) => *total += more,
                        (held, value) => *held = value,
                    }
                }
                _ => {
                    registry.forget_mirrored();
                    mirrored.retain(|_, value| value.is_counter());
                    cov.reach("the mirrored gauges forgotten");
                }
            }
        }
        if own.is_empty() && mirrored.is_empty() {
            cov.reach("an empty registry");
        }

        let text = render(registry.snapshot().into_iter().chain(registry.snapshot_mirrored()));
        let scraped = parse(&text)?;

        // Every series the registry may show, with what it holds of it: this process's own first, then the mirror's.
        let mut held: HashMap<Series, (Vec<MetricValue>, Vec<MetricValue>)> = HashMap::new();
        for (is_own, values) in [(true, &own), (false, &mirrored)] {
            for (key, value) in values {
                let Some(series) = expected_series(key, &mut cov) else {
                    continue;
                };
                let entry = held.entry(series).or_default();
                if is_own { &mut entry.0 } else { &mut entry.1 }.push(*value);
            }
        }
        let shown: HashSet<&Series> = scraped.samples.iter().map(|(s, _)| s).collect();
        if shown.len() != scraped.samples.len() {
            return Err(format!("a series is written twice:\n{text}"));
        }
        for (series, value) in &scraped.samples {
            let Some((own, mirrored)) = held.get(series) else {
                return Err(format!("{series:?} is in no registry:\n{text}"));
            };
            if !own.is_empty() && !mirrored.is_empty() {
                cov.reach("a series both processes hold");
            }
            let counters = |values: &[MetricValue]| values.iter().all(|v| matches!(v, MetricValue::Counter(_)));
            let sum = |values: &[&Vec<MetricValue>]| -> u64 {
                values
                    .iter()
                    .flat_map(|v| v.iter())
                    .map(|v| match v {
                        MetricValue::Counter(n) => *n,
                        _ => 0,
                    })
                    .sum()
            };
            let seconds = |values: &[MetricValue]| values.iter().all(|v| matches!(v, MetricValue::FloatCounter(_)));
            // Floats summed in a map's order differ in their last bits.
            let fsum = |values: &[&Vec<MetricValue>]| -> f64 {
                values
                    .iter()
                    .flat_map(|v| v.iter())
                    .map(|v| match v {
                        MetricValue::FloatCounter(n) => *n,
                        _ => 0.0,
                    })
                    .sum()
            };
            // Within one registry the order of a series' spellings is a map's, so only a series every one of whose
            // first group is a counter has one right answer: every counter given, summed.
            let first = if own.is_empty() { mirrored } else { own };
            let fine = if counters(first) {
                let total = if own.is_empty() {
                    sum(&[mirrored])
                } else {
                    sum(&[own, mirrored])
                };
                matches!(value, Read::Counter(n) if *n == total)
            } else if seconds(first) {
                let total = if own.is_empty() {
                    fsum(&[mirrored])
                } else {
                    fsum(&[own, mirrored])
                };
                let read = match value {
                    Read::Counter(n) => *n as f64,
                    Read::Float(f) => *f,
                };
                (read - total).abs() <= total * 1e-9
            } else {
                let all: Vec<&MetricValue> = own.iter().chain(mirrored).collect();
                match value {
                    Read::Counter(n) => *n <= sum(&[own, mirrored]) || all.iter().any(|v| same(v, value)),
                    Read::Float(f) => all.iter().any(|v| same(v, value)) || *f <= fsum(&[own, mirrored]) * (1.0 + 1e-9),
                }
            };
            if !fine {
                return Err(format!(
                    "{series:?} reads {value:?}, from {own:?} and {mirrored:?}:\n{text}"
                ));
            }
            let kind = &scraped.types[&series.0];
            if kind == "untyped" {
                cov.reach("an untyped family");
            } else if kind != "counter" && kind != "gauge" {
                return Err(format!("{} is typed {kind}:\n{text}", series.0));
            }
        }
        for series in held.keys() {
            if !shown.contains(series) {
                return Err(format!("{series:?} is missing:\n{text}"));
            }
        }
        Ok(())
    });
    cov.assert_complete();
}

/// A key as a registry could hold it, labels in any order, repeated or empty.
fn random_key(rng: &mut Rng, cov: &mut Coverage) -> MetricKey {
    let name = *rng.pick(&NAMES);
    let mut labels: Vec<(&'static str, String)> = Vec::new();
    for _ in 0..rng.below(4) {
        let key = *rng.pick(&KEYS);
        let value = rng.pick(&VALUES).to_string();
        if labels.iter().any(|(k, _)| *k == key) {
            cov.reach("a label repeated");
        }
        labels.push((key, value));
    }
    if labels.windows(2).any(|w| w[0].0 > w[1].0) {
        cov.reach("labels in another order");
    }
    MetricKey { name, labels }
}

fn random_float(rng: &mut Rng, cov: &mut Coverage) -> f64 {
    match rng.below(8) {
        0 => {
            cov.reach("a gauge that is not a number");
            *rng.pick(&[f64::NAN, f64::INFINITY, f64::NEG_INFINITY])
        }
        1 => -(rng.below(100) as f64) / 4.0,
        _ => rng.below(1_000_000) as f64 / 8.0,
    }
}

/// What Prometheus reads `key` as, or `None` for one the scrape leaves out.
fn expected_series(key: &MetricKey, cov: &mut Coverage) -> Option<Series> {
    let valid_name = key
        .name
        .chars()
        .enumerate()
        .all(|(i, c)| c.is_ascii_alphabetic() || c == '_' || c == ':' || (i > 0 && c.is_ascii_digit()));
    if !valid_name {
        cov.reach("a refused metric name");
        return None;
    }
    if key.labels.iter().any(|(k, _)| k.contains('-') || k.starts_with("__")) {
        cov.reach("a refused label name");
        return None;
    }
    let mut labels: Vec<(String, String)> = Vec::new();
    for (k, v) in &key.labels {
        if v.is_empty() {
            cov.reach("an empty label value");
            continue;
        }
        if v.contains(['\\', '"', '\n']) {
            cov.reach("an escaped label value");
        }
        if !labels.iter().any(|(held, _)| held == k) {
            labels.push((k.to_string(), v.clone()));
        }
    }
    labels.sort();
    Some((key.name.to_string(), labels))
}

#[derive(Debug)]
enum Read {
    Counter(u64),
    Float(f64),
}

fn same(held: &MetricValue, read: &Read) -> bool {
    match (held, read) {
        (MetricValue::Counter(a), Read::Counter(b)) => a == b,
        (MetricValue::Gauge(a) | MetricValue::FloatCounter(a), Read::Float(b)) => (a.is_nan() && b.is_nan()) || a == b,
        // A float that holds a whole number reads as one.
        (MetricValue::Gauge(a) | MetricValue::FloatCounter(a), Read::Counter(b)) => *a == *b as f64,
        _ => false,
    }
}

struct Scraped {
    types: HashMap<String, String>,
    samples: Vec<(Series, Read)>,
}

/// The text format, read strictly: a family's `# TYPE` once and before its samples, and every sample in a family
/// that has one.
fn parse(text: &str) -> Result<Scraped, String> {
    let mut types: HashMap<String, String> = HashMap::new();
    let mut samples = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').ok_or(format!("a bad TYPE line {line:?}"))?;
            if types.insert(name.to_string(), kind.to_string()).is_some() {
                return Err(format!("{name} is typed twice"));
            }
            current = Some(name.to_string());
            continue;
        }
        let (series, value) = sample_line(line)?;
        if current.as_deref() != Some(series.0.as_str()) {
            return Err(format!("{line:?} is outside its family"));
        }
        samples.push((series, value));
    }
    if !text.is_empty() && !text.ends_with('\n') {
        return Err("the text does not end its last line".to_string());
    }
    Ok(Scraped { types, samples })
}

fn sample_line(line: &str) -> Result<(Series, Read), String> {
    let bad = || format!("a bad sample line {line:?}");
    let name_end = line.find(['{', ' ']).ok_or_else(bad)?;
    let name = &line[..name_end];
    let mut rest = &line[name_end..];
    let mut labels = Vec::new();
    if let Some(mut inner) = rest.strip_prefix('{') {
        loop {
            let (key, after) = inner.split_once("=\"").ok_or_else(bad)?;
            let mut value = String::new();
            let mut chars = after.char_indices();
            let end = loop {
                match chars.next().ok_or_else(bad)? {
                    (i, '"') => break i,
                    (_, '\\') => match chars.next().ok_or_else(bad)?.1 {
                        '\\' => value.push('\\'),
                        '"' => value.push('"'),
                        'n' => value.push('\n'),
                        _ => return Err(bad()),
                    },
                    (_, c) => value.push(c),
                }
            };
            labels.push((key.to_string(), value));
            let after = &after[end + 1..];
            if let Some(more) = after.strip_prefix(',') {
                inner = more;
            } else if let Some(done) = after.strip_prefix('}') {
                rest = done;
                break;
            } else {
                return Err(bad());
            }
        }
    }
    let value = rest.strip_prefix(' ').ok_or_else(bad)?;
    let read = match value {
        "NaN" => Read::Float(f64::NAN),
        "+Inf" => Read::Float(f64::INFINITY),
        "-Inf" => Read::Float(f64::NEG_INFINITY),
        v => match v.parse::<u64>() {
            Ok(n) => Read::Counter(n),
            Err(_) => Read::Float(v.parse().map_err(|_| bad())?),
        },
    };
    let mut sorted = labels.clone();
    sorted.sort();
    if sorted != labels {
        return Err(format!("{line:?} does not sort its labels"));
    }
    Ok(((name.to_string(), labels), read))
}
