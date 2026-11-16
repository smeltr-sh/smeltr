//! Questions asked of the stored series (design doc, Observability): what the Metrics page charts and the agent
//! reads. `GET /api/series` answers one -- a metric, narrowed by labels, split by one, over a window, per second or
//! in totals per bar -- and `GET /api/series/metrics` says what can be asked: each stored metric, its labels and the
//! values they hold.
//!
//! A series' subject key holds its labels and its worker group (`registry::subject_key`); `group` and, for the flow
//! counters, `worker` are labels like any other here. Only the groups the caller views are read.

use std::collections::{BTreeMap, BTreeSet};

use axum::extract::{Query, State};

use super::envelope::{Envelope, Refusal};
use super::series::{RegistryStore, SubjectPoints, Tier, chart_point_ms};
use super::store::StoreError;
use super::{AppState, GROUP_LABEL, MAX_WINDOW_MINUTES, access, warnings};

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
const DAY_MS: i64 = 24 * HOUR_MS;

/// How a counter's points are drawn: as a rate, or as what each bar added up to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Draw {
    #[default]
    PerSecond,
    Totals,
}

/// `GET /api/series`'s query.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesQuery {
    pub metric: String,
    /// `label:value` pairs, comma-separated; a series must match every one.
    #[serde(default)]
    pub filter: String,
    /// The label whose values become the answer's series; none adds every subject into one.
    #[serde(default)]
    pub by: String,
    #[serde(default = "default_window_minutes")]
    pub window_minutes: i64,
    #[serde(default)]
    pub draw: Draw,
    /// The reader's offset from UTC, east positive: where its hours and days begin.
    #[serde(default)]
    pub utc_offset_minutes: i64,
    /// How long before now the window ends: a week, for the line a chart compares itself with.
    #[serde(default)]
    pub ago_minutes: i64,
}

fn default_window_minutes() -> i64 {
    60
}

/// What `GET /api/series` answers.
#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesAnswer {
    pub metric: String,
    pub counter: bool,
    pub draw: Draw,
    /// How wide each point is, and where the first one starts.
    pub point_millis: i64,
    pub from_millis: i64,
    pub series: Vec<Split>,
}

/// One series of the answer: the subjects whose split label holds `name`, added together.
#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Split {
    /// The split label's value; empty when nothing splits the answer.
    pub name: String,
    /// `[start, value]`, oldest first. A point nothing landed in is absent rather than zero: an outage is not a quiet
    /// spell.
    pub points: Vec<(i64, f64)>,
}

/// One label of a subject key, or its group.
fn labels_of(subject_key: &str) -> Option<Vec<(String, String)>> {
    serde_json::from_str(subject_key).ok()
}

fn value_of<'a>(labels: &'a [(String, String)], name: &str) -> Option<&'a str> {
    labels.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

/// `filter` as label and value pairs, or why it is not one.
fn parse_filter(filter: &str) -> Result<Vec<(String, String)>, String> {
    filter
        .split(',')
        .filter(|part| !part.is_empty())
        .map(|part| match part.split_once(':') {
            Some((label, value)) if !label.is_empty() => Ok((label.to_string(), value.to_string())),
            _ => Err(format!("filter `{part}` is not label:value")),
        })
        .collect()
}

/// How wide each point is and where the window starts, for `draw` over the `window_minutes` ending at `end_ms`, at
/// `now_ms`. Bars are a minute wide up to an hour, an hour wide up to a day, a day wide beyond; each starts on the
/// reader's clock. A point is never finer than the store still keeps the window's oldest bucket, so a window ended a
/// week ago is read in the five-minute buckets that week is kept in.
fn grid(draw: Draw, window_minutes: i64, offset_ms: i64, end_ms: i64, now_ms: i64) -> (i64, i64) {
    let window_ms = window_minutes * MINUTE_MS;
    let point_ms = match draw {
        Draw::PerSecond => chart_point_ms(std::time::Duration::from_millis(window_ms as u64)),
        Draw::Totals if window_minutes <= 60 => MINUTE_MS,
        Draw::Totals if window_minutes <= 24 * 60 => HOUR_MS,
        Draw::Totals => DAY_MS,
    }
    .max(Tier::holding(now_ms - (end_ms - window_ms)).bucket_ms());
    let now_ms = end_ms;
    let start = |at: i64| (at - offset_ms).div_euclid(point_ms) * point_ms + offset_ms;
    let from_ms = match draw {
        // The window's oldest whole point, as `/api/flow` reads it.
        Draw::PerSecond => start(now_ms - window_ms),
        // As many bars as the window holds, the last the one still filling.
        Draw::Totals => start(now_ms) - (window_ms / point_ms - 1) * point_ms,
    };
    (point_ms, from_ms)
}

/// The answer to `query` at `now_ms`, from `store`, over the groups `sees` lets in.
pub fn answer(
    store: &RegistryStore,
    query: &SeriesQuery,
    sees: impl Fn(&str) -> bool,
    now_ms: i64,
) -> Result<Result<SeriesAnswer, String>, StoreError> {
    let filter = match parse_filter(&query.filter) {
        Ok(filter) => filter,
        Err(why) => return Ok(Err(why)),
    };
    let window_minutes = query.window_minutes.clamp(1, MAX_WINDOW_MINUTES);
    // Where the reader's midnight falls past UTC's: a reader east of Greenwich starts the day before UTC does.
    let offset_ms = -query.utc_offset_minutes.clamp(-14 * 60, 14 * 60) * MINUTE_MS;
    let counter = store
        .metric_subjects()?
        .into_iter()
        .find(|(metric, _, _)| *metric == query.metric)
        .map(|(_, _, counter)| counter);
    // A metric nothing has reported yet is a quiet one, not a wrong question: by Prometheus's naming, a counter's
    // name ends `_total`.
    let counter = counter.unwrap_or_else(|| query.metric.ends_with("_total"));
    // A gauge's bars would be its highest per bar, which is what its line already draws.
    let draw = if counter { query.draw } else { Draw::PerSecond };
    let end_ms = now_ms - query.ago_minutes.clamp(0, MAX_WINDOW_MINUTES) * MINUTE_MS;
    let (point_ms, from_ms) = grid(draw, window_minutes, offset_ms, end_ms, now_ms);
    let mut splits: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    for (subject_key, points) in store.points_from(&query.metric, counter, point_ms, offset_ms, from_ms, end_ms)? {
        let Some(labels) = labels_of(&subject_key) else {
            continue;
        };
        if !value_of(&labels, GROUP_LABEL).is_some_and(&sees) {
            continue;
        }
        if !filter.iter().all(|(k, v)| value_of(&labels, k) == Some(v.as_str())) {
            continue;
        }
        let name = if query.by.is_empty() {
            String::new()
        } else {
            match value_of(&labels, &query.by) {
                Some(value) => value.to_string(),
                None => continue,
            }
        };
        add(splits.entry(name).or_default(), points);
    }
    let per = |value: f64| match (counter, draw) {
        (true, Draw::PerSecond) => value / (point_ms as f64 / 1000.0),
        _ => value,
    };
    Ok(Ok(SeriesAnswer {
        metric: query.metric.clone(),
        counter,
        draw,
        point_millis: point_ms,
        from_millis: from_ms,
        series: splits
            .into_iter()
            .map(|(name, points)| Split {
                name,
                points: points.into_iter().map(|(at, value)| (at, per(value))).collect(),
            })
            .collect(),
    }))
}

/// `points` added into `into`, point by point.
fn add(into: &mut BTreeMap<i64, f64>, points: SubjectPoints) {
    for (at, value) in points {
        *into.entry(at).or_default() += value;
    }
}

/// One metric that can be asked of: whether it counts, and each label with the values its series hold.
#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredMetric {
    pub name: String,
    pub counter: bool,
    pub labels: BTreeMap<String, BTreeSet<String>>,
}

/// Every metric the store holds a series of in the groups `sees` lets in.
pub fn metrics(store: &RegistryStore, sees: impl Fn(&str) -> bool) -> Result<Vec<StoredMetric>, StoreError> {
    let mut out: BTreeMap<String, StoredMetric> = BTreeMap::new();
    for (name, subject_key, counter) in store.metric_subjects()? {
        let Some(labels) = labels_of(&subject_key) else {
            continue;
        };
        if !value_of(&labels, GROUP_LABEL).is_some_and(&sees) {
            continue;
        }
        let metric = out.entry(name.clone()).or_insert_with(|| StoredMetric {
            name,
            counter,
            labels: BTreeMap::new(),
        });
        for (label, value) in labels {
            metric.labels.entry(label).or_default().insert(value);
        }
    }
    Ok(out.into_values().collect())
}

/// What `GET /api/series/metrics` answers.
#[derive(serde::Serialize)]
pub struct MetricList {
    pub metrics: Vec<StoredMetric>,
}

/// `GET /api/series`. The gate lets in anyone viewing somewhere; the answer holds only the caller's groups.
pub async fn get_series(
    State(state): State<AppState>,
    axum::Extension(access): axum::Extension<access::Access>,
    Query(query): Query<SeriesQuery>,
) -> Result<Envelope<SeriesAnswer>, Refusal> {
    let Some(store) = state.registry.series() else {
        return Err(Refusal::unavailable("this leader keeps no series"));
    };
    let sees = |group: &str| access.allows(group, access::Level::Viewer);
    match answer(store, &query, sees, warnings::now_millis()) {
        Ok(Ok(answer)) => Ok(Envelope::ok(answer)),
        Ok(Err(why)) => Err(Refusal::bad_request(why)),
        Err(e) => Err(Refusal::internal(e.to_string())),
    }
}

/// `GET /api/series/metrics`.
pub async fn get_metrics(
    State(state): State<AppState>,
    axum::Extension(access): axum::Extension<access::Access>,
) -> Result<Envelope<MetricList>, Refusal> {
    let Some(store) = state.registry.series() else {
        return Ok(Envelope::ok(MetricList { metrics: Vec::new() }));
    };
    let sees = |group: &str| access.allows(group, access::Level::Viewer);
    metrics(store, sees)
        .map(|metrics| Envelope::ok(MetricList { metrics }))
        .map_err(|e| Refusal::internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const ROWS: &str = "smeltr_rows_polled_total";
    const QUEUE: &str = "smeltr_destination_queue_bytes";

    fn source(group: &str, id: &str, worker: &str) -> String {
        format!(r#"[["group","{group}"],["source","{id}"],["worker","{worker}"]]"#)
    }

    fn asked(metric: &str) -> SeriesQuery {
        SeriesQuery {
            metric: metric.to_string(),
            filter: String::new(),
            by: String::new(),
            window_minutes: 60,
            draw: Draw::PerSecond,
            utc_offset_minutes: 0,
            ago_minutes: 0,
        }
    }

    /// Noon on a day, so an hour's and a day's bars begin where a reader would count them.
    const NOON: i64 = 1_763_294_400_000;

    fn store() -> RegistryStore {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        for (group, id, worker, rows) in [
            ("east", "a", "e1", 600.0),
            ("east", "b", "e1", 60.0),
            ("west", "a", "w1", 6.0),
        ] {
            store
                .record_delta(ROWS, &source(group, id, worker), NOON - 60_000, rows)
                .unwrap();
        }
        store
    }

    fn ask(store: &RegistryStore, query: &SeriesQuery, sees: &[&str]) -> SeriesAnswer {
        answer(store, query, |g| sees.contains(&g), NOON).unwrap().unwrap()
    }

    fn values(answer: &SeriesAnswer) -> Vec<(&str, f64)> {
        answer
            .series
            .iter()
            .map(|s| (s.name.as_str(), s.points.iter().map(|(_, v)| v).sum()))
            .collect()
    }

    #[test]
    fn unsplit_adds_every_series_the_caller_sees_into_one() {
        let store = store();
        let rows = ask(&store, &asked(ROWS), &["east"]);
        assert!(rows.counter);
        // Ten-second points over an hour: 660 rows in one bucket is 66 a second.
        assert_eq!(rows.point_millis, 10_000);
        assert_eq!(values(&rows), [("", 66.0)], "west is not the caller's");
    }

    #[test]
    fn a_split_names_each_value_of_its_label_and_a_filter_narrows_first() {
        let store = store();
        let by_source = ask(
            &store,
            &SeriesQuery {
                by: "source".into(),
                ..asked(ROWS)
            },
            &["east", "west"],
        );
        assert_eq!(values(&by_source), [("a", 60.6), ("b", 6.0)]);
        let by_group = ask(
            &store,
            &SeriesQuery {
                by: "group".into(),
                filter: "source:a".into(),
                ..asked(ROWS)
            },
            &["east", "west"],
        );
        assert_eq!(values(&by_group), [("east", 60.0), ("west", 0.6)]);
        let by_worker = ask(
            &store,
            &SeriesQuery {
                by: "worker".into(),
                ..asked(ROWS)
            },
            &["east", "west"],
        );
        assert_eq!(values(&by_worker), [("e1", 66.0), ("w1", 0.6)]);
    }

    #[test]
    fn totals_add_up_each_bar_from_the_readers_clock() {
        let store = store();
        store
            .record_delta(ROWS, &source("east", "a", "e1"), NOON - 3 * HOUR_MS, 1000.0)
            .unwrap();
        let hours = ask(
            &store,
            &SeriesQuery {
                draw: Draw::Totals,
                window_minutes: 24 * 60,
                ..asked(ROWS)
            },
            &["east"],
        );
        assert_eq!(hours.point_millis, HOUR_MS);
        assert_eq!(
            hours.from_millis,
            NOON - 23 * HOUR_MS,
            "24 bars, the last one still filling"
        );
        assert_eq!(
            hours.series[0].points,
            [(NOON - 3 * HOUR_MS, 1000.0), (NOON - HOUR_MS, 660.0)],
            "an hour nothing landed in is absent"
        );
        // Five and a half hours ahead of UTC, noon UTC is 17:30, so the reader's day began 17.5 hours before.
        let days = ask(
            &store,
            &SeriesQuery {
                draw: Draw::Totals,
                window_minutes: 7 * 24 * 60,
                utc_offset_minutes: 330,
                ..asked(ROWS)
            },
            &["east"],
        );
        let today = NOON - (17 * HOUR_MS + 30 * 60_000);
        assert_eq!((days.point_millis, days.from_millis), (DAY_MS, today - 6 * DAY_MS));
        assert_eq!(days.series[0].points, [(today, 1660.0)]);
    }

    /// The line a chart compares itself with: the same window, ended a while before now.
    #[test]
    fn a_window_ended_before_now_holds_nothing_after_its_end() {
        let store = store();
        store
            .record_delta(ROWS, &source("east", "a", "e1"), NOON - 3 * HOUR_MS, 1000.0)
            .unwrap();
        let earlier = ask(
            &store,
            &SeriesQuery {
                draw: Draw::Totals,
                window_minutes: 24 * 60,
                ago_minutes: 120,
                ..asked(ROWS)
            },
            &["east"],
        );
        assert_eq!(earlier.from_millis, NOON - 2 * HOUR_MS - 23 * HOUR_MS);
        assert_eq!(earlier.series[0].points, [(NOON - 3 * HOUR_MS, 1000.0)]);

        // A week back, ten-second buckets are long gone: the hour is read in the five-minute ones that week is kept
        // in, and its last point holds only what landed before the window's end.
        let week = 7 * DAY_MS;
        let key = source("east", "a", "e1");
        store.record_delta(ROWS, &key, NOON - week - 6 * 60_000, 5.0).unwrap();
        store.record_delta(ROWS, &key, NOON - week + 60_000, 7.0).unwrap();
        store.maintain(NOON).unwrap();
        let week_before = ask(
            &store,
            &SeriesQuery {
                ago_minutes: 7 * 24 * 60,
                draw: Draw::Totals,
                ..asked(ROWS)
            },
            &["east"],
        );
        assert_eq!(week_before.point_millis, 5 * 60_000);
        assert_eq!(week_before.series[0].points, [(NOON - week - 10 * 60_000, 5.0)]);
    }

    #[test]
    fn a_gauge_draws_its_highest_and_never_totals() {
        let store = RegistryStore::open(Path::new(":memory:")).unwrap();
        let dest = r#"[["destination","d"],["group","east"]]"#;
        store.record_gauge_max(QUEUE, dest, NOON - 60_000, 5.0).unwrap();
        store.record_gauge_max(QUEUE, dest, NOON - 50_000, 9.0).unwrap();
        let queue = ask(
            &store,
            &SeriesQuery {
                draw: Draw::Totals,
                window_minutes: 24 * 60,
                ..asked(QUEUE)
            },
            &["east"],
        );
        assert!(!queue.counter);
        assert_eq!(queue.draw, Draw::PerSecond);
        assert_eq!(values(&queue), [("", 9.0)]);
    }

    #[test]
    fn a_metric_nothing_reported_yet_answers_empty() {
        let quiet = ask(&store(), &asked("smeltr_commits_total"), &["east"]);
        assert!(quiet.counter && quiet.series.is_empty());
    }

    #[test]
    fn what_cannot_be_asked_says_why() {
        let store = store();
        let refused = |query: SeriesQuery| answer(&store, &query, |_| true, NOON).unwrap().unwrap_err();
        assert!(
            refused(SeriesQuery {
                filter: "source".into(),
                ..asked(ROWS)
            })
            .contains("label:value")
        );
    }

    #[test]
    fn the_metric_list_holds_each_label_with_its_values_in_the_callers_groups() {
        let store = store();
        let listed = metrics(&store, |g| g == "east").unwrap();
        assert_eq!(listed.len(), 1);
        let rows = &listed[0];
        assert_eq!((rows.name.as_str(), rows.counter), (ROWS, true));
        let held = |label: &str| rows.labels[label].iter().map(String::as_str).collect::<Vec<_>>();
        assert_eq!(held("source"), ["a", "b"]);
        assert_eq!(held("group"), ["east"]);
        assert_eq!(held("worker"), ["e1"]);
    }
}
