//! The basic API kind: a GET of one address, or a POST of a fixed body to it, whose answer is one record, a record
//! per line, or a record per element of a JSON array. With a `since` parameter it asks only for what is new; without one, every poll reads everything.
//!
//! Its cursor is the time a poll began, left once the whole poll is delivered. The next poll asks from it, less
//! the overlap, so what a poll read is asked for again only when the API recorded it late.

use std::time::SystemTime;

use super::api::{Answer, Api, First, Page, Records, at_micros, next_link};
use crate::format::json_scan::{Next, Reader};
use crate::logs::stamp;
use crate::ops::timeparse::parse_with_format;
use crate::plan::topology::{ApiRecords, HttpApiConfig, ListStart, Since};

pub struct HttpApi {
    url: reqwest::Url,
    records: ApiRecords,
    follow_links: bool,
    since: Option<Since>,
}

impl HttpApi {
    pub fn new(config: &HttpApiConfig) -> Result<Self, String> {
        Ok(HttpApi {
            url: reqwest::Url::parse(&config.url).map_err(|e| format!("{:?} is not a URL: {e}", config.url))?,
            records: config.records.clone(),
            follow_links: config.follow_links,
            since: config.since.clone(),
        })
    }
}

impl Api for HttpApi {
    fn first(&mut self, cursor: Option<&str>, now: SystemTime) -> Result<First, String> {
        let mut url = self.url.clone();
        let mut start = None;
        if let Some(since) = &self.since {
            let from = match cursor.map(parse_time) {
                Some(Some(at)) => Some(at.checked_sub(since.overlap).unwrap_or(SystemTime::UNIX_EPOCH)),
                Some(None) => return Err(format!("the cursor {:?} is not a time", cursor.unwrap_or_default())),
                None => match since.start {
                    ListStart::Everything => None,
                    ListStart::New => {
                        // Fixed now, so a holder taking over before this poll is delivered asks from here too.
                        start = Some(stamp(now));
                        Some(now)
                    }
                },
            };
            if let Some(from) = from {
                url.query_pairs_mut().append_pair(&since.param, &stamp(from));
            }
        }
        Ok(First {
            url: url.to_string(),
            cursor: start,
            end: self.since.as_ref().map(|_| stamp(now)),
        })
    }

    fn read(&mut self, url: &str, answer: Answer) -> Result<Page, String> {
        let next = match self.follow_links {
            true => next_link(url, &answer)?,
            false => None,
        };
        let records = match &self.records {
            ApiRecords::Body => Records::whole(answer.body),
            ApiRecords::Lines => Records::lines(answer.body),
            ApiRecords::Array(pointer) => match array_at(&answer.body, pointer)? {
                Some(open) => Records::array(answer.body, open),
                None => Records::none(),
            },
        };
        Ok(Page {
            records,
            cursor: None,
            next,
        })
    }
}

/// Where the `[` of the array at `pointer` in `body` is. `None` when nothing is there, or `null`. Checks that
/// `body` is JSON whole, which [`Records::array`] relies on.
pub(super) fn array_at(body: &[u8], pointer: &str) -> Result<Option<usize>, String> {
    let text = std::str::from_utf8(body).map_err(|e| not_json(body, e.valid_up_to()))?;
    let mut whole = Reader::of_str(text);
    if whole.skip().is_err() || !whole.end() {
        return Err(not_json(body, whole.at()));
    }
    // Where the value reached so far starts in `text`.
    let mut at = text.len() - text.trim_start().len();
    if !pointer.is_empty() {
        for token in pointer.strip_prefix('/').unwrap_or(pointer).split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            let mut r = Reader::of_str(&text[at..]);
            let mut found = None;
            match r.value().map_err(String::from)? {
                Next::Object => {
                    // Of a key given twice, the last value stands.
                    while let Some(key) = r.key().map_err(String::from)? {
                        let value = r.raw().map_err(String::from)?;
                        if key == token {
                            found = Some(value);
                        }
                    }
                }
                Next::Array => {
                    let wanted = token.parse::<usize>().ok();
                    let mut i = 0;
                    while found.is_none() && r.item().map_err(String::from)? {
                        let value = r.raw().map_err(String::from)?;
                        found = (wanted == Some(i)).then_some(value);
                        i += 1;
                    }
                }
                _ => {}
            }
            match found {
                // `value` borrows from `body`, so its place in `body` is the distance between them.
                Some(value) => at = value.as_ptr() as usize - body.as_ptr() as usize,
                None => return Ok(None),
            }
        }
    }
    match body.get(at) {
        Some(b'[') => Ok(Some(at)),
        Some(b'n') => Ok(None),
        _ => Err(format!(
            "the answer holds no array at {:?}; set where its records are",
            if pointer.is_empty() { "the top" } else { pointer }
        )),
    }
}

/// That `body` is not JSON, read as far as `at`.
fn not_json(body: &[u8], at: usize) -> String {
    let read = &body[..at.min(body.len())];
    let line = read.iter().filter(|&&b| b == b'\n').count() + 1;
    let column = read.len() - read.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1) + 1;
    format!("the answer is not JSON at line {line} column {column}")
}

/// What [`stamp`] wrote.
fn parse_time(text: &str) -> Option<SystemTime> {
    at_micros(parse_with_format(text, "%Y-%m-%dT%H:%M:%S.%fZ")?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn config(records: ApiRecords, since: Option<Since>) -> HttpApiConfig {
        HttpApiConfig {
            url: "https://api.x/events?limit=100".into(),
            method: crate::plan::topology::ApiMethod::Get,
            body: None,
            headers: Vec::new(),
            credentials: Vec::new(),
            records,
            follow_links: true,
            since,
            sign_in: None,
        }
    }

    fn since(start: ListStart, overlap: u64) -> Option<Since> {
        Some(Since {
            param: "since".into(),
            overlap: Duration::from_secs(overlap),
            start,
        })
    }

    fn answer(body: &str, link: Option<&str>) -> Answer {
        Answer {
            status: 200,
            headers: link.map(|l| ("link".to_string(), l.to_string())).into_iter().collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn at(millis: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
    }

    #[test]
    fn times_are_written_and_read_back() {
        let t = at(1_760_101_234_567);
        assert_eq!(stamp(t), "2025-10-10T13:00:34.567Z");
        assert_eq!(parse_time(&stamp(t)), Some(t));
        assert_eq!(parse_time("2025-10-10T13:00:34Z"), None);
        assert_eq!(parse_time("1969-12-31T23:59:59.000Z"), None);
        assert_eq!(parse_time("2025-13-10T13:00:34.567Z"), None);
    }

    fn records(page: Page) -> Vec<String> {
        let mut records = page.records;
        std::iter::from_fn(|| records.next_record())
            .map(|r| String::from_utf8(r).unwrap())
            .collect()
    }

    #[test]
    fn without_since_every_poll_asks_the_same_and_moves_nothing() {
        let mut api = HttpApi::new(&config(ApiRecords::Body, None)).unwrap();
        let first = api.first(None, at(0)).unwrap();
        assert_eq!(
            first,
            First {
                url: "https://api.x/events?limit=100".into(),
                cursor: None,
                end: None
            }
        );
        let page = api.read(&first.url, answer(r#"{"a":1}"#, None)).unwrap();
        assert_eq!(page.cursor, None);
        assert_eq!(records(page), [r#"{"a":1}"#]);
        assert!(records(api.read(&first.url, answer(" \n", None)).unwrap()).is_empty());
    }

    #[test]
    fn since_asks_from_the_cursor_less_the_overlap_and_a_poll_ends_at_its_start() {
        let mut api = HttpApi::new(&config(ApiRecords::Lines, since(ListStart::Everything, 60))).unwrap();
        let first = api.first(None, at(120_000)).unwrap();
        assert_eq!(first.cursor, None);
        assert_eq!(first.end.as_deref(), Some("1970-01-01T00:02:00.000Z"));
        assert_eq!(first.url, "https://api.x/events?limit=100");
        let page = api
            .read(
                &first.url,
                answer("a\r\n\nb\n", Some("<https://api.x/events?page=2>; rel=\"next\"")),
            )
            .unwrap();
        assert_eq!(page.next.as_deref(), Some("https://api.x/events?page=2"));
        assert_eq!(records(page), ["a", "b"]);

        let url = api.first(first.end.as_deref(), at(200_000)).unwrap().url;
        assert_eq!(url, "https://api.x/events?limit=100&since=1970-01-01T00%3A01%3A00.000Z");
        assert!(api.first(Some("yesterday"), at(0)).unwrap_err().contains("not a time"));
    }

    #[test]
    fn links_are_followed_only_when_asked() {
        let mut c = config(ApiRecords::Body, None);
        c.follow_links = false;
        let mut api = HttpApi::new(&c).unwrap();
        let page = api
            .read("https://api.x/e", answer("a", Some("<https://api.x/e?p=2>; rel=next")))
            .unwrap();
        assert_eq!(page.next, None);
    }

    #[test]
    fn starting_at_new_asks_from_the_first_poll() {
        let mut api = HttpApi::new(&config(ApiRecords::Body, since(ListStart::New, 0))).unwrap();
        let first = api.first(None, at(1000)).unwrap();
        assert!(
            first.url.ends_with("since=1970-01-01T00%3A00%3A01.000Z"),
            "{}",
            first.url
        );
        assert_eq!(
            first.cursor.as_deref(),
            Some("1970-01-01T00:00:01.000Z"),
            "fixed before reading"
        );
        let later = api.first(first.cursor.as_deref(), at(9000)).unwrap();
        assert_eq!(later.cursor, None);
    }

    fn elements(body: &[u8], pointer: &str) -> Result<Vec<String>, String> {
        let mut api = HttpApi::new(&config(ApiRecords::Array(pointer.into()), None)).unwrap();
        api.read("https://api.x/e", answer(std::str::from_utf8(body).unwrap(), None))
            .map(records)
    }

    #[test]
    fn an_array_is_found_by_its_pointer_and_its_elements_kept_as_written() {
        let body = br#"{"data": {"events": [ {"b":2, "a":1}, "x", 3 ]}, "a/b": [1]}"#;
        assert_eq!(
            elements(body, "/data/events").unwrap(),
            [r#"{"b":2, "a":1}"#, "\"x\"", "3"]
        );
        assert_eq!(elements(body, "/a~1b").unwrap(), ["1"]);
        assert_eq!(elements(b"[[1,2],[3]]", "/1").unwrap(), ["3"]);
        assert_eq!(elements(b" [1, 2] ", "").unwrap().len(), 2);
        assert!(elements(body, "/missing").unwrap().is_empty());
        assert!(elements(br#"{"data": null}"#, "/data").unwrap().is_empty());
        assert!(elements(body, "/data").unwrap_err().contains("no array at \"/data\""));
        assert!(elements(b"{\"a\":1}", "").unwrap_err().contains("the top"));
        assert!(elements(b"<html>", "").unwrap_err().contains("not JSON"));
        assert!(elements(b"[1, 2] [3]", "").unwrap_err().contains("not JSON"));
        assert_eq!(
            elements(br#"{"a": [1], "a": [2, 3]}"#, "/a").unwrap(),
            ["2", "3"],
            "the last key stands"
        );
        assert_eq!(elements(br#"{"a": [1, [2]]}"#, "/a/1").unwrap(), ["2"]);
        assert!(elements(b"[1, 2]", "/x").unwrap().is_empty(), "an array has no key");
        assert_eq!(
            array_at(b"{\n  \"a\": [1,\n  ?]}", "").unwrap_err(),
            "the answer is not JSON at line 3 column 3"
        );
    }
}
