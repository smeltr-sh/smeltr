//! Okta's System Log, read as a polling request: ascending, with no end, so every page links to the next, even a
//! page of nothing new. The cursor is the next link of the last page delivered, so a source resumes exactly after
//! what it delivered, and asks Okta's own order rather than its clock.

use std::time::{Duration, SystemTime};

use super::api::{Answer, Api, First, Page, Records, at_micros, next_link};
use super::http_api::array_at;
use crate::logs::stamp;
use crate::plan::topology::{ListStart, OktaLogsConfig};

/// The most events Okta returns in one page.
const PAGE: &str = "1000";

pub struct OktaLogs {
    logs: reqwest::Url,
    start: ListStart,
}

impl OktaLogs {
    pub fn new(config: &OktaLogsConfig) -> Result<Self, String> {
        let url = config.logs_url();
        Ok(OktaLogs {
            logs: reqwest::Url::parse(&url).map_err(|e| format!("{url:?} is not a URL: {e}"))?,
            start: config.start,
        })
    }
}

impl Api for OktaLogs {
    fn first(&mut self, cursor: Option<&str>, now: SystemTime) -> Result<First, String> {
        if let Some(cursor) = cursor {
            let url = reqwest::Url::parse(cursor).map_err(|e| format!("the cursor {cursor:?} is not a URL: {e}"))?;
            if url.origin() != self.logs.origin() || url.path() != self.logs.path() {
                return Err(format!("the cursor {cursor:?} is not a page of {}", self.logs));
            }
            return Ok(First {
                url: cursor.to_string(),
                ..First::default()
            });
        }
        let mut url = self.logs.clone();
        url.query_pairs_mut()
            .append_pair("sortOrder", "ASCENDING")
            .append_pair("limit", PAGE);
        let mut start = None;
        if self.start == ListStart::New {
            url.query_pairs_mut().append_pair("since", &stamp(now));
            // Fixed now, so a holder taking over before this poll is delivered asks from here too.
            start = Some(url.to_string());
        }
        Ok(First {
            url: url.to_string(),
            cursor: start,
            end: None,
        })
    }

    fn read(&mut self, url: &str, answer: Answer) -> Result<Page, String> {
        let next = next_link(url, &answer)?;
        let records = match array_at(&answer.body, "")? {
            Some(open) => Records::array(answer.body, open),
            None => Records::none(),
        };
        Ok(Page {
            records,
            cursor: next.clone(),
            next,
        })
    }

    /// Okta refuses with 403 a caller who lacks an admin role, which a scope alone does not give.
    fn refused_hint(&self, status: u16) -> Option<&'static str> {
        (status == 403).then_some("Check that the app, or the token's admin, has the Read-only Administrator role")
    }

    /// Okta says when its rate limit resets, in seconds since the epoch, and asks for a second more: its `Date`
    /// is truncated to the second.
    fn wait(&self, answer: &Answer, now: SystemTime) -> Option<Duration> {
        let secs: i64 = answer.header("x-rate-limit-reset")?.trim().parse().ok()?;
        let reset = at_micros(secs.checked_mul(1_000_000)?)?;
        Some(reset.duration_since(now).unwrap_or_default() + Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::topology::{OktaSignIn, SecretRef};

    fn okta(start: ListStart) -> OktaLogs {
        OktaLogs::new(&OktaLogsConfig {
            org: "https://acme.okta.com".into(),
            sign_in: OktaSignIn::Token(SecretRef::LeaderStored {
                name: "okta".into(),
                version: 1,
            }),
            start,
        })
        .unwrap()
    }

    fn answer(body: &str, next: Option<&str>) -> Answer {
        Answer {
            status: 200,
            headers: next
                .map(|n| ("link".to_string(), format!("<{n}>; rel=\"next\"")))
                .into_iter()
                .collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn the_first_poll_asks_in_order_and_new_fixes_its_start() {
        let first = okta(ListStart::Everything).first(None, at(1)).unwrap();
        assert_eq!(
            first,
            First {
                url: "https://acme.okta.com/api/v1/logs?sortOrder=ASCENDING&limit=1000".into(),
                cursor: None,
                end: None,
            }
        );
        let new = okta(ListStart::New).first(None, at(1)).unwrap();
        assert_eq!(
            new.url,
            "https://acme.okta.com/api/v1/logs?sortOrder=ASCENDING&limit=1000&since=1970-01-01T00%3A00%3A01.000Z"
        );
        assert_eq!(new.cursor.as_deref(), Some(new.url.as_str()), "fixed before reading");
    }

    #[test]
    fn a_poll_resumes_at_the_next_link_and_each_page_carries_its_own() {
        let mut api = okta(ListStart::New);
        let after = "https://acme.okta.com/api/v1/logs?after=abc&sortOrder=ASCENDING&limit=1000";
        assert_eq!(api.first(Some(after), at(1)).unwrap().url, after);
        let page = api
            .read(
                after,
                answer(r#"[{"uuid":"a"}, {"uuid":"b"}]"#, Some("/api/v1/logs?after=def")),
            )
            .unwrap();
        let next = "https://acme.okta.com/api/v1/logs?after=def";
        assert_eq!((page.cursor.as_deref(), page.next.as_deref()), (Some(next), Some(next)));
        let mut records = page.records;
        assert_eq!(records.next_record().unwrap(), br#"{"uuid":"a"}"#);
        assert_eq!(records.next_record().unwrap(), br#"{"uuid":"b"}"#);
        assert_eq!(records.next_record(), None);

        let empty = api.read(next, answer("[]", Some(next))).unwrap();
        assert_eq!(empty.cursor.as_deref(), Some(next));
    }

    #[test]
    fn a_cursor_from_elsewhere_is_refused() {
        let mut api = okta(ListStart::Everything);
        for cursor in [
            "https://other.okta.com/api/v1/logs?after=abc",
            "https://acme.okta.com/api/v1/users",
            "not a url",
        ] {
            assert!(api.first(Some(cursor), at(1)).is_err(), "{cursor}");
        }
    }

    #[test]
    fn an_answer_that_is_not_an_array_is_an_error() {
        let mut api = okta(ListStart::Everything);
        let url = "https://acme.okta.com/api/v1/logs";
        assert!(api.read(url, answer(r#"{"errorCode":"E0000011"}"#, None)).is_err());
    }

    #[test]
    fn a_rate_limit_waits_until_its_reset() {
        let api = okta(ListStart::Everything);
        let limited = Answer {
            status: 429,
            headers: vec![("x-rate-limit-reset".into(), "100".into())],
            body: Vec::new(),
        };
        assert_eq!(api.wait(&limited, at(40)), Some(Duration::from_secs(61)));
        assert_eq!(api.wait(&limited, at(400)), Some(Duration::from_secs(1)));
        assert_eq!(api.wait(&answer("", None), at(40)), None);
    }
}
