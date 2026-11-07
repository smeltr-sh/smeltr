use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpApiSourceDto {
    /// The whole address asked, with any fixed query parameters.
    pub url: String,
    /// `"GET"` (default) or `"POST"`.
    #[serde(default, skip_serializing_if = "ApiMethodDto::is_default")]
    pub method: ApiMethodDto,
    /// The JSON every POST carries. Absent is a POST of nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    /// Header name -> its value, committed to the config repository.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Header name -> the credential holding its value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub credentials: BTreeMap<String, SecretRefDto>,
    #[serde(flatten)]
    pub tls: TlsDto,
    /// `"body"` (default), `"lines"` or `"array"`.
    #[serde(default, skip_serializing_if = "ApiRecordsDto::is_default")]
    pub records: ApiRecordsDto,
    /// The JSON pointer to the array `"array"` reads, such as `/data`. Absent is an answer that is the array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_at: Option<String>,
    /// Whether a poll follows each answer's `Link: rel="next"` to the next page.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub follow_links: bool,
    /// The query parameter that asks for what happened since a time, sent the start of the last poll whose
    /// records every destination took. Absent reads everything every poll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_param: Option<String>,
    /// How many seconds before the last poll's start `sinceParam` asks from, for an API that records late.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub since_overlap_secs: u64,
    #[serde(default, skip_serializing_if = "PollIntervalSecs::is_default")]
    pub poll_interval_secs: PollIntervalSecs,
    /// What the first poll asks for, with `sinceParam`.
    #[serde(default, skip_serializing_if = "ListStartDto::is_default")]
    pub start: ListStartDto,
    /// Signing in as an app for the token every request carries.
    #[serde(flatten)]
    pub sign_in: AppSignInDto,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl SourceSpec for HttpApiSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let what = format!("source {:?}", s.id);
        check_api_source(&what, ("url", &self.url), &self.tls, self.poll_interval_secs, errors);
        check_http_headers(
            &what,
            &self.headers,
            &self.credentials,
            SOURCE_HEADERS,
            "for every request",
            errors,
        );
        check_app_sign_in(&what, &self.sign_in, &self.headers, &self.credentials, errors);
        check_records_at(&what, self.records, &self.records_at, errors);
        if self.body.is_some() && self.method != ApiMethodDto::Post {
            errors
                .at("body")
                .push(format!("{what} sends a body only with a POST; set method to \"POST\""));
        }
        match &self.since_param {
            Some(param) if param.is_empty() || !param.bytes().all(is_query_name_byte) => errors.at("sinceParam").push(
                format!("{what} names the query parameter {param:?}; a name is letters, digits and -._~[] only"),
            ),
            Some(param)
                if reqwest::Url::parse(self.url.trim())
                    .is_ok_and(|u| u.query_pairs().any(|(name, _)| name == param.as_str())) =>
            {
                errors.at("sinceParam").push(format!(
                    "{what} asks for {param:?} in its URL already, and sinceParam adds it on every poll; \
                         take it out of the URL"
                ))
            }
            Some(_) => {}
            None => {
                if self.since_overlap_secs != 0 {
                    errors.at("sinceOverlapSecs").push(format!(
                        "{what} sets an overlap and no sinceParam; the overlap is how far back sinceParam \
                             asks from"
                    ));
                }
                if self.start != ListStartDto::Everything {
                    errors.at("start").push(format!(
                        "{what} starts at new records and has no sinceParam to ask for them with; with \
                             none, every poll reads everything"
                    ));
                }
            }
        }
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let Self {
            unknown: _,
            url,
            method,
            body,
            headers,
            credentials,
            tls,
            records,
            records_at,
            follow_links,
            since_param,
            since_overlap_secs,
            poll_interval_secs,
            start,
            sign_in,
        } = self;
        SourceKind::Api(crate::plan::topology::ApiSourceConfig {
            api: crate::plan::topology::Api::HttpApi(crate::plan::topology::HttpApiConfig {
                url: url.trim().to_string(),
                method: match method {
                    ApiMethodDto::Get => crate::plan::topology::ApiMethod::Get,
                    ApiMethodDto::Post => crate::plan::topology::ApiMethod::Post,
                },
                body: body.as_ref().map(|b| b.to_string()),
                headers: headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                credentials: credentials.iter().map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
                records: match records {
                    ApiRecordsDto::Body => ApiRecords::Body,
                    ApiRecordsDto::Lines => ApiRecords::Lines,
                    ApiRecordsDto::Array => ApiRecords::Array(records_at.clone().unwrap_or_default()),
                },
                follow_links: *follow_links,
                since: since_param.as_ref().map(|param| crate::plan::topology::Since {
                    param: param.clone(),
                    overlap: Duration::from_secs(*since_overlap_secs),
                    start: match start {
                        ListStartDto::Everything => ListStart::Everything,
                        ListStartDto::New => ListStart::New,
                    },
                }),
                sign_in: to_app_sign_in(sign_in),
            }),
            tls: to_tls(tls, of, Side::Client),
            poll_interval: Duration::from_secs(poll_interval_secs.0),
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            credentials, sign_in, ..
        } = self;
        (credentials.values().map(|r| ("credentials", r)))
            .chain(sign_in.secrets())
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            credentials, sign_in, ..
        } = self;
        credentials.values_mut().chain(sign_in.secrets_mut()).collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        std::iter::once(("url", self.url.as_str()))
            .chain(self.sign_in.urls())
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::HTTP_API
    }

    fn single_worker(&self) -> bool {
        true
    }
}
