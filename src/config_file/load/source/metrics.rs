use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MetricsSourceDto {
    pub bucket_seconds: u64,
}

impl SourceSpec for MetricsSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self { bucket_seconds } = self;
        if *bucket_seconds == 0 {
            errors.at("bucketSeconds").push(format!(
                "source {:?} drains its bucket every 0 seconds; bucketSeconds is at least 1, since \
                     waiting out the bucket is what paces its reader",
                s.id
            ));
        }
    }

    fn to_kind(&self, _of: &str) -> SourceKind {
        let Self { bucket_seconds } = self;
        SourceKind::Metrics(MetricsSourceConfig {
            bucket: Duration::from_secs(*bucket_seconds),
        })
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        Vec::new()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        Vec::new()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        Vec::new()
    }

    fn tls(&self) -> Option<TlsDto> {
        None
    }

    fn declared_meta(&self) -> SourceMeta {
        SourceMeta::METRICS
    }
}
