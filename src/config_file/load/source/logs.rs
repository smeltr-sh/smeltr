use super::super::*;
use super::SourceSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogsSourceDto {
    /// The lowest severity it carries. See [`LevelLabel`].
    #[serde(default, skip_serializing_if = "LevelLabel::is_default")]
    pub min_level: LevelLabel,
}

impl SourceSpec for LogsSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let Self { min_level } = self;
        if crate::logs::level_for_label(&min_level.0).is_none() {
            errors.at("minLevel").push(format!(
                "source {:?} carries logs from {:?} down; it must be one of {}",
                s.id,
                min_level.0,
                crate::logs::LEVEL_LABELS.join(", ")
            ));
        }
    }

    fn to_kind(&self, _of: &str) -> SourceKind {
        // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
        let Self { min_level } = self;
        SourceKind::Logs(LogsSourceConfig {
            min_level: crate::logs::level_for_label(&min_level.0).unwrap_or(tracing::Level::INFO),
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
        SourceMeta::LOGS
    }
}
