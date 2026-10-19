//! Storage credentials a service vends, such as a table's catalog: kept, and asked for again shortly before they
//! expire.

use std::sync::Arc;
use std::time::SystemTime;

use object_store::CredentialProvider;
use object_store::aws::AwsCredential;

use crate::io::objects::RENEW_BEFORE;

/// Asks the vending service for fresh credentials, and when they expire. Err is why it could not.
type Renew = dyn Fn() -> Result<(Arc<AwsCredential>, SystemTime), String> + Send + Sync;

/// AWS credentials a service vends, renewed [`RENEW_BEFORE`] they expire.
pub struct Vended {
    /// Who vends them, as an error names it: `the catalog`, `Unity Catalog`.
    what: String,
    renew: Arc<Renew>,
    /// The credentials, and when they expire.
    cached: tokio::sync::Mutex<(Arc<AwsCredential>, SystemTime)>,
}

impl Vended {
    /// Starts from `first`, the credentials already vended and when they expire. `renew` blocks: it runs
    /// on tokio's blocking pool, one renewal at a time. `what` names the service in an error.
    pub fn new(
        what: String,
        first: (Arc<AwsCredential>, SystemTime),
        renew: impl Fn() -> Result<(Arc<AwsCredential>, SystemTime), String> + Send + Sync + 'static,
    ) -> Vended {
        Vended {
            what,
            renew: Arc::new(renew),
            cached: tokio::sync::Mutex::new(first),
        }
    }
}

impl std::fmt::Debug for Vended {
    /// Who vends them: the credentials are secrets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vended").field("what", &self.what).finish()
    }
}

#[async_trait::async_trait]
impl CredentialProvider for Vended {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut cached = self.cached.lock().await;
        if SystemTime::now() + RENEW_BEFORE < cached.1 {
            return Ok(cached.0.clone());
        }
        let renew = self.renew.clone();
        let renewed = tokio::task::spawn_blocking(move || renew())
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r)
            .map_err(|why| object_store::Error::Generic {
                store: "S3",
                source: format!("renewing the credentials {} vends: {why}", self.what).into(),
            })?;
        *cached = renewed;
        Ok(cached.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    fn credential(key_id: &str) -> Arc<AwsCredential> {
        Arc::new(AwsCredential {
            key_id: key_id.into(),
            secret_key: "SECRET".into(),
            token: None,
        })
    }

    #[test]
    fn credentials_are_renewed_only_when_due() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let asked = Arc::new(AtomicUsize::new(0));
        let fresh = SystemTime::now() + RENEW_BEFORE + Duration::from_secs(3600);
        let counted = asked.clone();
        let vended = Vended::new("the catalog".into(), (credential("first"), fresh), move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok((credential("renewed"), fresh))
        });
        assert_eq!(runtime.block_on(vended.get_credential()).unwrap().key_id, "first");
        assert_eq!(asked.load(Ordering::SeqCst), 0);

        let due = SystemTime::now() + RENEW_BEFORE - Duration::from_secs(1);
        let counted = asked.clone();
        let vended = Vended::new("the catalog".into(), (credential("first"), due), move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok((credential("renewed"), fresh))
        });
        assert_eq!(runtime.block_on(vended.get_credential()).unwrap().key_id, "renewed");
        assert_eq!(runtime.block_on(vended.get_credential()).unwrap().key_id, "renewed");
        assert_eq!(asked.load(Ordering::SeqCst), 1, "renewed credentials are kept");
        assert!(!format!("{vended:?}").contains("SECRET"));
    }

    #[test]
    fn a_failed_renewal_names_who_vends_them() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let vended = Vended::new("the catalog".into(), (credential("first"), SystemTime::now()), || {
            Err("the table is gone".into())
        });
        let error = runtime.block_on(vended.get_credential()).unwrap_err().to_string();
        assert!(
            error.contains("renewing the credentials the catalog vends: the table is gone"),
            "{error}"
        );
    }
}
