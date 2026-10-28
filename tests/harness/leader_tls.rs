//! A leader's TLS directory, outside a running leader.

use std::path::Path;
use std::sync::Arc;

use smeltr::leader::sqlite_store::SqliteStore;

/// Gives `tls_dir` a certificate list of its own, as `main` opens one before `tls::ensure`.
pub fn listing(tls_dir: &Path) -> Arc<SqliteStore> {
    let key = tls_dir.with_extension("key");
    let _ = std::fs::remove_file(&key);
    let list = Arc::new(SqliteStore::open(Path::new(":memory:"), &key).expect("a certificate list"));
    smeltr::leader::tls::read_list(tls_dir, list.clone());
    list
}
