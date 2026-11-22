//! The Kerberos ticket a Kafka client signs in with over GSSAPI, got and renewed in this process.
//!
//! librdkafka's own way is to run `sasl.kerberos.kinit.cmd` through `system()`, which needs a shell and MIT's `kinit`
//! on the host. The image has neither. So smeltr turns that off (`sasl.kerberos.min.time.before.relogin` 0) and does
//! what `kinit -t <keytab> -k <principal>` did, through the MIT krb5 linked in for Cyrus SASL: a ticket from the
//! keytab, stored in the process's default credential cache, which is where Cyrus's GSSAPI reads it.
//! [`cache_in_memory`] makes that cache one in memory, so no file is written, as a read-only root filesystem needs.
//!
//! The default cache is one per process, so a process holds one Kerberos identity at a time. A second principal
//! while the first is held is refused rather than left to overwrite it.
//!
//! A client with no keytab gets nothing here: it signs in with whatever ticket the cache already holds, as one an
//! operator keeps fresh outside smeltr and names with `KRB5CCNAME`.
//!
//! On Windows librdkafka signs in through SSPI as the account smeltr runs under, and none of this applies.

use rdkafka::ClientConfig;

use crate::io::IoError;

/// Makes the default credential cache one in this process's memory, unless `KRB5CCNAME` already names one.
///
/// Call it at the start of `main`, before any other thread exists: it sets the environment, which races readers. A
/// child process inherits the name and gets a cache of its own, since a memory cache lives in one process.
pub fn cache_in_memory() {
    if cfg!(target_os = "linux") && std::env::var_os("KRB5CCNAME").is_none_or(|v| v.is_empty()) {
        // SAFETY: no other thread exists yet, as the caller ensures.
        unsafe { std::env::set_var("KRB5CCNAME", "MEMORY:smeltr") };
    }
}

/// What librdkafka names the client when a config names no principal.
const DEFAULT_PRINCIPAL: &str = "kafkaclient";

/// Whether `config` signs in over GSSAPI.
pub(super) fn selected(config: &ClientConfig) -> bool {
    selects(|key| config.get(key))
}

/// Whether a client whose properties `get` reads signs in over GSSAPI: when it names it, or under a SASL protocol that
/// names no mechanism, since GSSAPI is librdkafka's default.
pub(super) fn selects<'a>(get: impl Fn(&str) -> Option<&'a str>) -> bool {
    match get("sasl.mechanism").or_else(|| get("sasl.mechanisms")) {
        Some(mechanism) => mechanism.eq_ignore_ascii_case("GSSAPI"),
        None => get("security.protocol").is_some_and(|p| p.to_ascii_uppercase().starts_with("SASL")),
    }
}

/// The principal and keytab `config` signs in with, when it names a keytab.
fn identity(config: &ClientConfig) -> Option<Identity> {
    let keytab = config.get("sasl.kerberos.keytab").filter(|k| !k.is_empty())?;
    Some(Identity {
        principal: config
            .get("sasl.kerberos.principal")
            .filter(|p| !p.is_empty())
            .unwrap_or(DEFAULT_PRINCIPAL)
            .to_string(),
        keytab: keytab.to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    principal: String,
    keytab: String,
}

/// A held ticket. The ticket is renewed for as long as one of these lives, so a client keeps its own beside it.
#[derive(Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) struct Held {
    _identity: std::sync::Arc<Identity>,
}

/// The ticket `config` signs in with, got now if it is not already held, and renewed from then on. `None` when
/// `config` does not sign in over GSSAPI, names no keytab, or runs where SSPI signs in instead.
pub(super) fn hold(config: &ClientConfig) -> Result<Option<Held>, IoError> {
    if !selected(config) {
        return Ok(None);
    }
    let Some(identity) = identity(config) else {
        return Ok(None);
    };
    imp::hold(identity)
}

/// Whether a process holding `held` may take `asked` too: one identity per process, since both share the default
/// cache, and a second principal would overwrite the first's ticket.
#[cfg(target_os = "linux")]
fn conflict(held: &Identity, asked: &Identity) -> Option<String> {
    (held.principal != asked.principal).then(|| {
        format!(
            "this process already signs in to Kafka as Kerberos principal {:?}, and one process holds one Kerberos identity: \
             {:?} would replace its ticket",
            held.principal, asked.principal
        )
    })
}

#[cfg(target_os = "linux")]
mod imp {
    use std::ffi::{CStr, CString, c_char, c_int, c_long, c_void};
    use std::sync::{Arc, Mutex, Once, Weak};
    use std::time::Duration;

    use super::{Held, Identity, conflict};
    use crate::io::IoError;

    /// How often a held ticket is got again: librdkafka's own `sasl.kerberos.min.time.before.relogin` default, so the
    /// KDC sees what it saw when librdkafka ran `kinit`.
    const RENEW_EVERY: Duration = Duration::from_secs(60);

    type Context = *mut c_void;
    type Principal = *mut c_void;
    type Keytab = *mut c_void;
    type Cache = *mut c_void;
    type ErrorCode = c_long;

    /// Room for MIT's `krb5_creds`, which the caller allocates and krb5 fills. Its fields are never read here, so
    /// only its size matters, and this is twice it on every 64-bit target.
    #[repr(C, align(8))]
    struct Creds([u8; 256]);

    // MIT krb5, linked statically for Cyrus SASL's GSSAPI (`sasl2-sys`'s `gssapi-vendored`).
    unsafe extern "C" {
        fn krb5_init_context(context: *mut Context) -> ErrorCode;
        fn krb5_free_context(context: Context);
        fn krb5_parse_name(context: Context, name: *const c_char, principal: *mut Principal) -> ErrorCode;
        fn krb5_free_principal(context: Context, principal: Principal);
        fn krb5_kt_resolve(context: Context, name: *const c_char, keytab: *mut Keytab) -> ErrorCode;
        fn krb5_kt_close(context: Context, keytab: Keytab) -> ErrorCode;
        fn krb5_get_init_creds_keytab(
            context: Context,
            creds: *mut Creds,
            client: Principal,
            keytab: Keytab,
            start_time: c_int,
            in_tkt_service: *const c_char,
            options: *mut c_void,
        ) -> ErrorCode;
        fn krb5_free_cred_contents(context: Context, creds: *mut Creds);
        fn krb5_cc_default(context: Context, cache: *mut Cache) -> ErrorCode;
        fn krb5_cc_initialize(context: Context, cache: Cache, principal: Principal) -> ErrorCode;
        fn krb5_cc_store_cred(context: Context, cache: Cache, creds: *mut Creds) -> ErrorCode;
        fn krb5_cc_close(context: Context, cache: Cache) -> ErrorCode;
        fn krb5_get_error_message(context: Context, code: ErrorCode) -> *const c_char;
        fn krb5_free_error_message(context: Context, message: *const c_char);
    }

    /// The identity held, while any client holds it.
    static HELD: Mutex<Weak<Identity>> = Mutex::new(Weak::new());

    pub(super) fn hold(identity: Identity) -> Result<Option<Held>, IoError> {
        let mut held = HELD.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(current) = held.upgrade() {
            if let Some(why) = conflict(&current, &identity) {
                return Err(IoError::transient(why));
            }
            // The same principal's ticket is already held and renewed, whichever keytab it came from.
            return Ok(Some(Held { _identity: current }));
        }
        // Got before the client exists, so its first sign-in finds a ticket, as librdkafka's first `kinit` did.
        acquire(&identity).map_err(IoError::transient)?;
        let identity = Arc::new(identity);
        *held = Arc::downgrade(&identity);
        static RENEWER: Once = Once::new();
        RENEWER.call_once(|| {
            std::thread::Builder::new()
                .name("smeltr-kerberos".into())
                .spawn(renew)
                .expect("the Kerberos renewal thread starts");
        });
        Ok(Some(Held { _identity: identity }))
    }

    /// Gets the held identity's ticket again every [`RENEW_EVERY`], while a client holds it.
    fn renew() {
        loop {
            std::thread::sleep(RENEW_EVERY);
            let Some(identity) = HELD.lock().unwrap_or_else(|p| p.into_inner()).upgrade() else {
                continue;
            };
            if let Err(why) = acquire(&identity) {
                tracing::warn!(name: "kerberos_renewal_failed", target: "sources", principal = %identity.principal, error = %why, "a Kerberos ticket for Kafka could not be renewed; clients keep the one they hold until it expires");
            }
        }
    }

    /// `kinit -t <keytab> -k <principal>`: a ticket for `identity` from its keytab, written to the default cache.
    fn acquire(identity: &Identity) -> Result<(), String> {
        let principal = CString::new(identity.principal.as_str()).map_err(|_| "the principal holds a NUL byte")?;
        let keytab = CString::new(identity.keytab.as_str()).map_err(|_| "the keytab path holds a NUL byte")?;
        let mut context: Context = std::ptr::null_mut();
        // SAFETY: krb5 writes the context it creates; it is freed below on every path.
        let code = unsafe { krb5_init_context(&mut context) };
        if code != 0 {
            return Err(format!("Kerberos could not start (krb5 error {code})"));
        }
        let result = with_context(context, &principal, &keytab, identity);
        // SAFETY: the context made above, no longer used.
        unsafe { krb5_free_context(context) };
        result
    }

    fn with_context(context: Context, principal: &CStr, keytab: &CStr, identity: &Identity) -> Result<(), String> {
        let failed = |what: &str, code: ErrorCode| format!("{what}: {}", message(context, code));
        let mut client: Principal = std::ptr::null_mut();
        // SAFETY: a live context and a NUL-terminated name; krb5 writes the principal, freed below.
        let code = unsafe { krb5_parse_name(context, principal.as_ptr(), &mut client) };
        if code != 0 {
            return Err(failed(&format!("Kerberos principal {:?}", identity.principal), code));
        }
        let mut table: Keytab = std::ptr::null_mut();
        // SAFETY: as above; the keytab handle is closed below.
        let code = unsafe { krb5_kt_resolve(context, keytab.as_ptr(), &mut table) };
        let result = if code != 0 {
            Err(failed(&format!("keytab {:?}", identity.keytab), code))
        } else {
            let result = store(context, client, table, identity, &failed);
            // SAFETY: the handle resolved above.
            unsafe { krb5_kt_close(context, table) };
            result
        };
        // SAFETY: the principal parsed above.
        unsafe { krb5_free_principal(context, client) };
        result
    }

    fn store(
        context: Context,
        client: Principal,
        table: Keytab,
        identity: &Identity,
        failed: &dyn Fn(&str, ErrorCode) -> String,
    ) -> Result<(), String> {
        let mut creds = Creds([0; 256]);
        // SAFETY: live handles; krb5 fills `creds`, whose contents are freed below once got.
        let code = unsafe {
            krb5_get_init_creds_keytab(
                context,
                &mut creds,
                client,
                table,
                0,
                std::ptr::null(),
                std::ptr::null_mut(),
            )
        };
        if code != 0 {
            return Err(failed(
                &format!(
                    "a Kerberos ticket for {:?} from keytab {:?}",
                    identity.principal, identity.keytab
                ),
                code,
            ));
        }
        let mut cache: Cache = std::ptr::null_mut();
        // SAFETY: a live context; krb5 writes the cache handle, closed below.
        let mut code = unsafe { krb5_cc_default(context, &mut cache) };
        if code == 0 {
            // Between these two calls the cache is empty for a moment, and a sign-in then fails and is retried, as
            // under `kinit`. The krb5 linked here (1.19) has no one-step replace for a memory cache.
            // SAFETY: the cache, principal and credentials above, all live.
            code = unsafe { krb5_cc_initialize(context, cache, client) };
            if code == 0 {
                code = unsafe { krb5_cc_store_cred(context, cache, &mut creds) };
            }
            // SAFETY: the cache opened above.
            unsafe { krb5_cc_close(context, cache) };
        }
        // SAFETY: the credentials krb5 filled above.
        unsafe { krb5_free_cred_contents(context, &mut creds) };
        if code != 0 {
            return Err(failed("the default Kerberos credential cache", code));
        }
        Ok(())
    }

    /// krb5's own words for `code`.
    fn message(context: Context, code: ErrorCode) -> String {
        // SAFETY: krb5 returns a NUL-terminated string it owns until it is freed here.
        unsafe {
            let text = krb5_get_error_message(context, code);
            if text.is_null() {
                return format!("krb5 error {code}");
            }
            let owned = CStr::from_ptr(text).to_string_lossy().into_owned();
            krb5_free_error_message(context, text);
            owned
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{Held, Identity};
    use crate::io::IoError;

    /// SSPI signs in as the account smeltr runs under, from no keytab.
    pub(super) fn hold(_identity: Identity) -> Result<Option<Held>, IoError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(pairs: &[(&str, &str)]) -> ClientConfig {
        let mut config = ClientConfig::new();
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        config
    }

    #[test]
    fn only_a_gssapi_client_with_a_keytab_has_an_identity_to_hold() {
        let gssapi = config(&[
            ("sasl.mechanism", "GSSAPI"),
            ("sasl.kerberos.keytab", "/k"),
            ("sasl.kerberos.principal", "me"),
        ]);
        assert!(selected(&gssapi));
        assert_eq!(
            identity(&gssapi),
            Some(Identity {
                principal: "me".into(),
                keytab: "/k".into()
            })
        );
        let unnamed = config(&[("sasl.mechanisms", "gssapi"), ("sasl.kerberos.keytab", "/k")]);
        assert!(selected(&unnamed));
        assert_eq!(identity(&unnamed).unwrap().principal, DEFAULT_PRINCIPAL);
        assert_eq!(identity(&config(&[("sasl.mechanism", "GSSAPI")])), None);
        assert!(selected(&config(&[("security.protocol", "sasl_ssl")])));
        assert!(!selected(&config(&[("security.protocol", "SSL")])));
        assert!(!selected(&config(&[
            ("sasl.mechanism", "PLAIN"),
            ("sasl.kerberos.keytab", "/k")
        ])));
        assert!(hold(&config(&[("sasl.mechanism", "PLAIN")])).unwrap().is_none());
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn a_second_principal_is_refused_and_the_same_one_with_another_keytab_is_not() {
        let held = Identity {
            principal: "a".into(),
            keytab: "/a".into(),
        };
        let other = Identity {
            principal: "b".into(),
            keytab: "/a".into(),
        };
        let why = conflict(&held, &other).unwrap();
        assert!(
            why.contains("\"a\"") && why.contains("\"b\"") && why.contains("one Kerberos identity"),
            "{why}"
        );
        let rekeyed = Identity {
            principal: "a".into(),
            keytab: "/b".into(),
        };
        assert_eq!(conflict(&held, &rekeyed), None);
    }
}
