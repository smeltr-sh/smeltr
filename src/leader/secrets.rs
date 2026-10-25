//! The secrets encryption key, which seals a leader-stored secret's value, and the sealing itself.
//!
//! One key, generated when a leader starts with nothing sealed and kept in its own file outside the leader's data
//! directory: a backup of that directory must not carry the key that opens it. A value is sealed under the key with
//! the secret's name as associated data, so a ciphertext cannot be moved to another name.
//!
//! This protects a stolen file, a backup, a discarded disk. It does not protect a running leader, which holds the
//! key in order to serve workers.

use std::fs;
use std::path::Path;

use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use aws_lc_rs::hmac;
use aws_lc_rs::rand::SecureRandom;

use super::auth;
use super::private_file;

#[derive(Debug)]
pub struct KeyError(pub String);

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for KeyError {}

/// A sealed value, as stored. `key_id` names the key that sealed it: a row carrying any other is unusable, which is
/// the same fact as a missing one and travels the same path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sealed {
    key_id: String,
    nonce: [u8; NONCE_LEN],
    ciphertext: Vec<u8>,
}

impl Sealed {
    /// One read back from storage, or `None` when the nonce is not the length a sealed value carries -- the only
    /// way the three columns can disagree with each other.
    pub fn from_parts(key_id: String, nonce: &[u8], ciphertext: Vec<u8>) -> Option<Sealed> {
        Some(Sealed {
            key_id,
            nonce: nonce.try_into().ok()?,
            ciphertext,
        })
    }

    /// Which key sealed it. Comparing this to the leader's own is how a row is called unusable without opening it.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn nonce(&self) -> &[u8] {
        &self.nonce
    }

    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    /// One text line, for a value kept where only text goes: a leader setting.
    pub fn to_text(&self) -> String {
        format!(
            "{}:{}:{}",
            self.key_id,
            auth::hex_encode(&self.nonce),
            auth::hex_encode(&self.ciphertext)
        )
    }

    /// [`Sealed::to_text`] read back, or `None` when `text` is not one.
    pub fn from_text(text: &str) -> Option<Sealed> {
        let mut parts = text.split(':');
        let (key_id, nonce, ciphertext) = (parts.next()?, parts.next()?, parts.next()?);
        Sealed::from_parts(key_id.to_string(), &hex_decode(nonce)?, hex_decode(ciphertext)?)
    }
}

pub struct EncryptionKey {
    key_id: String,
    key: LessSafeKey,
}

impl EncryptionKey {
    /// Names the key without exposing it: a digest of the material, truncated. Stored beside every sealed value.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// `value` sealed under this key, bound to `name`.
    pub fn seal(&self, name: &str, value: &[u8]) -> Sealed {
        let mut nonce = [0u8; NONCE_LEN];
        aws_lc_rs::rand::SystemRandom::new()
            .fill(&mut nonce)
            .expect("the OS random number generator");
        let mut buf = value.to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(name.as_bytes()),
                &mut buf,
            )
            .expect("sealing a value already held in memory");
        Sealed {
            key_id: self.key_id.clone(),
            nonce,
            ciphertext: buf,
        }
    }

    /// The value back, or `None` when `sealed` was not sealed by this key under this name.
    pub fn open(&self, name: &str, sealed: &Sealed) -> Option<Vec<u8>> {
        if sealed.key_id != self.key_id {
            return None;
        }
        let mut buf = sealed.ciphertext.clone();
        let plain = self
            .key
            .open_in_place(
                Nonce::assume_unique_for_key(sealed.nonce),
                Aad::from(name.as_bytes()),
                &mut buf,
            )
            .ok()?;
        Some(plain.to_vec())
    }

    /// Whether `candidate` is the sealed value. The answer an admin gets about a secret; the value itself never
    /// leaves this process.
    pub fn verify(&self, name: &str, sealed: &Sealed, candidate: &str) -> bool {
        let Some(value) = self.open(name, sealed) else {
            return false;
        };
        // Through HMAC tags rather than byte by byte. This is an oracle an admin may ask repeatedly, so the
        // comparison must not take longer the closer a guess gets, nor refuse faster for a candidate of the wrong
        // length. Tags are one length whatever the value's, which hides it; a per-call key means they mean
        // nothing outside this line.
        let mut material = [0u8; 32];
        aws_lc_rs::rand::SystemRandom::new()
            .fill(&mut material)
            .expect("the OS random number generator");
        let key = hmac::Key::new(hmac::HMAC_SHA256, &material);
        hmac::verify(&key, candidate.as_bytes(), hmac::sign(&key, &value).as_ref()).is_ok()
    }
}

/// The key at `path`, or `None` when there is none yet. A leader reads this at startup to refuse a key file anyone
/// but its owner can read, before it has any reason to seal anything.
pub fn load(path: &Path) -> Result<Option<EncryptionKey>, KeyError> {
    let material = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(KeyError(format!("{}: {e}", path.display()))),
    };
    private_file::check(path, "the secrets encryption key").map_err(KeyError)?;
    key_from(material.trim(), path).map(Some)
}

/// The key at `path`, generating one there if it does not exist. A leader writes it at startup while it has sealed
/// nothing, so the key exists before the first value needs it.
pub fn load_or_create(path: &Path) -> Result<EncryptionKey, KeyError> {
    // Two stores at once on a leader with no key would each write one; there is only ever one key.
    static CREATING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _one = CREATING.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(key) = load(path)? {
        return Ok(key);
    }
    create(path)
}

/// Writes a new key readable by its owner alone, in a directory of the same. Says so once, which is the only moment
/// an operator is looking.
fn create(path: &Path) -> Result<EncryptionKey, KeyError> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        private_file::create_dir(parent).map_err(|e| KeyError(format!("{}: {e}", parent.display())))?;
    }
    let material = auth::generate_secret();
    // Renamed into place whole: every store reads the key from disk, and one reading it half written would fail.
    private_file::replace(path, material.as_bytes()).map_err(|e| KeyError(format!("{}: {e}", path.display())))?;
    tracing::info!(
        target: "secrets.key_created",
        path = %path.display(),
        "wrote a new secrets encryption key. Back it up: without it, a restored leader cannot open the stored secrets."
    );
    key_from(&material, path)
}

fn key_from(material: &str, path: &Path) -> Result<EncryptionKey, KeyError> {
    let bytes = hex_decode(material).ok_or_else(|| {
        KeyError(format!(
            "{}: not a secrets encryption key -- expected {} hex characters",
            path.display(),
            AES_256_GCM.key_len() * 2
        ))
    })?;
    let key = UnboundKey::new(&AES_256_GCM, &bytes).map_err(|_| {
        KeyError(format!(
            "{}: not a secrets encryption key -- it holds {} bytes, not {}",
            path.display(),
            bytes.len(),
            AES_256_GCM.key_len()
        ))
    })?;
    Ok(EncryptionKey {
        key_id: auth::hash_secret(material)[..KEY_ID_LEN].to_string(),
        key: LessSafeKey::new(key),
    })
}

/// Enough of the digest to tell two keys apart in a row and a log line.
const KEY_ID_LEN: usize = 16;

pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> EncryptionKey {
        key_from(&auth::generate_secret(), Path::new("<test>")).expect("a freshly generated key")
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("smeltr-secrets-{}-{tag}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir.join("sealing.key")
    }

    #[test]
    fn a_sealed_value_opens_under_its_own_name() {
        let key = key();
        let sealed = key.seal("kafka-west-sasl", b"hunter2");
        assert_eq!(key.open("kafka-west-sasl", &sealed).as_deref(), Some(&b"hunter2"[..]));
    }

    /// The name is associated data, so a row's ciphertext cannot be copied into another row and read there.
    #[test]
    fn a_ciphertext_does_not_open_under_another_name() {
        let key = key();
        let sealed = key.seal("kafka-west-sasl", b"hunter2");
        assert_eq!(key.open("kafka-east-sasl", &sealed), None);
    }

    #[test]
    fn another_key_does_not_open_it() {
        let sealed = key().seal("kafka-west-sasl", b"hunter2");
        assert_eq!(key().open("kafka-west-sasl", &sealed), None);
    }

    /// Not the same test: a foreign key's tag would fail anyway. This pins the `key_id` gate, which is what lets a
    /// row be called unusable without attempting to open it.
    #[test]
    fn a_row_sealed_under_another_key_id_is_refused_before_it_is_opened() {
        let key = key();
        let mut sealed = key.seal("kafka-west-sasl", b"hunter2");
        sealed.key_id = "0123456789abcdef".to_string();
        assert_ne!(sealed.key_id, key.key_id());
        assert_eq!(key.open("kafka-west-sasl", &sealed), None);
    }

    #[test]
    fn a_tampered_ciphertext_does_not_open() {
        let key = key();
        let mut sealed = key.seal("kafka-west-sasl", b"hunter2");
        sealed.ciphertext[0] ^= 1;
        assert_eq!(key.open("kafka-west-sasl", &sealed), None);
    }

    /// A fresh nonce per sealing: the same value stored twice must not be recognizable as the same value.
    #[test]
    fn sealing_one_value_twice_gives_two_ciphertexts() {
        let key = key();
        assert_ne!(
            key.seal("kafka-west-sasl", b"hunter2").ciphertext,
            key.seal("kafka-west-sasl", b"hunter2").ciphertext
        );
    }

    #[test]
    fn verify_answers_for_the_value_and_for_nothing_else() {
        let key = key();
        let sealed = key.seal("kafka-west-sasl", b"hunter2");
        assert!(key.verify("kafka-west-sasl", &sealed, "hunter2"));
        assert!(!key.verify("kafka-west-sasl", &sealed, "hunter3"));
        assert!(!key.verify("kafka-west-sasl", &sealed, "hunter2 "));
        assert!(!key.verify("kafka-west-sasl", &sealed, ""));
        assert!(!key.verify("kafka-east-sasl", &sealed, "hunter2"));
    }

    #[test]
    fn nothing_is_written_until_a_value_needs_sealing() {
        let path = temp_path("absent");
        assert!(load(&path).expect("an absent key is not an error").is_none());
        assert!(!path.exists());
    }

    /// Stores racing on a leader with no key yet all get the one key, rather than each writing its own.
    #[test]
    fn stores_racing_on_a_leader_with_no_key_share_one() {
        let path = temp_path("raced");
        let ids: std::collections::BTreeSet<String> = std::thread::scope(|s| {
            let racers: Vec<_> = (0..8)
                .map(|_| s.spawn(|| load_or_create(&path).expect("a key").key_id().to_string()))
                .collect();
            racers.into_iter().map(|r| r.join().unwrap()).collect()
        });
        assert_eq!(ids.len(), 1, "{ids:?}");
        assert_eq!(load(&path).unwrap().unwrap().key_id(), ids.first().unwrap().as_str());
    }

    #[test]
    fn the_key_written_on_first_use_is_the_one_read_back() {
        let path = temp_path("first-use");
        let first = load_or_create(&path).expect("write a key");
        let again = load_or_create(&path).expect("read it back");
        assert_eq!(first.key_id(), again.key_id());

        let sealed = first.seal("kafka-west-sasl", b"hunter2");
        assert_eq!(again.open("kafka-west-sasl", &sealed).as_deref(), Some(&b"hunter2"[..]));
    }

    #[test]
    fn a_file_that_is_not_a_key_refuses() {
        let path = temp_path("not-a-key");
        private_file::create_dir(path.parent().expect("a parent")).expect("create the directory");
        fs::write(&path, "not hex").expect("write the file");
        assert!(load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_key_file_anyone_can_read_refuses() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_path("world-readable");
        load_or_create(&path).expect("write a key");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("loosen it");
        assert!(load(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_new_key_and_its_directory_belong_to_their_owner_alone() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_path("modes");
        load_or_create(&path).expect("write a key");
        let mode = |p: &Path| fs::metadata(p).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode(&path), 0o600);
        assert_eq!(mode(path.parent().expect("a parent")), 0o700);
    }

    #[test]
    fn hex_reads_a_key_and_nothing_else() {
        assert_eq!(hex_decode("00ff10"), Some(vec![0, 255, 16]));
        assert_eq!(hex_decode(""), Some(vec![]));
        assert_eq!(hex_decode("abc"), None);
        assert_eq!(hex_decode("zz"), None);
        assert_eq!(hex_decode("ké"), None);
    }
}
