//! One directory of an SFTP server as an [`ObjectStore`], so the listing core reads it as it reads a bucket.
//!
//! A key `a/b.log` is the file `{directory}/a/b.log`. A file's ETag is its size and modification time, which is all
//! SFTP knows of a version: a rewrite within the same second to the same size keeps it.
//!
//! A name that is not UTF-8 reaches the store with each byte that is not replaced by U+FFFD, so that file cannot be
//! read by its key.
//!
//! One SSH connection carries every call. It opens on first use, and a call that fails for any reason but the
//! server's answer about a file drops it, so the next call opens a new one.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    RenameTargetMode,
};
use russh::client;
use russh::keys::{Algorithm, PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use super::IoError;
use crate::plan::topology::{HeldSecrets, SftpCredential, SftpStore};

const STORE: &str = "SFTP";

/// How much one read of a file asks the server for.
const READ_CHUNK: usize = 256 * 1024;

/// How often an idle connection asks the server whether it is there.
const KEEPALIVE: Duration = Duration::from_secs(15);

/// How long connecting and signing in may take. Every call waits on the one connecting.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// A directory on an SFTP server, read and written as an object store.
#[derive(Clone)]
pub struct Sftp {
    inner: Arc<Inner>,
}

struct Inner {
    /// The directory as the place key names it.
    address: String,
    host: String,
    port: u16,
    username: String,
    credential: Credential,
    host_key: PublicKey,
    directory: String,
    /// A listing descends only into directories whose keys this could start with.
    names: String,
    /// A listing skips a directory below its top that this user may not read, rather than fail.
    skip_unreadable: bool,
    connection: tokio::sync::Mutex<Option<Arc<Connection>>>,
}

enum Credential {
    Password(String),
    Key(Arc<PrivateKey>),
}

/// An authenticated SSH connection and the SFTP session on it. The SSH handle is held so the connection lives as
/// long as anything still reads through the session.
struct Connection {
    ssh: client::Handle<Pinned>,
    sftp: SftpSession,
}

/// Accepts only the server whose key was pinned.
struct Pinned(PublicKey);

impl client::Handler for Pinned {
    type Error = russh::Error;

    async fn check_server_key(&mut self, offered: &PublicKeyOrCertificate) -> Result<bool, Self::Error> {
        Ok(matches!(offered, PublicKeyOrCertificate::PublicKey { key, .. } if key.key_data() == self.0.key_data()))
    }
}

impl Sftp {
    /// Resolves the credential from `secrets` and parses the pinned host key. Connects on first use, not here. A
    /// listing lists every file whose key starts with `names`, and may skip the others. With `skip_unreadable`, it
    /// skips a directory below its top that this user may not read, with a warning; otherwise that fails it, so a
    /// ledger never takes the files there for gone.
    pub fn new(store: &SftpStore, names: &str, skip_unreadable: bool, secrets: &HeldSecrets) -> Result<Sftp, IoError> {
        let access = &store.access;
        let credential = match &access.credential {
            SftpCredential::Password(r) => Credential::Password(super::held(secrets, r, "the sftp store", "password")?),
            SftpCredential::PrivateKey(r) => {
                let pem = super::held(secrets, r, "the sftp store", "private key")?;
                let key = russh::keys::decode_secret_key(&pem, None).map_err(|e| {
                    IoError::transient(format!("sftp: the private key from {} is not one: {e}", r.describe()))
                })?;
                Credential::Key(Arc::new(key))
            }
        };
        Ok(Sftp {
            inner: Arc::new(Inner {
                address: store.address(),
                host: access.host.clone(),
                port: access.port,
                username: access.username.clone(),
                credential,
                host_key: host_key(&access.host_key)?,
                directory: store.directory.clone(),
                names: names.to_string(),
                skip_unreadable,
                connection: tokio::sync::Mutex::new(None),
            }),
        })
    }
}

/// The server's public key from one line as OpenSSH writes it: algorithm, base64 key, and an optional comment.
fn host_key(line: &str) -> Result<PublicKey, IoError> {
    PublicKey::from_openssh(line.trim()).map_err(|e| {
        IoError::transient(format!(
            "sftp: the host key {:?} is not an OpenSSH public key: {e}",
            line.trim()
        ))
    })
}

/// The host key algorithms to ask the server for: only the pinned key's, so a server holding keys of several kinds
/// shows the one pinned.
fn host_key_algorithms(pinned: &PublicKey) -> Vec<Algorithm> {
    russh::Preferred::DEFAULT
        .key
        .iter()
        .filter(|a| match (a, pinned.algorithm()) {
            // The key is the same whichever hash a signature with it uses.
            (Algorithm::Rsa { .. }, Algorithm::Rsa { .. }) => true,
            (a, p) => **a == p,
        })
        .cloned()
        .collect()
}

/// The remote path of `key` under `directory`. An empty directory is the login directory, so the path is relative.
fn remote(directory: &str, key: &str) -> String {
    match (directory, key) {
        ("", "") => ".".to_string(),
        ("", k) => k.to_string(),
        (d, "") => d.to_string(),
        (d, k) if d.ends_with('/') => format!("{d}{k}"),
        (d, k) => format!("{d}/{k}"),
    }
}

/// The ETag of a file, from what a stat tells.
fn e_tag(size: u64, mtime: u32) -> String {
    format!("{size}-{mtime}")
}

fn meta(location: Path, attrs: &FileAttributes) -> ObjectMeta {
    let size = attrs.size.unwrap_or(0);
    let mtime = attrs.mtime.unwrap_or(0);
    ObjectMeta {
        location,
        last_modified: (SystemTime::UNIX_EPOCH + Duration::from_secs(mtime.into())).into(),
        size,
        e_tag: Some(e_tag(size, mtime)),
        version: None,
    }
}

fn is_missing(e: &SftpError) -> bool {
    matches!(e, SftpError::Status(s) if s.status_code == StatusCode::NoSuchFile)
}

/// Whether the connection, rather than the server's answer about one file, failed.
fn connection_lost(e: &SftpError) -> bool {
    !matches!(e, SftpError::Status(_) | SftpError::Limited(_))
}

/// The SFTP error inside an I/O error from reading or writing a file, which the session wraps that way.
fn from_io(e: std::io::Error) -> SftpError {
    match e.get_ref().and_then(|inner| inner.downcast_ref::<SftpError>()) {
        Some(inner) => inner.clone(),
        None => SftpError::from(e),
    }
}

fn generic(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: source.into(),
    }
}

/// `e` as object_store states it, about the file at `remote`.
fn failure(remote: &str, e: SftpError) -> object_store::Error {
    if is_missing(&e) {
        object_store::Error::NotFound {
            path: remote.to_string(),
            source: Box::new(e),
        }
    } else {
        generic(format!("{remote}: {e}"))
    }
}

impl Inner {
    fn remote(&self, location: &Path) -> String {
        remote(&self.directory, location.as_ref())
    }

    /// The open connection, or a new one when there is none or it has closed.
    async fn connection(&self) -> object_store::Result<Arc<Connection>> {
        let mut slot = self.connection.lock().await;
        if let Some(c) = slot.as_ref()
            && !c.ssh.is_closed()
        {
            return Ok(c.clone());
        }
        *slot = None;
        let connected = tokio::time::timeout(CONNECT_TIMEOUT, self.connect())
            .await
            .unwrap_or_else(|_| Err(format!("no connection within {CONNECT_TIMEOUT:?}")));
        let c = Arc::new(connected.map_err(|why| generic(format!("{self}: {why}")))?);
        *slot = Some(c.clone());
        Ok(c)
    }

    async fn connect(&self) -> Result<Connection, String> {
        // A connection that stops answering closes, so the next call connects again rather than wait on it.
        let config = Arc::new(client::Config {
            keepalive_interval: Some(KEEPALIVE),
            keepalive_max: 3,
            preferred: russh::Preferred {
                key: host_key_algorithms(&self.host_key).into(),
                ..russh::Preferred::DEFAULT
            },
            ..client::Config::default()
        });
        let pinned = Pinned(self.host_key.clone());
        let mut ssh = client::connect(config, (self.host.as_str(), self.port), pinned)
            .await
            .map_err(|e| match e {
                russh::Error::UnknownKey => "the server's host key is not the one configured".to_string(),
                e => e.to_string(),
            })?;
        let (signed_in, what) = match &self.credential {
            Credential::Password(password) => (ssh.authenticate_password(&self.username, password).await, "password"),
            Credential::Key(key) => {
                let hash = ssh
                    .best_supported_rsa_hash()
                    .await
                    .map_err(|e| e.to_string())?
                    .flatten();
                let signed_in = ssh
                    .authenticate_publickey(&self.username, PrivateKeyWithHashAlg::new(key.clone(), hash))
                    .await;
                (signed_in, "private key")
            }
        };
        if !signed_in.map_err(|e| e.to_string())?.success() {
            return Err(format!(
                "the server refused the {what} given for user {}",
                self.username
            ));
        }
        let channel = ssh.channel_open_session().await.map_err(|e| e.to_string())?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| e.to_string())?;
        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| format!("starting SFTP: {e}"))?;
        Ok(Connection { ssh, sftp })
    }

    /// Runs `op` on the connection, dropping that connection when `op` fails for anything but the server's answer
    /// about a file.
    async fn run<T>(
        &self,
        op: impl AsyncFnOnce(&Arc<Connection>) -> Result<T, SftpError>,
    ) -> object_store::Result<Result<T, SftpError>> {
        let c = self.connection().await?;
        let result = op(&c).await;
        if let Err(e) = &result
            && connection_lost(e)
        {
            let mut slot = self.connection.lock().await;
            if slot.as_ref().is_some_and(|s| Arc::ptr_eq(s, &c)) {
                *slot = None;
            }
        }
        Ok(result)
    }

    /// One directory's entries: regular files as objects, and directories by name. A directory missing, or a file
    /// in its place, has none. So does one below the top of a walk that this user may not read, when the store skips
    /// those.
    async fn read_dir(&self, key: &str, top: bool) -> object_store::Result<(Vec<ObjectMeta>, Vec<String>)> {
        let dir = remote(&self.directory, key);
        let entries = self
            .run(async |c| match c.sftp.read_dir(dir.clone()).await {
                Ok(entries) => Ok(Some(entries)),
                Err(e) if is_missing(&e) => Ok(None),
                Err(SftpError::Status(s))
                    if !top && self.skip_unreadable && s.status_code == StatusCode::PermissionDenied =>
                {
                    tracing::warn!(target: "sftp.unreadable_directory", directory = %dir, "skipping a directory this user may not read");
                    Ok(None)
                }
                Err(e @ SftpError::Status(_)) => match c.sftp.metadata(dir.clone()).await {
                    Ok(attrs) if !attrs.file_type().is_dir() => Ok(None),
                    _ => Err(e),
                },
                Err(e) => Err(e),
            })
            .await?
            .map_err(|e| failure(&dir, e))?;
        let (mut files, mut dirs) = (Vec::new(), Vec::new());
        for entry in entries.into_iter().flatten() {
            let name = entry.file_name();
            // A name holding a slash would read as a file in a directory that is not there.
            if name.contains('/') || is_temporary(&name) {
                continue;
            }
            let child = if key.is_empty() { name } else { format!("{key}/{name}") };
            // A listing describes a link, not what it points to, so a link is skipped. A server may leave out what
            // kind of entry each is, which a stat of the entry itself then tells.
            let mut attrs = entry.metadata();
            if attrs.permissions.is_none() {
                let path = remote(&self.directory, &child);
                attrs = match self.run(async |c| c.sftp.symlink_metadata(path.clone()).await).await? {
                    Ok(attrs) => attrs,
                    Err(e) if is_missing(&e) => continue,
                    Err(e) => return Err(failure(&path, e)),
                };
            }
            if attrs.file_type().is_dir() {
                if may_hold(&child, &self.names) {
                    dirs.push(child);
                }
            } else if attrs.file_type().is_file() {
                files.push(meta(Path::parse(&child)?, &attrs));
            }
        }
        Ok((files, dirs))
    }

    /// Creates each directory above `location` under the store's directory that is not there yet.
    async fn create_parents(&self, c: &Connection, location: &Path) -> Result<(), SftpError> {
        let parts: Vec<_> = location.parts().collect();
        for depth in 1..parts.len() {
            let key = parts[..depth].iter().map(|p| p.as_ref()).collect::<Vec<_>>().join("/");
            let dir = remote(&self.directory, &key);
            match c.sftp.metadata(dir.clone()).await {
                Ok(_) => continue,
                Err(e) if is_missing(&e) => {}
                Err(e) => return Err(e),
            }
            if let Err(e) = c.sftp.create_dir(dir.clone()).await {
                // Another writer may have made it in between.
                if !c.sftp.metadata(dir).await.is_ok_and(|a| a.file_type().is_dir()) {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Renames `from` over `to`. A server that refuses to rename over an existing file has that file removed first.
    async fn replace(&self, c: &Connection, from: &str, to: &str) -> Result<(), SftpError> {
        match c.sftp.rename(from, to).await {
            Err(SftpError::Status(s)) if s.status_code == StatusCode::Failure && c.sftp.try_exists(to).await? => {
                c.sftp.remove_file(to).await?;
                c.sftp.rename(from, to).await
            }
            other => other,
        }
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let path = self.remote(location);
        match self.run(async |c| c.sftp.remove_file(path.clone()).await).await? {
            Err(e) if is_missing(&e) => Ok(()),
            other => other.map_err(|e| failure(&path, e)),
        }
    }
}

impl fmt::Display for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.address)
    }
}

impl fmt::Display for Sftp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// The address alone: the credential never reaches a log line.
impl fmt::Debug for Sftp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sftp({self})")
    }
}

/// The key of `location` as a prefix to list under, without a trailing slash.
fn prefix_key(prefix: Option<&Path>) -> String {
    prefix.map(|p| p.as_ref().to_string()).unwrap_or_default()
}

#[async_trait::async_trait]
impl ObjectStore for Sftp {
    /// Writes the payload to a temporary file beside `location`, then renames it into place, so no reader sees part
    /// of it.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let inner = &self.inner;
        let path = inner.remote(location);
        let create = match opts.mode {
            PutMode::Overwrite => false,
            PutMode::Create => true,
            PutMode::Update(_) => {
                return Err(object_store::Error::NotImplemented {
                    operation: "put with an update condition".to_string(),
                    implementer: STORE.to_string(),
                });
            }
        };
        let temporary = temporary(&path);
        let written = inner
            .run(async |c| {
                if create && c.sftp.try_exists(path.clone()).await? {
                    return Ok(None);
                }
                inner.create_parents(c, location).await?;
                let mut file = c.sftp.create(temporary.clone()).await?;
                for chunk in payload.iter() {
                    file.write_all(chunk).await.map_err(from_io)?;
                }
                file.shutdown().await.map_err(from_io)?;
                if let Err(e) = inner.replace(c, &temporary, &path).await {
                    let _ = c.sftp.remove_file(temporary.clone()).await;
                    return Err(e);
                }
                c.sftp.metadata(path.clone()).await.map(Some)
            })
            .await?
            .map_err(|e| failure(&path, e))?;
        let Some(attrs) = written else {
            return Err(object_store::Error::AlreadyExists {
                path: path.clone(),
                source: format!("{path} exists").into(),
            });
        };
        Ok(PutResult {
            e_tag: Some(e_tag(attrs.size.unwrap_or(0), attrs.mtime.unwrap_or(0))),
            version: None,
            extensions: Extensions::default(),
        })
    }

    /// Writes each part into a temporary file beside `location`, renamed into place on `complete`, so no reader sees
    /// part of it.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let inner = &self.inner;
        let path = inner.remote(location);
        let temporary = temporary(&path);
        inner
            .run(async |c| {
                inner.create_parents(c, location).await?;
                // Made empty now, so an upload of no parts completes to an empty file.
                let made = c
                    .sftp
                    .create(temporary.clone())
                    .await?
                    .shutdown()
                    .await
                    .map_err(from_io);
                if made.is_err() {
                    // No upload is handed back to abort, so nothing else would remove it.
                    let _ = c.sftp.remove_file(temporary.clone()).await;
                }
                made
            })
            .await?
            .map_err(|e| failure(&path, e))?;
        Ok(Box::new(Upload {
            inner: inner.clone(),
            path,
            temporary,
            written: 0,
        }))
    }

    /// Opens the file and checks the conditions against what the open file is, then streams the range asked for.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let inner = &self.inner;
        let path = inner.remote(location);
        let (file, attrs, c) = inner
            .run(async |c| {
                let file = c.sftp.open(path.clone()).await?;
                let attrs = file.metadata().await?;
                Ok((file, attrs, c.clone()))
            })
            .await?
            .map_err(|e| failure(&path, e))?;
        if attrs.file_type().is_dir() {
            return Err(object_store::Error::NotFound {
                path: path.clone(),
                source: format!("{path} is a directory").into(),
            });
        }
        let meta = meta(location.clone(), &attrs);
        options.check_preconditions(&meta)?;
        let range = match &options.range {
            Some(r) => r.as_range(meta.size).map_err(|e| generic(format!("{path}: {e}")))?,
            None => 0..meta.size,
        };
        let payload = if options.head {
            stream::empty().boxed()
        } else {
            let mut file = file;
            file.seek(std::io::SeekFrom::Start(range.start))
                .await
                .map_err(|e| failure(&path, from_io(e)))?;
            let what = path.clone();
            ReaderStream::with_capacity(file.take(range.end - range.start), READ_CHUNK)
                .map(move |read| {
                    // The stream holds the connection, which the file's session needs open until it is read.
                    let _held = &c;
                    read.map_err(|e| generic(format!("{what}: {}", from_io(e))))
                })
                .boxed()
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    /// Removes each file. One already gone is removed.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        locations
            .then(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    /// Every regular file under `prefix`, one directory read at a time. Links are skipped, and a prefix that is no
    /// directory lists nothing.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let walk = Walk {
            inner: self.inner.clone(),
            directories: vec![prefix_key(prefix)],
            top: true,
            found: Vec::new(),
        };
        stream::unfold(Some(walk), |walk| async move {
            let mut walk = walk?;
            loop {
                if let Some(meta) = walk.found.pop() {
                    return Some((Ok(meta), Some(walk)));
                }
                let key = walk.directories.pop()?;
                let top = std::mem::replace(&mut walk.top, false);
                match walk.inner.read_dir(&key, top).await {
                    Ok((files, dirs)) => {
                        walk.found = files;
                        walk.directories.extend(dirs);
                    }
                    Err(e) => return Some((Err(e), None)),
                }
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let (objects, dirs) = self.inner.read_dir(&prefix_key(prefix), true).await?;
        Ok(ListResult {
            common_prefixes: dirs.iter().map(Path::parse).collect::<Result<_, _>>()?,
            objects,
            extensions: Extensions::default(),
        })
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "SFTP has no copy on the server; rename moves a file".into(),
        })
    }

    /// Renames on the server, creating the target's directories first.
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        let inner = &self.inner;
        let (source, target) = (inner.remote(from), inner.remote(to));
        let create = matches!(options.target_mode, RenameTargetMode::Create);
        let renamed = inner
            .run(async |c| {
                if create && c.sftp.try_exists(target.clone()).await? {
                    return Ok(false);
                }
                inner.create_parents(c, to).await?;
                if create {
                    c.sftp.rename(source.clone(), target.clone()).await?;
                } else {
                    inner.replace(c, &source, &target).await?;
                }
                Ok(true)
            })
            .await?
            .map_err(|e| failure(&source, e))?;
        if !renamed {
            return Err(object_store::Error::AlreadyExists {
                path: target.clone(),
                source: format!("{target} exists").into(),
            });
        }
        Ok(())
    }
}

/// A multipart upload into a temporary file, renamed over the target when complete.
struct Upload {
    inner: Arc<Inner>,
    path: String,
    temporary: String,
    /// Where the next part starts: parts are given in order, though they may be written in any.
    written: u64,
}

/// The store's address and the file: the credential never reaches a log line.
impl fmt::Debug for Upload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Upload({}: {})", self.inner, self.path)
    }
}

#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let (inner, temporary) = (self.inner.clone(), self.temporary.clone());
        let offset = self.written;
        self.written += data.content_length() as u64;
        Box::pin(async move {
            inner
                .run(async |c| {
                    // No truncate: another part may already have written past this one.
                    let mut file = c
                        .sftp
                        .open_with_flags(temporary.clone(), OpenFlags::WRITE | OpenFlags::CREATE)
                        .await?;
                    file.seek(std::io::SeekFrom::Start(offset)).await.map_err(from_io)?;
                    for chunk in data.iter() {
                        file.write_all(chunk).await.map_err(from_io)?;
                    }
                    file.shutdown().await.map_err(from_io)
                })
                .await?
                .map_err(|e| failure(&temporary, e))
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let (inner, path, temporary) = (&self.inner, &self.path, &self.temporary);
        let attrs = inner
            .run(async |c| {
                if let Err(e) = inner.replace(c, temporary, path).await {
                    let _ = c.sftp.remove_file(temporary.clone()).await;
                    return Err(e);
                }
                c.sftp.metadata(path.clone()).await
            })
            .await?
            .map_err(|e| failure(path, e))?;
        Ok(PutResult {
            e_tag: Some(e_tag(attrs.size.unwrap_or(0), attrs.mtime.unwrap_or(0))),
            version: None,
            extensions: Extensions::default(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let temporary = &self.temporary;
        match self
            .inner
            .run(async |c| c.sftp.remove_file(temporary.clone()).await)
            .await?
        {
            Err(e) if is_missing(&e) => Ok(()),
            other => other.map_err(|e| failure(temporary, e)),
        }
    }
}

/// What a listing has yet to read: directories by key, and the files of the last one read.
struct Walk {
    inner: Arc<Inner>,
    directories: Vec<String>,
    /// Whether the next directory read is the one the walk started at.
    top: bool,
    found: Vec<ObjectMeta>,
}

/// Whether the directory `key` may hold a key that starts with `names`.
fn may_hold(key: &str, names: &str) -> bool {
    let dir = format!("{key}/");
    names.starts_with(&dir) || dir.starts_with(names)
}

/// Whether `name` is one [`temporary`] gives, for a file not yet in place.
fn is_temporary(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('.').and_then(|n| n.strip_suffix(".tmp")) else {
        return false;
    };
    let Some((_, tag)) = rest.rsplit_once('.') else {
        return false;
    };
    let numbers: Vec<_> = tag.split('-').collect();
    numbers.len() == 3
        && numbers
            .iter()
            .all(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// A name beside `path` no other writer picks, for a file until it is renamed into place.
fn temporary(path: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let (dir, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (format!("{dir}/"), name),
        None => (String::new(), path),
    };
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{dir}.{name}.{}-{n}-{nanos}.tmp", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    use crate::plan::topology::SecretRef;

    const KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEB";
    const RSA: &str = include_str!("../../tests/live/sftp/ssh_host_rsa_key.pub");

    #[test]
    fn a_key_is_a_path_under_the_directory() {
        assert_eq!(remote("/srv/logs", "a/b.log"), "/srv/logs/a/b.log");
        assert_eq!(remote("/srv/logs/", "a/b.log"), "/srv/logs/a/b.log");
        assert_eq!(remote("/", "a/b.log"), "/a/b.log");
        assert_eq!(remote("incoming", "b.log"), "incoming/b.log");
        assert_eq!(remote("", "a/b.log"), "a/b.log");
        assert_eq!(remote("/srv/logs", ""), "/srv/logs");
        assert_eq!(remote("", ""), ".");
    }

    #[test]
    fn a_host_key_is_parsed_with_or_without_a_comment() {
        let bare = host_key(KEY).unwrap();
        let commented = host_key(&format!("  {KEY} root@server\n")).unwrap();
        assert_eq!(bare.key_data(), commented.key_data());
    }

    #[test]
    fn a_host_key_that_is_none_is_refused() {
        assert!(host_key("ssh-ed25519 notbase64!").is_err());
        assert!(host_key("").is_err());
        assert!(host_key("AAAAC3NzaC1lZDI1NTE5").is_err());
    }

    #[test]
    fn only_the_pinned_key_is_accepted() {
        let pinned = host_key(KEY).unwrap();
        let other =
            host_key("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIC other").unwrap();
        let accepts = |offered: PublicKey| {
            let mut handler = Pinned(pinned.clone());
            let offered = PublicKeyOrCertificate::PublicKey {
                key: offered,
                hash_alg: None,
            };
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(client::Handler::check_server_key(&mut handler, &offered))
                .unwrap()
        };
        assert!(accepts(pinned.clone()));
        assert!(!accepts(other));
    }

    #[test]
    fn the_e_tag_is_size_and_modification_time() {
        let attrs = FileAttributes {
            size: Some(1234),
            mtime: Some(1_700_000_000),
            ..FileAttributes::empty()
        };
        let meta = meta(Path::from("a/b.log"), &attrs);
        assert_eq!(meta.e_tag.as_deref(), Some("1234-1700000000"));
        assert_eq!(meta.size, 1234);
        assert_eq!(meta.last_modified.timestamp(), 1_700_000_000);
        assert_eq!(meta.version, None);
    }

    #[test]
    fn a_temporary_file_sits_beside_its_target() {
        let t = temporary("/srv/logs/a/b.log");
        assert!(t.starts_with("/srv/logs/a/.b.log."), "{t}");
        assert!(t.ends_with(".tmp"));
        assert_ne!(t, temporary("/srv/logs/a/b.log"));
        assert!(temporary("b.log").starts_with(".b.log."));
    }

    #[test]
    fn a_listing_skips_only_the_temporary_files_it_writes() {
        let name = |path: &str| temporary(path).rsplit('/').next().unwrap().to_string();
        assert!(is_temporary(&name("/srv/logs/a/b.log")));
        assert!(is_temporary(&name("b")));
        assert!(!is_temporary(".b.log.tmp"));
        assert!(!is_temporary(".b.1-2.tmp"));
        assert!(!is_temporary("b.log.1-2-3.tmp"));
        assert!(!is_temporary(".b.x-2-3.tmp"));
    }

    #[test]
    fn a_listing_descends_only_where_the_names_may_be() {
        assert!(may_hold("logs", ""));
        assert!(may_hold("logs", "logs/app-"));
        assert!(may_hold("logs", "lo"));
        assert!(may_hold("logs/app", "logs/"));
        assert!(may_hold("logs/app-1", "logs/app-"));
        assert!(!may_hold("archive", "logs/app-"));
        assert!(!may_hold("logs/other", "logs/app-"));
        assert!(!may_hold("logsx", "logs/"));
    }

    #[test]
    fn only_the_pinned_key_s_algorithm_is_asked_for() {
        assert_eq!(host_key_algorithms(&host_key(KEY).unwrap()), vec![Algorithm::Ed25519]);
        let rsa = host_key_algorithms(&host_key(RSA).unwrap());
        assert!(
            !rsa.is_empty() && rsa.iter().all(|a| matches!(a, Algorithm::Rsa { .. })),
            "{rsa:?}"
        );
    }

    /// The live server's `upload` directory as a store, or `None` when `SFTP_TEST_HOST` (as `host:port`) is not set
    /// and `SMELTR_LIVE_TESTS` is not `required`. `just live-up` starts the server.
    fn live(test: &str) -> Option<Sftp> {
        let address = match std::env::var("SFTP_TEST_HOST") {
            Ok(a) if !a.is_empty() => a,
            _ if std::env::var("SMELTR_LIVE_TESTS").is_ok_and(|v| v == "required") => {
                panic!("{test} needs an SFTP server and SFTP_TEST_HOST is not set")
            }
            _ => {
                eprintln!("skipping {test}: set SFTP_TEST_HOST to run it against an SFTP server (`just live-up`)");
                return None;
            }
        };
        let (host, port) = address.rsplit_once(':').expect("SFTP_TEST_HOST is host:port");
        let password = SecretRef::LeaderStored {
            name: "sftp-password".to_string(),
            version: 1,
        };
        let store = SftpStore {
            directory: "upload".to_string(),
            access: crate::plan::topology::SftpAccess {
                host: host.to_string(),
                port: port.parse().expect("SFTP_TEST_HOST's port"),
                username: "smeltr".to_string(),
                credential: SftpCredential::Password(password),
                host_key: include_str!("../../tests/live/sftp/ssh_host_ed25519_key.pub").to_string(),
            },
        };
        let mut secrets = HeldSecrets::default();
        secrets.set_stored([("sftp-password".to_string(), 1, "smeltr-live-1".to_string())]);
        Some(Sftp::new(&store, "", false, &secrets).unwrap())
    }

    /// A directory of its own for one test, under the store's.
    fn unique(test: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("multipart-{test}-{nanos}")
    }

    #[tokio::test]
    async fn live_parts_finished_out_of_order_are_read_back_whole() {
        let Some(store) = live("live_parts_finished_out_of_order_are_read_back_whole") else {
            return;
        };
        let dir = unique("parts");
        let location = Path::from(format!("{dir}/a/b.bin"));
        let parts = [vec![b'a'; 300_000], vec![b'b'; 5], vec![b'c'; 70_000]];
        let mut upload = store.put_multipart(&location).await.unwrap();
        let pending: Vec<_> = parts.iter().map(|p| upload.put_part(p.clone().into())).collect();
        for part in pending.into_iter().rev() {
            part.await.unwrap();
        }
        let put = upload.complete().await.unwrap();

        let read = store.get(&location).await.unwrap();
        assert_eq!(read.meta.e_tag, put.e_tag);
        assert_eq!(read.bytes().await.unwrap().to_vec(), parts.concat());
        let listed: Vec<_> = store
            .list(Some(&Path::from(dir)))
            .map(|m| m.unwrap().location)
            .collect()
            .await;
        assert_eq!(listed, vec![location]);
    }

    #[tokio::test]
    async fn live_parts_written_through_write_multipart_are_one_file() {
        let Some(store) = live("live_parts_written_through_write_multipart_are_one_file") else {
            return;
        };
        let location = Path::from(format!("{}/c.bin", unique("writer")));
        let body: Vec<u8> = (0..30_000u32).map(|n| n as u8).collect();
        let mut writer =
            object_store::WriteMultipart::new_with_chunk_size(store.put_multipart(&location).await.unwrap(), 10_000);
        writer.write(&body);
        writer.finish().await.unwrap();
        assert_eq!(
            store.get(&location).await.unwrap().bytes().await.unwrap().to_vec(),
            body
        );
    }

    #[tokio::test]
    async fn live_an_aborted_upload_leaves_nothing() {
        let Some(store) = live("live_an_aborted_upload_leaves_nothing") else {
            return;
        };
        let dir = unique("abort");
        let location = Path::from(format!("{dir}/d.bin"));
        let mut upload = store.put_multipart(&location).await.unwrap();
        upload.put_part(vec![b'x'; 1000].into()).await.unwrap();
        upload.abort().await.unwrap();

        assert!(matches!(
            store.head(&location).await,
            Err(object_store::Error::NotFound { .. })
        ));
        // The listing hides a temporary, so the directory itself is read for one.
        let path = store.inner.remote(&Path::from(dir));
        let left = store
            .inner
            .run(async |c| c.sftp.read_dir(path.clone()).await)
            .await
            .unwrap()
            .unwrap();
        let names: Vec<_> = left.map(|e| e.file_name()).collect();
        assert!(names.is_empty(), "{names:?}");
    }
}
