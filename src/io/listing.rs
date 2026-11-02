//! A [`Queue`] over one bucket's listing: each object under a prefix is a message of its own, offered when a listing
//! finds it new or changed since it was last read.
//!
//! ```text
//!   list, page by page ──► new or changed? ──► receive: one object per message ──► ObjectSource reads it
//!         │                                                                                  │
//!         └── a ledger key unseen: gone ◄── ledger ◄── keep ◄── delete: every record committed ┘
//!                                                       └─ or delete the object, or move it under another prefix
//! ```
//!
//! What is read is kept in a ledger of key and version, handed to the leader as [`Progress`], so whichever worker
//! holds the source next reads only what this one did not. Deleted or moved after commit, the bucket is its own queue and no
//! ledger is kept.
//!
//! An object handed out [`MAX_LAPSES`] times and never committed is unreadable, and no longer offered until it changes:
//! the listing's own redrive. Kept, it goes into the ledger as read; deleted or moved, it stays in the bucket.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::heartbeat::LedgerEntry;
use crate::io::object_source::{Message, ObjectRef, Queue};
use crate::io::{IoError, Progress};
use crate::plan::topology::{AfterCommit, ListStart};

/// How long one page of a listing may take before the listing fails.
const PAGE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long one HEAD or delete after commit may take before it fails, to be tried again.
const CHANGE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the rename of a move may take before it fails, to be tried again. A bucket copies a large object whole.
const COPY_TIMEOUT: Duration = Duration::from_secs(600);

/// How many times one version of an object may lapse before it counts as unreadable.
const MAX_LAPSES: u32 = 5;

/// The cursor of a source that has listed once. Its ledger then holds what a start from new skipped, so a later
/// holder reads everything it does not hold.
const LISTED: &str = "listed";

/// The objects under a prefix of one bucket, as a [`Queue`]. A message's handle is its object's key.
pub struct Listing {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    /// Only keys that start with it, as S3 means a prefix: not only whole segments.
    prefix: String,
    /// What a listing asks the store for: `prefix` up to its last `/`. The store matches whole segments only.
    listed_under: Option<Path>,
    poll_interval: Duration,
    start: ListStart,
    after_commit: AfterCommit,
    runtime: Arc<tokio::runtime::Runtime>,
    /// [`PAGE_TIMEOUT`] but in tests.
    page_timeout: Duration,
    /// An object is read only once two listings in a row find it the same: what a file server shows may be an upload
    /// under way.
    steady: bool,
    state: Mutex<State>,
    /// Wakes a receive waiting for the next listing when the source closes.
    stopped: Condvar,
}

#[derive(Default)]
struct State {
    /// What has been read, by key. Kept only after commit, and empty but for a start from new.
    ledger: HashMap<String, Read>,
    /// What the last listing found new or changed and no receive has taken yet.
    found: VecDeque<Found>,
    /// Empty objects the last listing found, with the ETag listed, to delete or move unread: they hold no record.
    empty: Vec<(String, Option<String>)>,
    in_flight: HashMap<String, InFlight>,
    /// Keys deleted or moved after commit, by the listing under way when they were: one started before may still see
    /// them.
    committed: HashMap<String, Committed>,
    /// Objects deleted or moved after commit that lapsed [`MAX_LAPSES`] times, left in the bucket while unchanged.
    unreadable: HashMap<String, Read>,
    /// The source is closing: no listing goes on, and an object shown again has not lapsed.
    stopping: bool,
    /// For a listing read [`Listing::when_steady`], what each key held at the last listing that found it new or
    /// changed, and that listing's number.
    unsteady: HashMap<String, (String, u64)>,
    /// The number of the last listing started.
    generation: u64,
    last_listing: Option<Instant>,
    /// A listing has completed in this source's life, on this worker or one before it.
    started: bool,
    /// A listing has completed on this worker.
    listed: bool,
    /// What `take_progress` hands out next. `added` is by key, so a key read twice is sent once.
    cursor: Option<String>,
    added: BTreeMap<String, String>,
    removed: Vec<String>,
}

impl State {
    /// Records `key` as read at `entry`, in the ledger and in the change the leader is handed next.
    fn keep(&mut self, key: String, entry: String, generation: u64) {
        self.added.insert(key.clone(), entry.clone());
        self.ledger.insert(key, Read { entry, generation });
    }
}

/// One key's entry in the ledger.
struct Read {
    /// Its version read, as [`Version`] writes it.
    entry: String,
    /// The last listing that saw it.
    generation: u64,
}

/// An object a listing found to read.
struct Found {
    key: String,
    entry: String,
    e_tag: Option<String>,
    version: Option<String>,
}

/// An object handed out and not yet committed.
struct InFlight {
    /// When it may be offered again unless extended.
    until: Instant,
    entry: String,
    /// What a delete or move after commit checks the object still holds.
    e_tag: Option<String>,
    /// The last listing that saw it.
    generation: u64,
    /// How many times this entry was handed out before and lapsed.
    lapses: u32,
}

/// A key deleted or moved after commit.
struct Committed {
    /// The listing under way when it was. [`u64::MAX`] is one whose delete failed, until it is tried again.
    generation: u64,
    /// The version read, which a retried delete or move checks.
    e_tag: Option<String>,
}

/// What a delete or move after commit did.
enum Change {
    Done,
    /// The key holds another version than the one read, which is left to be read.
    Replaced,
}

/// Which version of an object was read, as a listing compares it. JSON, so another kind of listing can add fields.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Version<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    e_tag: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
}

/// A ledger entry as the leader stores it, read back as the [`Version`] it names. Fields it does not name, such as the
/// `bytes` and `tail` of a build that read objects in parts, are left out, so the entry compares with what a listing
/// finds. An entry this build cannot parse is compared as it is.
fn stored_version(stored: String) -> String {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Entry {
        e_tag: Option<String>,
        version: Option<String>,
    }
    match serde_json::from_str::<Entry>(&stored) {
        Ok(entry) => serde_json::to_string(&Version {
            e_tag: entry.e_tag.as_deref(),
            version: entry.version.as_deref(),
        })
        .expect("two strings serialize"),
        Err(_) => stored,
    }
}

impl Listing {
    /// Lists `bucket` in `store` for keys under `prefix`, once `poll_interval` after the last listing started, on
    /// `runtime`. A prefix the store cannot list is an error here.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        bucket: &str,
        prefix: &str,
        poll_interval: Duration,
        start: ListStart,
        after_commit: AfterCommit,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Listing, IoError> {
        let segments = prefix.rfind('/').map_or("", |at| &prefix[..at]);
        let listed_under = if segments.is_empty() {
            None
        } else {
            Some(
                Path::parse(segments)
                    .map_err(|e| IoError::transient(format!("bucket {bucket}: prefix {prefix:?}: {e}")))?,
            )
        };
        Ok(Listing {
            store,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            listed_under,
            poll_interval,
            start,
            after_commit,
            runtime,
            page_timeout: PAGE_TIMEOUT,
            steady: false,
            state: Mutex::default(),
            stopped: Condvar::new(),
        })
    }

    /// Reads an object only once two listings in a row find it the same.
    pub fn when_steady(mut self) -> Listing {
        self.steady = true;
        self
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Each field is whole at every point the lock is released.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Up to `max` of what the last listing found, each in flight for `visibility` from now.
    fn offer(&self, max: usize, visibility: Duration) -> Vec<Message> {
        let now = Instant::now();
        let mut state = self.state();
        let mut out = Vec::new();
        while out.len() < max
            && let Some(found) = state.found.pop_front()
        {
            let taken = state.in_flight.get(&found.key).is_some_and(|f| f.until > now)
                || state.committed.contains_key(&found.key)
                || state.ledger.get(&found.key).is_some_and(|r| r.entry == found.entry)
                || state.unreadable.get(&found.key).is_some_and(|r| r.entry == found.entry);
            if taken {
                continue;
            }
            // Still in flight, it was handed out and lapsed.
            let lapses = match state.in_flight.get(&found.key) {
                Some(f) if f.entry == found.entry => f.lapses + 1,
                _ => 0,
            };
            if lapses >= MAX_LAPSES {
                self.give_up(&mut state, found);
                continue;
            }
            state.unreadable.remove(&found.key);
            let generation = state.generation;
            state.in_flight.insert(
                found.key.clone(),
                InFlight {
                    until: now + visibility,
                    entry: found.entry,
                    e_tag: found.e_tag,
                    generation,
                    lapses,
                },
            );
            out.push(Message {
                handle: found.key.clone(),
                objects: Ok(vec![ObjectRef {
                    bucket: self.bucket.clone(),
                    key: found.key,
                    version: found.version,
                }]),
            });
        }
        out
    }

    /// No longer offers `found`, which lapsed [`MAX_LAPSES`] times: kept, it is recorded as read; deleted or moved, it
    /// is left in the bucket.
    fn give_up(&self, state: &mut State, found: Found) {
        state.in_flight.remove(&found.key);
        let keep = self.after_commit == AfterCommit::Keep;
        tracing::warn!(
            target: "listing.unreadable",
            bucket = %self.bucket,
            key = %found.key,
            lapses = MAX_LAPSES,
            "an object was handed out {MAX_LAPSES} times and never committed; it is no longer read until it changes{}",
            if keep { ", and is recorded as read" } else { ", and is skipped" }
        );
        let generation = state.generation;
        if keep {
            state.keep(found.key, found.entry, generation);
        } else {
            let read = Read {
                entry: found.entry,
                generation,
            };
            state.unreadable.insert(found.key, read);
        }
    }

    /// Lists the bucket, marking each ledger key seen and finding what is new or changed. Only a listing that
    /// completes says what is gone. One the source's closing stops fails.
    fn list(&self) -> Result<(), IoError> {
        let generation = {
            let mut state = self.state();
            state.generation += 1;
            state.last_listing = Some(Instant::now());
            state.found.clear();
            state.empty.clear();
            state.generation
        };
        let mut pages = self.store.list(self.listed_under.as_ref());
        let listed = self.runtime.block_on(async {
            loop {
                if self.state().stopping {
                    return Err("the source is closing".to_string());
                }
                let next = std::future::poll_fn(|cx| pages.as_mut().poll_next(cx));
                match tokio::time::timeout(self.page_timeout, next).await {
                    Err(_) => return Err(format!("no answer within {:?}", self.page_timeout)),
                    Ok(None) => return Ok(()),
                    Ok(Some(Err(e))) => return Err(listing_failed(&e)),
                    Ok(Some(Ok(meta))) => self.saw(generation, &meta),
                }
            }
        });
        if let Err(why) = listed {
            // Only part of the bucket. Left to read, a start from new would read what its first listing that
            // completes takes as already there.
            self.state().found.clear();
            return Err(IoError::transient(format!("bucket {}: listing: {why}", self.bucket)));
        }
        self.completed(generation);
        let empty = std::mem::take(&mut self.state().empty);
        for (key, e_tag) in empty {
            // A folder marker is gone by its listed key, so it counts as done and is left; any other failure is
            // tried again on the next listing.
            if let Err(why) = self.after_commit(&key, e_tag.as_deref()) {
                tracing::warn!(target: "listing.empty", bucket = %self.bucket, key, error = %why,
                    "an empty object was not removed; trying again on the next listing");
            }
        }
        Ok(())
    }

    fn saw(&self, generation: u64, meta: &ObjectMeta) {
        let key = meta.location.as_ref();
        if !key.starts_with(&self.prefix) {
            return;
        }
        let entry = serde_json::to_string(&Version {
            e_tag: meta.e_tag.as_deref(),
            version: meta.version.as_deref(),
        })
        .expect("two strings serialize");
        // An empty object holds no record, and a folder marker (`logs/`) is listed as a key with its `/` gone, which
        // no GET finds. Neither is read; delete and move still take an empty object, so the bucket drains, once it is
        // steady where the listing waits for that: a file just created is empty until it is written.
        if meta.size == 0 {
            if self.after_commit != AfterCommit::Keep {
                let mut state = self.state();
                if self.steady {
                    let seen = state.unsteady.insert(key.to_string(), (entry.clone(), generation));
                    if seen.is_none_or(|(before, _)| before != entry) {
                        return;
                    }
                }
                state.empty.push((key.to_string(), meta.e_tag.clone()));
            }
            return;
        }
        let now = Instant::now();
        let mut state = self.state();
        if let Some(read) = state.ledger.get_mut(key) {
            read.generation = generation;
            if read.entry == entry {
                return;
            }
        }
        if let Some(f) = state.in_flight.get_mut(key) {
            f.generation = generation;
            if f.until > now {
                // Still steady should the read lapse: it is offered again without waiting a listing more.
                if self.steady {
                    state.unsteady.insert(key.to_string(), (entry, generation));
                }
                return;
            }
        }
        if let Some(skipped) = state.unreadable.get_mut(key) {
            skipped.generation = generation;
            if skipped.entry == entry {
                return;
            }
        }
        if state.committed.contains_key(key) {
            return;
        }
        // A first listing that starts from new records what it finds as read, steady or not.
        if self.steady && (state.started || self.start == ListStart::Everything) {
            let seen = state.unsteady.insert(key.to_string(), (entry.clone(), generation));
            if seen.is_none_or(|(before, _)| before != entry) {
                return;
            }
        }
        state.found.push_back(Found {
            key: key.to_string(),
            entry,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        });
    }

    fn completed(&self, generation: u64) {
        let now = Instant::now();
        let mut state = self.state();
        let state = &mut *state;
        if !state.listed {
            state.listed = true;
            state.cursor = Some(LISTED.to_string());
        }
        if !state.started {
            state.started = true;
            if self.start == ListStart::New {
                for found in std::mem::take(&mut state.found) {
                    state.keep(found.key, found.entry, generation);
                }
            }
        }
        if self.after_commit == AfterCommit::Keep {
            let in_flight = &state.in_flight;
            let gone: Vec<String> = state
                .ledger
                .iter()
                .filter(|(key, read)| read.generation != generation && !in_flight.contains_key(*key))
                .map(|(key, _)| key.clone())
                .collect();
            for key in gone {
                state.ledger.remove(&key);
                state.added.remove(&key);
                state.removed.push(key);
            }
        }
        // A listing that started after a delete no longer sees what it deleted.
        state.committed.retain(|_, c| c.generation >= generation);
        state.unreadable.retain(|_, r| r.generation == generation);
        state.unsteady.retain(|_, (_, seen)| *seen == generation);
        // One lapsed and gone is never offered again.
        state
            .in_flight
            .retain(|_, f| f.until > now || f.generation == generation);
    }

    /// Deletes `key`, or moves it under the move prefix in place of the source's own, if it still holds `e_tag`, the
    /// version read. One gone was deleted by an earlier try, and is done, but for a move that did not reach its
    /// destination.
    fn after_commit(&self, key: &str, e_tag: Option<&str>) -> Result<Change, String> {
        let path = Path::parse(key).map_err(|e| e.to_string())?;
        let to = match &self.after_commit {
            AfterCommit::Move { prefix } => {
                let to = format!("{prefix}{}", key.strip_prefix(&self.prefix).unwrap_or(key));
                Some(Path::parse(&to).map_err(|e| format!("cannot move to {to:?}: {e}"))?)
            }
            _ => None,
        };
        match self.within(CHANGE_TIMEOUT, self.store.head(&path))? {
            Ok(meta) => {
                // Azure lists an ETag bare and answers a HEAD with it quoted.
                let bare = |t: &str| t.trim_matches('"').to_string();
                if e_tag.is_some() && meta.e_tag.as_deref().map(bare) != e_tag.map(bare) {
                    return Ok(Change::Replaced);
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                let Some(to) = to else { return Ok(Change::Done) };
                return match self.within(CHANGE_TIMEOUT, self.store.head(&to))? {
                    Ok(_) => Ok(Change::Done),
                    // Something else removed it, and no retry can move what is not there.
                    Err(e) => {
                        tracing::warn!(
                            target: "listing.moved_away",
                            bucket = %self.bucket,
                            key,
                            error = %e,
                            "an object read was gone before it could be moved, and is not under the move prefix"
                        );
                        Ok(Change::Done)
                    }
                };
            }
            Err(e) => return Err(e.to_string()),
        }
        // A rename: a copy and a delete in a bucket, one call on a file server.
        let changed = match &to {
            Some(to) => self.within(COPY_TIMEOUT, self.store.rename(&path, to))?,
            None => self.within(CHANGE_TIMEOUT, self.store.delete(&path))?,
        };
        changed.map_err(|e| e.to_string())?;
        Ok(Change::Done)
    }

    /// What `call` answers, or why it gave no answer within `limit`.
    fn within<T>(
        &self,
        limit: Duration,
        call: impl Future<Output = object_store::Result<T>>,
    ) -> Result<object_store::Result<T>, String> {
        crate::io::object_source::within(&self.runtime, limit, call)
    }
}

/// Why a listing failed. A key the store cannot parse fails every listing, so its error, which names the key, says so.
fn listing_failed(e: &object_store::Error) -> String {
    match e {
        object_store::Error::InvalidPath { .. } => {
            format!("{e}; the prefix cannot be listed until that key is renamed or removed")
        }
        _ => e.to_string(),
    }
}

impl Queue for Listing {
    fn name(&self) -> &str {
        &self.bucket
    }

    /// What the last listing found, or if nothing and one is due, what a new listing finds. Otherwise waits until one
    /// is due, up to `wait` or until the source closes, and answers nothing.
    fn receive(&self, max: usize, wait: Duration, visibility: Duration) -> Result<Vec<Message>, IoError> {
        let offered = self.offer(max, visibility);
        if !offered.is_empty() {
            return Ok(offered);
        }
        let state = self.state();
        let due = state.last_listing.map(|at| at + self.poll_interval);
        let now = Instant::now();
        match due {
            Some(due) if due > now => {
                let _ = self
                    .stopped
                    .wait_timeout_while(state, wait.min(due - now), |s| !s.stopping);
                Ok(Vec::new())
            }
            _ => {
                drop(state);
                self.list()?;
                Ok(self.offer(max, visibility))
            }
        }
    }

    /// In flight for `visibility` from now; zero offers it again on the next listing, a lapse unless the source is
    /// closing.
    fn extend(&self, handles: &[String], visibility: Duration) -> Vec<(String, String)> {
        let until = Instant::now() + visibility;
        let mut state = self.state();
        // Shown again as the source closes, it is forgotten rather than lapsed.
        let forget = visibility.is_zero() && state.stopping;
        // One no longer in flight is committed or forgotten: nothing is left to extend.
        for handle in handles {
            if forget {
                state.in_flight.remove(handle);
            } else if let Some(f) = state.in_flight.get_mut(handle) {
                f.until = until;
            }
        }
        Vec::new()
    }

    /// Every record of each is committed: keeps it in the ledger, or deletes or moves it.
    fn delete(&self, handles: &[String]) -> Vec<(String, String)> {
        if self.after_commit == AfterCommit::Keep {
            let mut state = self.state();
            let state = &mut *state;
            for handle in handles {
                // One no longer in flight lapsed and went from the bucket: nothing is left to keep.
                let Some(f) = state.in_flight.remove(handle) else {
                    continue;
                };
                let generation = state.generation;
                state.keep(handle.clone(), f.entry, generation);
            }
            return Vec::new();
        }
        let mut failed = Vec::new();
        for handle in handles {
            let e_tag = {
                let state = self.state();
                match (state.in_flight.get(handle), state.committed.get(handle)) {
                    (Some(f), _) => f.e_tag.clone(),
                    (None, Some(c)) => c.e_tag.clone(),
                    (None, None) => None,
                }
            };
            let done = self.after_commit(handle, e_tag.as_deref());
            let mut state = self.state();
            state.in_flight.remove(handle);
            let generation = match &done {
                Ok(Change::Done) => state.generation,
                Ok(Change::Replaced) => {
                    // The next listing finds the version that replaced it.
                    state.committed.remove(handle);
                    continue;
                }
                Err(_) => u64::MAX,
            };
            state.committed.insert(handle.clone(), Committed { generation, e_tag });
            if let Err(why) = done {
                failed.push((handle.clone(), why));
            }
        }
        failed
    }

    /// A first listing's cursor goes out alone, before anything it put in the ledger: a ledger too large for one
    /// heartbeat is sent in parts, and a holder that took over between them reads the rest again rather than skip it.
    fn take_progress(&self) -> Option<Progress> {
        if self.after_commit != AfterCommit::Keep {
            return None;
        }
        let mut state = self.state();
        if state.cursor.is_some() {
            return Some(Progress {
                cursor: state.cursor.take(),
                ..Progress::default()
            });
        }
        if state.added.is_empty() && state.removed.is_empty() {
            return None;
        }
        Some(Progress {
            cursor: state.cursor.take(),
            added: std::mem::take(&mut state.added)
                .into_iter()
                .map(|(item, entry)| LedgerEntry { item, entry })
                .collect(),
            removed: std::mem::take(&mut state.removed),
        })
    }

    fn resume(&self, from: Progress) -> Result<(), IoError> {
        let mut state = self.state();
        for item in from.removed {
            state.ledger.remove(&item);
        }
        for LedgerEntry { item, entry } in from.added {
            let read = Read {
                entry: stored_version(entry),
                generation: 0,
            };
            state.ledger.insert(item, read);
        }
        state.started |= from.cursor.is_some();
        Ok(())
    }

    /// A listing under way stops at its next page, and fails; a receive waiting for the next answers at once.
    fn stop(&self) {
        self.state().stopping = true;
        self.stopped.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    use super::*;
    use crate::io::object_source::{MAX_RECORD_BYTES, ObjectSource};
    use crate::io::{CommitMap, Source, TopicPartition};
    use crate::plan::topology::Framing;

    const HOUR: Duration = Duration::from_secs(3600);

    struct Setup {
        store: Arc<InMemory>,
        runtime: Arc<tokio::runtime::Runtime>,
    }

    impl Setup {
        fn new() -> Setup {
            Setup {
                store: Arc::new(InMemory::new()),
                runtime: crate::io::objects::runtime().unwrap(),
            }
        }

        fn put(&self, key: &str, body: &str) {
            let path = Path::parse(key).unwrap();
            let put = self.store.put(&path, PutPayload::from(body.to_string()));
            self.runtime.block_on(put).unwrap();
        }

        fn remove(&self, key: &str) {
            self.runtime
                .block_on(self.store.delete(&Path::parse(key).unwrap()))
                .unwrap();
        }

        fn exists(&self, key: &str) -> bool {
            let path = Path::parse(key).unwrap();
            self.runtime.block_on(self.store.head(&path)).is_ok()
        }

        /// Lists on every receive that finds nothing waiting.
        fn listing(&self, prefix: &str, start: ListStart, after_commit: AfterCommit) -> Listing {
            Listing::new(
                self.store.clone(),
                "logs",
                prefix,
                Duration::ZERO,
                start,
                after_commit,
                self.runtime.clone(),
            )
            .unwrap()
        }
    }

    /// The keys one receive hands out, sorted.
    fn receive(listing: &Listing) -> Vec<String> {
        let mut keys: Vec<String> = listing
            .receive(100, Duration::ZERO, HOUR)
            .unwrap()
            .into_iter()
            .map(|m| {
                let objects = m.objects.unwrap();
                assert_eq!(objects.len(), 1);
                assert_eq!((objects[0].bucket.as_str(), &objects[0].key), ("logs", &m.handle));
                m.handle
            })
            .collect();
        keys.sort();
        keys
    }

    fn handles(keys: &[&str]) -> Vec<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    fn items(progress: &Progress) -> Vec<&str> {
        progress.added.iter().map(|e| e.item.as_str()).collect()
    }

    #[test]
    fn a_new_object_is_read_once() {
        let s = Setup::new();
        s.put("a", "1");
        s.put("b", "2");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        assert_eq!(receive(&listing), ["a", "b"]);
        assert!(receive(&listing).is_empty(), "in flight");
        assert!(listing.delete(&handles(&["a", "b"])).is_empty());
        assert_eq!(listing.take_progress().unwrap().cursor.as_deref(), Some(LISTED));
        let progress = listing.take_progress().unwrap();
        assert_eq!((progress.cursor.as_deref(), items(&progress)), (None, vec!["a", "b"]));
        assert!(
            progress
                .added
                .iter()
                .all(|e| e.entry.len() <= crate::heartbeat::MAX_LEDGER_ENTRY)
        );
        assert!(receive(&listing).is_empty(), "in the ledger");
        assert_eq!(listing.take_progress(), None);
    }

    #[test]
    fn a_changed_object_is_read_again() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        assert_eq!(receive(&listing), ["a"]);
        listing.delete(&handles(&["a"]));
        listing.take_progress();
        let first = listing.take_progress().unwrap().added;
        s.put("a", "2");
        assert_eq!(receive(&listing), ["a"]);
        listing.delete(&handles(&["a"]));
        let second = listing.take_progress().unwrap();
        assert_eq!(items(&second), ["a"]);
        assert_ne!(second.added[0].entry, first[0].entry);
    }

    #[test]
    fn a_gone_object_leaves_the_ledger_only_after_a_completed_listing() {
        let s = Setup::new();
        s.put("a", "1");
        s.put("b", "2");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        receive(&listing);
        listing.delete(&handles(&["a", "b"]));
        listing.take_progress();
        listing.take_progress();
        s.remove("a");
        assert!(receive(&listing).is_empty());
        let progress = listing.take_progress().unwrap();
        assert_eq!((progress.removed, progress.added), (handles(&["a"]), Vec::new()));

        // An object held in flight is not gone while it is read.
        s.put("c", "3");
        assert_eq!(receive(&listing), ["c"]);
        s.remove("c");
        assert!(receive(&listing).is_empty());
        assert_eq!(listing.take_progress(), None);
    }

    /// A listing that fails says nothing of what it did not see.
    #[test]
    fn a_failed_listing_removes_nothing() {
        let s = Setup::new();
        s.put("a", "1");
        let throttle = object_store::throttle::ThrottleConfig {
            wait_list_per_entry: Duration::from_millis(500),
            ..Default::default()
        };
        let slow = object_store::throttle::ThrottledStore::new(s.store.clone(), throttle);
        let listing = Listing {
            page_timeout: Duration::from_millis(50),
            ..Listing::new(
                Arc::new(slow),
                "logs",
                "",
                Duration::ZERO,
                ListStart::Everything,
                AfterCommit::Keep,
                s.runtime.clone(),
            )
            .unwrap()
        };
        listing
            .resume(Progress {
                cursor: Some(LISTED.to_string()),
                added: vec![LedgerEntry {
                    item: "gone".to_string(),
                    entry: "{}".to_string(),
                }],
                removed: Vec::new(),
            })
            .unwrap();
        let failed = listing.receive(100, Duration::ZERO, HOUR).unwrap_err();
        assert!(failed.message.contains("no answer"), "{}", failed.message);
        assert_eq!(listing.take_progress(), None);
    }

    #[test]
    fn a_prefix_matches_part_of_a_segment() {
        let s = Setup::new();
        s.put("logs/2024-01.json", "1");
        s.put("logs/2024/02.json", "2");
        s.put("logs/2025-01.json", "3");
        s.put("logs2024", "4");
        s.put("logs/2024-empty", "");
        let listing = s.listing("logs/2024", ListStart::Everything, AfterCommit::Keep);
        assert_eq!(receive(&listing), ["logs/2024-01.json", "logs/2024/02.json"]);
        let whole = s.listing("logs", ListStart::Everything, AfterCommit::Keep);
        assert_eq!(receive(&whole).len(), 4);
    }

    #[test]
    fn a_start_from_new_reads_only_what_arrives_after_the_first_listing() {
        let s = Setup::new();
        s.put("old", "1");
        let listing = s.listing("", ListStart::New, AfterCommit::Keep);
        assert!(receive(&listing).is_empty());
        let cursor = listing.take_progress().unwrap();
        assert_eq!(
            cursor,
            Progress {
                cursor: Some(LISTED.to_string()),
                ..Progress::default()
            },
            "alone"
        );
        assert_eq!(items(&listing.take_progress().unwrap()), ["old"]);
        s.put("new", "2");
        assert_eq!(receive(&listing), ["new"]);

        // Resumed from a source that has started, everything it does not hold is new.
        let resumed = s.listing("", ListStart::New, AfterCommit::Keep);
        resumed
            .resume(Progress {
                cursor: Some(LISTED.to_string()),
                ..Progress::default()
            })
            .unwrap();
        assert_eq!(receive(&resumed), ["new", "old"]);
    }

    #[test]
    fn a_resumed_listing_skips_what_its_ledger_holds() {
        let s = Setup::new();
        s.put("a", "1");
        s.put("b", "2");
        let first = s.listing("", ListStart::Everything, AfterCommit::Keep);
        assert_eq!(receive(&first), ["a", "b"]);
        first.delete(&handles(&["a"]));
        let cursor = first.take_progress().unwrap();
        let progress = first.take_progress().unwrap();

        let next = s.listing("", ListStart::Everything, AfterCommit::Keep);
        next.resume(cursor).unwrap();
        next.resume(progress).unwrap();
        assert_eq!(receive(&next), ["b"]);
    }

    #[test]
    fn a_lapsed_object_is_offered_again() {
        let s = Setup::new();
        s.put("a", "1");
        s.put("b", "2");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        let offered = listing.receive(100, Duration::ZERO, Duration::from_millis(50)).unwrap();
        assert_eq!(offered.len(), 2);
        assert!(listing.extend(&handles(&["a"]), HOUR).is_empty());
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(receive(&listing), ["b"], "a was extended");
        assert!(listing.extend(&handles(&["a"]), Duration::ZERO).is_empty());
        assert_eq!(receive(&listing), ["a"], "shown again at once");
    }

    #[test]
    fn a_committed_object_is_deleted() {
        let s = Setup::new();
        s.put("in/a", "1");
        s.put("other", "2");
        let listing = s.listing("in/", ListStart::Everything, AfterCommit::Delete);
        assert_eq!(receive(&listing), ["in/a"]);
        assert!(listing.delete(&handles(&["in/a"])).is_empty());
        assert_eq!((s.exists("in/a"), s.exists("other")), (false, true));
        assert!(receive(&listing).is_empty());
        assert_eq!(listing.take_progress(), None);
    }

    #[test]
    fn an_empty_object_is_deleted_unread_and_kept_unread() {
        let s = Setup::new();
        s.put("in/empty", "");
        let kept = s.listing("in/", ListStart::Everything, AfterCommit::Keep);
        assert!(receive(&kept).is_empty());
        assert!(s.exists("in/empty"));
        let deleting = s.listing("in/", ListStart::Everything, AfterCommit::Delete);
        assert!(receive(&deleting).is_empty());
        assert!(!s.exists("in/empty"));
    }

    #[test]
    fn a_committed_object_is_moved() {
        let s = Setup::new();
        s.put("in/a", "1");
        let listing = s.listing(
            "in/",
            ListStart::Everything,
            AfterCommit::Move {
                prefix: "archive/".to_string(),
            },
        );
        assert_eq!(receive(&listing), ["in/a"]);
        assert!(listing.delete(&handles(&["in/a"])).is_empty());
        assert_eq!((s.exists("in/a"), s.exists("archive/a")), (false, true));
        assert!(receive(&listing).is_empty());
        assert_eq!(listing.take_progress(), None);
    }

    /// A key gone and read again in one change is removed, then added: removals apply first.
    #[test]
    fn a_change_removes_before_it_adds() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        receive(&listing);
        listing.delete(&handles(&["a"]));
        listing.take_progress();
        listing.take_progress();
        s.remove("a");
        receive(&listing);
        s.put("a", "2");
        assert_eq!(receive(&listing), ["a"]);
        listing.delete(&handles(&["a"]));
        let progress = listing.take_progress().unwrap();
        assert_eq!((&progress.removed, items(&progress)), (&handles(&["a"]), vec!["a"]));

        // Gone before it was sent, an addition is never sent.
        s.put("b", "1");
        assert_eq!(receive(&listing), ["b"]);
        listing.delete(&handles(&["b"]));
        s.remove("b");
        receive(&listing);
        let progress = listing.take_progress().unwrap();
        assert_eq!((progress.removed, progress.added), (handles(&["b"]), Vec::new()));
    }

    /// The keys one receive hands out, each in flight for `visibility`.
    fn receive_for(listing: &Listing, visibility: Duration) -> Vec<String> {
        let messages = listing.receive(100, Duration::ZERO, visibility).unwrap();
        messages.into_iter().map(|m| m.handle).collect()
    }

    /// Over `store`, which answers each listed object after `per_object`.
    fn slow(s: &Setup, per_object: Duration, start: ListStart, poll_interval: Duration) -> Listing {
        let throttle = object_store::throttle::ThrottleConfig {
            wait_list_per_entry: per_object,
            ..Default::default()
        };
        let slow = object_store::throttle::ThrottledStore::new(s.store.clone(), throttle);
        Listing::new(
            Arc::new(slow),
            "logs",
            "",
            poll_interval,
            start,
            AfterCommit::Keep,
            s.runtime.clone(),
        )
        .unwrap()
    }

    #[test]
    fn an_object_that_lapses_too_often_is_kept_as_read() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        for _ in 0..MAX_LAPSES {
            assert_eq!(receive_for(&listing, Duration::ZERO), ["a"]);
        }
        assert!(receive_for(&listing, Duration::ZERO).is_empty(), "unreadable");
        listing.take_progress();
        assert_eq!(
            items(&listing.take_progress().unwrap()),
            ["a"],
            "kept by the leader too"
        );

        // Rewritten, it is read again from a count of none.
        s.put("a", "2");
        for _ in 0..MAX_LAPSES {
            assert_eq!(receive_for(&listing, Duration::ZERO), ["a"]);
        }
        assert!(receive(&listing).is_empty());
    }

    #[test]
    fn a_steady_listing_reads_an_object_once_two_listings_find_it_the_same() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep).when_steady();
        assert!(receive(&listing).is_empty(), "seen once");
        s.put("a", "12");
        assert!(receive(&listing).is_empty(), "changed since");
        assert_eq!(receive(&listing), ["a"], "the same twice");
    }

    #[test]
    fn a_steady_listing_deletes_an_empty_object_once_two_listings_find_it_the_same() {
        let s = Setup::new();
        s.put("a", "");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Delete).when_steady();
        assert!(receive(&listing).is_empty());
        assert!(s.exists("a"), "seen once");
        assert!(receive(&listing).is_empty());
        assert!(!s.exists("a"), "the same twice");
    }

    #[test]
    fn a_steady_object_whose_read_lapses_is_offered_on_the_next_listing() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Delete).when_steady();
        assert!(receive(&listing).is_empty(), "seen once");
        assert_eq!(receive(&listing), ["a"]);
        assert!(receive(&listing).is_empty(), "in flight");
        listing.extend(&handles(&["a"]), Duration::ZERO);
        assert_eq!(receive(&listing), ["a"], "still steady");
    }

    #[test]
    fn an_object_shown_again_too_often_is_left_in_the_bucket() {
        let s = Setup::new();
        s.put("in/a", "1");
        let listing = s.listing("in/", ListStart::Everything, AfterCommit::Delete);
        for _ in 0..MAX_LAPSES {
            assert_eq!(receive(&listing), ["in/a"]);
            listing.extend(&handles(&["in/a"]), Duration::ZERO);
        }
        assert!(receive(&listing).is_empty(), "unreadable");
        assert!(receive(&listing).is_empty(), "skipped while unchanged");
        assert!(s.exists("in/a"));

        s.put("in/a", "2");
        assert_eq!(receive(&listing), ["in/a"]);
        assert!(listing.delete(&handles(&["in/a"])).is_empty());
        assert!(!s.exists("in/a"));
    }

    #[test]
    fn an_object_shown_again_as_the_source_closes_has_not_lapsed() {
        let s = Setup::new();
        s.put("a", "1");
        let listing = s.listing("", ListStart::Everything, AfterCommit::Keep);
        for _ in 0..MAX_LAPSES - 1 {
            assert_eq!(receive_for(&listing, Duration::ZERO), ["a"]);
        }
        listing.state().stopping = true;
        listing.extend(&handles(&["a"]), Duration::ZERO);
        assert!(listing.state().in_flight.is_empty(), "forgotten");
    }

    /// What a failed listing found is not read; the next that completes is the baseline of a start from new.
    #[test]
    fn a_failed_listing_leaves_nothing_to_read() {
        let s = Setup::new();
        for key in ["a", "b", "c", "d"] {
            s.put(key, "1");
        }
        let listing = Arc::new(slow(&s, Duration::from_millis(100), ListStart::New, Duration::ZERO));
        let stopper = {
            let listing = listing.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(250));
                listing.stop();
            })
        };
        let failed = listing.receive(100, Duration::ZERO, HOUR).unwrap_err();
        stopper.join().unwrap();
        assert!(failed.message.contains("closing"), "{}", failed.message);
        assert!(listing.state().found.is_empty());
        assert_eq!(listing.take_progress(), None);

        // Reopened, as no source is but in this test, so the next listing completes.
        listing.state().stopping = false;
        assert!(receive(&listing).is_empty(), "the baseline");
        assert_eq!(listing.take_progress().unwrap().cursor.as_deref(), Some(LISTED));
        assert_eq!(items(&listing.take_progress().unwrap()), ["a", "b", "c", "d"]);
    }

    #[test]
    fn stopping_ends_a_listing_and_a_wait() {
        let s = Setup::new();
        for i in 0..20 {
            s.put(&format!("k{i:02}"), "1");
        }
        let listing = Arc::new(slow(&s, Duration::from_millis(100), ListStart::Everything, HOUR));
        let stop_after = |listing: &Arc<Listing>| {
            let listing = listing.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(150));
                listing.stop();
            })
        };
        let started = Instant::now();
        let stopper = stop_after(&listing);
        assert!(listing.receive(100, Duration::ZERO, HOUR).is_err());
        stopper.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1), "stopped between pages");
        assert_eq!(listing.take_progress(), None, "a stopped listing removes nothing");

        // A receive waiting for the next listing.
        listing.state().stopping = false;
        let started = Instant::now();
        let stopper = stop_after(&listing);
        assert!(listing.receive(100, HOUR, HOUR).unwrap().is_empty());
        stopper.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(1), "woken");
    }

    #[test]
    fn an_object_replaced_since_its_read_is_not_deleted() {
        let s = Setup::new();
        s.put("in/a", "1");
        let listing = s.listing("in/", ListStart::Everything, AfterCommit::Delete);
        assert_eq!(receive(&listing), ["in/a"]);
        s.put("in/a", "2");
        assert!(listing.delete(&handles(&["in/a"])).is_empty());
        assert!(s.exists("in/a"));
        assert_eq!(receive(&listing), ["in/a"], "the new version is read");
        assert!(listing.delete(&handles(&["in/a"])).is_empty());
        assert!(!s.exists("in/a"));
    }

    #[test]
    fn a_move_retried_after_its_source_is_gone_is_done() {
        let s = Setup::new();
        s.put("in/a", "1");
        s.put("in/b", "1");
        let listing = s.listing(
            "in/",
            ListStart::Everything,
            AfterCommit::Move {
                prefix: "archive/".to_string(),
            },
        );
        assert_eq!(receive(&listing), ["in/a", "in/b"]);
        // A try whose answer was lost: copied and deleted.
        s.put("archive/a", "1");
        s.remove("in/a");
        assert!(listing.delete(&handles(&["in/a"])).is_empty());

        // Gone, and never copied: nothing is left to move, so no retry is asked for.
        s.remove("in/b");
        assert!(listing.delete(&handles(&["in/b"])).is_empty());
    }

    #[test]
    fn a_key_the_store_cannot_parse_is_named() {
        let source = Path::parse("in/a//b").unwrap_err();
        let why = listing_failed(&object_store::Error::InvalidPath { source });
        assert!(why.contains("\"in/a//b\""), "{why}");
        assert!(why.contains("renamed or removed"), "{why}");
    }

    /// A source reading what `listing` offers from the setup's store, line by line.
    fn source(s: &Setup, listing: Listing) -> ObjectSource {
        let store: Arc<dyn ObjectStore> = s.store.clone();
        ObjectSource::new(
            Arc::new(listing),
            Box::new(move |_: &str| Ok(store.clone())),
            s.runtime.clone(),
            Framing::Lines,
            HOUR,
            MAX_RECORD_BYTES,
        )
        .unwrap()
    }

    /// The lines of the polls from the first that reads any up to the next that reads none, each committed. A poll
    /// that reaches its deadline ends mid-object.
    fn read_lines(source: &mut ObjectSource) -> Vec<String> {
        let mut lines = Vec::new();
        for _ in 0..20 {
            let records = source.poll_batch(1000, 1 << 20, Duration::from_millis(50)).unwrap();
            if records.is_empty() {
                if lines.is_empty() {
                    continue;
                }
                break;
            }
            let mut offsets = CommitMap::new();
            for r in &records {
                let at = offsets
                    .entry(TopicPartition::new(r.topic.clone(), r.partition))
                    .or_insert(r.offset);
                *at = (*at).max(r.offset);
            }
            source.commit(&offsets).unwrap();
            lines.extend(
                records
                    .into_iter()
                    .map(|r| String::from_utf8(r.bytes().unwrap().to_vec()).unwrap()),
            );
        }
        lines
    }

    #[test]
    fn a_grown_object_is_read_whole_again() {
        let s = Setup::new();
        s.put("a", "a\nb");
        let mut source = source(&s, s.listing("", ListStart::Everything, AfterCommit::Keep));
        assert_eq!(
            read_lines(&mut source),
            ["a", "b"],
            "a last line with no newline is read"
        );
        s.put("a", "a\nb\nc\n");
        assert_eq!(read_lines(&mut source), ["a", "b", "c"]);
        assert!(read_lines(&mut source).is_empty());
    }

    #[test]
    fn a_rewritten_object_is_read_whole() {
        let s = Setup::new();
        s.put("a", "a\nb\n");
        let mut source = source(&s, s.listing("", ListStart::Everything, AfterCommit::Keep));
        assert_eq!(read_lines(&mut source), ["a", "b"]);
        s.put("a", "x\n");
        assert_eq!(read_lines(&mut source), ["x"]);
        assert!(read_lines(&mut source).is_empty());
    }

    /// A build that read objects in parts stored how far it read beside the version. Resumed, that entry is the
    /// version alone, so the object it names is not read again.
    #[test]
    fn a_ledger_entry_with_a_read_count_is_its_version() {
        let s = Setup::new();
        s.put("a", "a\nb\n");
        let mut first = source(&s, s.listing("", ListStart::Everything, AfterCommit::Keep));
        assert_eq!(read_lines(&mut first), ["a", "b"]);
        let cursor = first.take_progress().unwrap();
        let mut progress = first.take_progress().unwrap();
        drop(first);
        let mut entry: serde_json::Value = serde_json::from_str(&progress.added[0].entry).unwrap();
        entry["bytes"] = 4.into();
        entry["tail"] = "0".repeat(64).into();
        progress.added[0].entry = entry.to_string();

        let next = s.listing("", ListStart::Everything, AfterCommit::Keep);
        next.resume(cursor).unwrap();
        next.resume(progress).unwrap();
        let mut next = source(&s, next);
        assert!(read_lines(&mut next).is_empty());
    }
}
