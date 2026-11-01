//! A single-worker source, read only while the leader holds it for this worker.
//!
//! Every worker of a group runs the same bundle, so every one of them deploys a single-worker source; the leader
//! names one of them its holder (`HeartbeatResponse::holds`). Each such source is read through a [`HeldSource`] on
//! the same reader thread and lane as any other source. What it wraps is opened only while held, and resumes from
//! what the leader says an earlier holder delivered:
//!
//! ```text
//!   supervisor                     runtime: frame thread              reader thread
//!       |--- Holds: hold, ledger file -->| Held::apply                    |
//!       |                                |   Slot::grant ---------------->| opens the source, resumes it
//!       |                                |                                | polls, commits
//!       |<-- Report: progress -----------|<-- outbox <--------------------| take_progress after each poll and commit
//!       |--- Holds: without it --------->|   Slot::revoke --------------->| drops the source
//! ```
//!
//! A revoke drops the source at once, with batches still in flight. Their commits are ignored, so whoever holds
//! it next reads them again: duplicates, never lost rows.
//!
//! Progress the leader has not taken is bounded. Past [`BACKLOG_FULL`], here or in the supervisor, held sources
//! read nothing new until it is back under [`BACKLOG_EASED`]; what they already read still commits and reports.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::heartbeat::{Hold, LedgerEntry, MAX_CURSOR, MAX_LEDGER_ENTRY, MAX_LEDGER_ITEM, SourceKey, SourceProgress};
use crate::io::{CommitMap, IoError, PartitionLag, Progress, Source, SourceRecord, TopicPartition};

/// One hold as the supervisor hands it to its runtime: what the leader said, and where the supervisor wrote the
/// ledger it fetched for it. With no file, the source is only kept reading where it already is, never granted:
/// granted without its ledger, it would read everything again.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Holding {
    pub hold: Hold,
    pub ledger_file: Option<std::path::PathBuf>,
}

impl Holding {
    /// Remove this hold's ledger file, if it has one, for a hold that will not be granted from it.
    pub fn discard_ledger(&self) {
        if let Some(file) = &self.ledger_file {
            let _ = std::fs::remove_file(file);
        }
    }
}

/// Whether one source is held now, shared between the frame thread that hears it and the reader thread that acts
/// on it. Each grant and each revoke is a new generation, so a reader that missed both still sees the change.
#[derive(Default)]
pub struct Slot {
    grant: Mutex<Grant>,
    changed: Condvar,
}

#[derive(Default)]
struct Grant {
    generation: u64,
    /// Where to resume from while held. `None` is not held.
    from: Option<Progress>,
}

impl Slot {
    fn grant(&self, from: Progress) {
        let mut grant = self.grant.lock().unwrap();
        grant.generation += 1;
        grant.from = Some(from);
        self.changed.notify_all();
    }

    fn revoke(&self) {
        let mut grant = self.grant.lock().unwrap();
        grant.generation += 1;
        grant.from = None;
        self.changed.notify_all();
    }

    fn is_held(&self) -> bool {
        self.grant.lock().unwrap().from.is_some()
    }

    fn generation(&self) -> u64 {
        self.grant.lock().unwrap().generation
    }
}

/// Every single-worker source this runtime deploys, by key, and the progress their commits have yielded and the
/// supervisor has not taken yet. One per runtime: the frame thread applies holds to it, and the report takes from it.
#[derive(Default)]
pub struct Held {
    slots: Mutex<HashMap<SourceKey, Arc<Slot>>>,
    outbox: Arc<Outbox>,
    /// The sequence number of the last holds frame applied, so the supervisor can tell a grant that failed from
    /// one this runtime has not reached yet.
    applied: AtomicU64,
}

impl Held {
    /// A source of this deployment read through a hold: `open` builds it each time it is granted.
    pub fn source(&self, key: SourceKey, open: Opener) -> HeldSource {
        let slot = self.slots.lock().unwrap().entry(key.clone()).or_default().clone();
        HeldSource {
            key,
            slot,
            open,
            inner: None,
            seen: 0,
            resume_from: None,
            touched: BTreeSet::new(),
            revoked: Vec::new(),
            keep_headers: false,
            name: None,
            blocked: false,
            outbox: self.outbox.clone(),
        }
    }

    /// Every single-worker source deployed here, for the leader to know which workers may hold it.
    pub fn keys(&self) -> Vec<SourceKey> {
        let mut keys: Vec<SourceKey> = self.slots.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Every single-worker source this runtime holds now. A source the supervisor named and this leaves out is
    /// one whose grant failed, once [`Held::applied`] has reached the frame that named it.
    pub fn holding(&self) -> Vec<SourceKey> {
        let mut keys: Vec<SourceKey> = self
            .slots
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, slot)| slot.is_held())
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        keys
    }

    /// The sequence number of the last holds frame applied.
    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    /// Follow the supervisor's list, frame `seq`, which is whole: grant each source it names that is not held,
    /// and revoke each held one it leaves out. A source already held is left reading, whatever the list says it
    /// resumes from: the leader's copy of its progress trails what it has read.
    ///
    /// A ledger file is read and removed. A grant comes with its ledger file; one without it, or whose file cannot
    /// be read, is not made: the source stays unheld rather than reading everything again.
    /// The supervisor sees it missing from [`Held::holding`] and offers it again, with the ledger fetched afresh.
    pub fn apply(&self, seq: u64, holdings: Vec<Holding>) {
        let slots = self.slots.lock().unwrap();
        let named: BTreeSet<&SourceKey> = holdings.iter().map(|h| &h.hold.key).collect();
        for (key, slot) in slots.iter() {
            if slot.is_held() && !named.contains(key) {
                slot.revoke();
            }
        }
        for holding in &holdings {
            // A ledger file is this runtime's to remove, whatever comes of the grant: nothing else reads it.
            let slot = match slots.get(&holding.hold.key) {
                Some(slot) if !slot.is_held() => slot,
                _ => {
                    holding.discard_ledger();
                    continue;
                }
            };
            let Some(file) = &holding.ledger_file else {
                continue;
            };
            match resume_point(&holding.hold, file) {
                Ok(from) => slot.grant(from),
                Err(e) => tracing::warn!(
                    target: "runtime.ledger_unreadable",
                    subject = %holding.hold.key.source,
                    error = %e,
                    "the ledger of a source this worker now holds could not be read, so it is not read until the \
                     supervisor fetches it again"
                ),
            }
        }
        self.applied.fetch_max(seq, Ordering::Relaxed);
    }

    /// What commits have yielded, oldest first, up to `entries` ledger entries and about `bytes` of JSON. See
    /// [`take_ledger`].
    pub fn take_progress(&self, entries: usize, bytes: usize) -> Vec<SourceProgress> {
        take_ledger(&mut self.outbox.pending.lock().unwrap(), entries, bytes)
    }

    /// Progress a report could not carry, ahead of whatever commits have yielded since.
    pub fn untake_progress(&self, unsent: Vec<SourceProgress>) {
        if unsent.is_empty() {
            return;
        }
        let mut outbox = self.outbox.pending.lock().unwrap();
        let since = std::mem::take(&mut *outbox);
        *outbox = unsent;
        outbox.extend(since);
    }

    /// Whether the supervisor's own backlog for the leader is past its bound. Held sources read nothing new while
    /// it is, as while this runtime's is.
    pub fn set_upstream_backlogged(&self, backlogged: bool) {
        self.outbox.upstream.store(backlogged, Ordering::Relaxed);
    }

    /// Whether held sources are reading nothing new for the backlog.
    pub fn paused(&self) -> bool {
        self.outbox.paused()
    }
}

/// How much progress, in [`backlog`]'s weight, may wait for the leader before held sources stop reading: eight
/// beats' worth. A leader briefly away, or a burst past one beat, costs nothing, and a long outage holds the
/// worker to about 90 MB of ledger at its longest entries, here and in the supervisor each, rather than all it
/// can read.
pub const BACKLOG_FULL: usize = 8 * crate::heartbeat::LEDGER_PER_HEARTBEAT;

/// Where a full backlog has drained far enough for reading to resume: half the bound, so a backlog hovering at the
/// bound does not stop and start the read on every report.
pub const BACKLOG_EASED: usize = BACKLOG_FULL / 2;

/// How much `pending` weighs against [`BACKLOG_FULL`]: its ledger entries, and one for each progress, so a source
/// that yields only cursors is bounded too.
pub fn backlog(pending: &[SourceProgress]) -> usize {
    pending
        .iter()
        .map(|p| 1 + p.progress.added.len() + p.progress.removed.len())
        .sum()
}

/// Whether a backlog of `weight` is full, given whether it `was`: full from [`BACKLOG_FULL`], until it eases to
/// [`BACKLOG_EASED`].
pub fn backlogged(was: bool, weight: usize) -> bool {
    if was {
        weight > BACKLOG_EASED
    } else {
        weight >= BACKLOG_FULL
    }
}

/// What commits have yielded and no report has carried yet, and whether held sources may read more.
#[derive(Default)]
struct Outbox {
    pending: Mutex<Vec<SourceProgress>>,
    /// This outbox is past its bound, moved on each check.
    full: AtomicBool,
    /// The supervisor's backlog is past its bound, as its last frame said.
    upstream: AtomicBool,
}

impl Outbox {
    fn paused(&self) -> bool {
        let weight = backlog(&self.pending.lock().unwrap());
        let was = self.full.load(Ordering::Relaxed);
        let now = backlogged(was, weight);
        // Said once per change, by whichever reader saw it first.
        if now != was
            && self
                .full
                .compare_exchange(was, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            if now {
                tracing::warn!(
                    target: "runtime.progress_backlogged",
                    weight,
                    "more progress is waiting for the supervisor than this runtime keeps, so held sources read \
                     nothing new until it drains"
                );
            } else {
                tracing::info!(
                    target: "runtime.progress_drained",
                    weight,
                    "the progress waiting for the supervisor has drained, so held sources read again"
                );
            }
        }
        now || self.upstream.load(Ordering::Relaxed)
    }
}

/// The cursor a hold names and the ledger written beside it, as one resume point. The file is removed once read:
/// a later grant fetches the ledger afresh, since this worker's own reports will have moved it.
fn resume_point(hold: &Hold, file: &std::path::Path) -> Result<Progress, String> {
    let mut from = Progress {
        cursor: hold.cursor.clone(),
        ..Default::default()
    };
    let read = read_ledger(file, &mut from.added);
    let _ = std::fs::remove_file(file);
    read.map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(from)
}

/// Each line of `file` into `into`, one at a time, so a large ledger is held once, parsed.
fn read_ledger(file: &std::path::Path, into: &mut Vec<LedgerEntry>) -> std::io::Result<()> {
    use std::io::BufRead;
    let mut lines = std::io::BufReader::new(std::fs::File::open(file)?);
    let mut line = String::new();
    loop {
        line.clear();
        if lines.read_line(&mut line)? == 0 {
            return Ok(());
        }
        if !line.trim().is_empty() {
            into.push(serde_json::from_str(&line).map_err(std::io::Error::other)?);
        }
    }
}

/// Takes from the front of `pending` up to `entries` ledger entries, added and removed, and about `bytes` of their
/// JSON. A progress that does not fit whole is split, and its cursor rides the remainder, since a cursor says
/// everything before it was delivered. A cursor counts toward the bytes and not the entries. The first entry is
/// always taken, whatever it weighs, so nothing in `pending` can hold back what is behind it.
pub fn take_ledger(pending: &mut Vec<SourceProgress>, entries: usize, bytes: usize) -> Vec<SourceProgress> {
    let mut entries_left = entries;
    let mut bytes_left = bytes;
    let mut whole = 0;
    let mut part = None;
    for p in pending.iter() {
        let taken_any = whole > 0;
        let head = json_len(&p.key) + p.progress.cursor.as_ref().map_or(0, json_len) + PROGRESS_FRAMING;
        if head > bytes_left && taken_any {
            break;
        }
        bytes_left = bytes_left.saturating_sub(head);
        let mut fits = |weight: usize, first: bool| {
            if entries_left == 0 || (weight > bytes_left && !first) {
                return false;
            }
            entries_left -= 1;
            bytes_left = bytes_left.saturating_sub(weight);
            true
        };
        let removed = p
            .progress
            .removed
            .iter()
            .enumerate()
            .take_while(|(i, r)| fits(json_len(r) + 1, !taken_any && *i == 0))
            .count();
        let added = if removed == p.progress.removed.len() {
            p.progress
                .added
                .iter()
                .enumerate()
                .take_while(|(i, a)| fits(json_len(a) + 1, !taken_any && removed == 0 && *i == 0))
                .count()
        } else {
            0
        };
        if removed == p.progress.removed.len() && added == p.progress.added.len() {
            whole += 1;
            continue;
        }
        part = Some((removed, added));
        break;
    }
    let mut taken: Vec<SourceProgress> = pending.drain(..whole).collect();
    if let Some((removed, added)) = part.filter(|&(r, a)| r + a > 0)
        && let Some(next) = pending.first_mut()
    {
        taken.push(SourceProgress {
            key: next.key.clone(),
            progress: Progress {
                cursor: None,
                removed: next.progress.removed.drain(..removed).collect(),
                added: next.progress.added.drain(..added).collect(),
            },
        });
    }
    taken
}

/// What one progress costs on the wire beyond its key, its cursor and its entries: field names and punctuation.
const PROGRESS_FRAMING: usize = 64;

fn json_len(value: &impl serde::Serialize) -> usize {
    serde_json::to_vec(value).map_or(0, |v| v.len())
}

/// `progress` without what is over the limits the leader keeps a ledger to, which it would refuse anyway. Dropped
/// here so no report carries more than those limits allow: what is dropped is read again by the next holder,
/// duplicates and never lost rows.
fn admitted(key: &SourceKey, mut progress: Progress) -> Progress {
    let (cursor_refused, refused) = progress.admit();
    if cursor_refused {
        tracing::warn!(
            target: "runtime.progress_refused",
            subject = %key.source,
            "a cursor longer than {MAX_CURSOR} bytes was refused, so the source resumes from the one before it"
        );
    }
    if refused > 0 {
        tracing::warn!(
            target: "runtime.progress_refused",
            subject = %key.source,
            refused,
            "ledger entries over {MAX_LEDGER_ITEM} bytes of item or {MAX_LEDGER_ENTRY} of entry were refused, so \
             their items are read again by the next holder"
        );
    }
    progress
}

/// Builds the source a hold reads, each time it is granted.
pub type Opener = Box<dyn FnMut() -> Result<Box<dyn Source>, IoError> + Send>;

/// A single-worker source, read only while its [`Slot`] says it is held. Unheld, a poll waits out its timeout and
/// returns nothing, and a commit is ignored.
pub struct HeldSource {
    key: SourceKey,
    slot: Arc<Slot>,
    open: Opener,
    inner: Option<Box<dyn Source>>,
    /// The slot's generation this has acted on.
    seen: u64,
    /// A grant not yet opened.
    resume_from: Option<Progress>,
    /// Every partition the open source has handed a record from.
    touched: BTreeSet<TopicPartition>,
    /// Partitions of a source dropped since the reader last asked. Their batches must never commit, here or in the
    /// source a regrant opens.
    revoked: Vec<TopicPartition>,
    keep_headers: bool,
    blocked: bool,
    /// The id the author gave the source, handed to each source a grant opens.
    name: Option<String>,
    outbox: Arc<Outbox>,
}

impl HeldSource {
    /// Act on a grant or revoke the slot has had since the last call.
    fn follow(&mut self) {
        let grant = self.slot.grant.lock().unwrap();
        if grant.generation == self.seen {
            return;
        }
        self.seen = grant.generation;
        self.resume_from = grant.from.clone();
        drop(grant);
        if self.inner.take().is_some() {
            self.revoked.extend(std::mem::take(&mut self.touched));
        }
    }

    /// Open a grant, once the reader has taken what the last source revoked: a batch the reader still tracks from
    /// it must not commit against a partition the new one reads.
    fn open_granted(&mut self) -> Result<(), IoError> {
        if self.inner.is_some() || !self.revoked.is_empty() {
            return Ok(());
        }
        let Some(from) = self.resume_from.clone() else {
            return Ok(());
        };
        let mut source = (self.open)()?;
        source.resume(from)?;
        source.keep_headers(self.keep_headers);
        if let Some(name) = &self.name {
            source.named(name);
        }
        if self.blocked {
            source.block()?;
        }
        self.resume_from = None;
        self.inner = Some(source);
        Ok(())
    }
}

impl HeldSource {
    /// Moves what the open source has delivered since it was last asked into the outbox for the leader.
    fn hand_over_progress(&mut self) {
        let Some(progress) = self.inner.as_mut().and_then(|s| s.take_progress()) else {
            return;
        };
        let progress = admitted(&self.key, progress);
        self.outbox.pending.lock().unwrap().push(SourceProgress {
            key: self.key.clone(),
            progress,
        });
    }
}

impl Source for HeldSource {
    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        self.follow();
        self.open_granted()?;
        // Unheld, or held with the backlog full: read nothing, waking for a grant or a revoke rather than sleeping
        // through it. What was read already still commits.
        let Some(inner) = self.inner.as_mut().filter(|_| !self.outbox.paused()) else {
            let grant = self.slot.grant.lock().unwrap();
            let seen = self.seen;
            let _ = self
                .slot
                .changed
                .wait_timeout_while(grant, timeout, |g| g.generation == seen)
                .unwrap();
            return Ok(Vec::new());
        };
        let records = inner.poll_batch(max_rows, max_bytes, timeout)?;
        // A poll can make progress with nothing to commit: a listing's first, which marks where a start from new
        // begins, must reach the leader before a failover can lose it.
        self.hand_over_progress();
        for r in &records {
            self.touched.insert(TopicPartition::new(r.topic.clone(), r.partition));
        }
        Ok(records)
    }

    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        // Read from a source since dropped: whoever holds it now reads these again.
        if self.slot.generation() != self.seen {
            return Ok(());
        }
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        inner.commit(offsets)?;
        self.hand_over_progress();
        Ok(())
    }

    fn block(&mut self) -> Result<(), IoError> {
        self.blocked = true;
        self.inner.as_mut().map_or(Ok(()), |s| s.block())
    }

    fn release(&mut self) -> Result<(), IoError> {
        self.blocked = false;
        self.inner.as_mut().map_or(Ok(()), |s| s.release())
    }

    fn take_dropped(&mut self) -> u64 {
        self.inner.as_mut().map_or(0, |s| s.take_dropped())
    }

    fn take_refused(&mut self) -> Vec<(&'static str, u64)> {
        self.inner.as_mut().map_or_else(Vec::new, |s| s.take_refused())
    }

    fn counts_messages(&self) -> bool {
        self.inner.as_ref().is_some_and(|s| s.counts_messages())
    }

    /// None of it while this worker does not hold the source.
    fn held_bytes(&self) -> Option<u64> {
        Some(self.inner.as_ref().and_then(|s| s.held_bytes()).unwrap_or(0))
    }

    fn take_finished(&mut self) -> u64 {
        self.inner.as_mut().map_or(0, |s| s.take_finished())
    }

    fn named(&mut self, name: &str) {
        self.name = Some(name.to_string());
        if let Some(s) = self.inner.as_mut() {
            s.named(name);
        }
    }

    fn keep_headers(&mut self, keep: bool) {
        self.keep_headers = keep;
        if let Some(s) = self.inner.as_mut() {
            s.keep_headers(keep);
        }
    }

    fn take_revoked(&mut self) -> Vec<TopicPartition> {
        self.follow();
        let mut revoked = std::mem::take(&mut self.revoked);
        if let Some(s) = self.inner.as_mut() {
            revoked.extend(s.take_revoked());
        }
        revoked
    }

    fn keep_membership(&mut self, timeout: Duration) -> Result<(), IoError> {
        self.inner.as_mut().map_or(Ok(()), |s| s.keep_membership(timeout))
    }

    fn assigned_partitions(&self) -> Vec<(String, i32)> {
        self.inner.as_ref().map(|s| s.assigned_partitions()).unwrap_or_default()
    }

    fn lag(&mut self) -> Vec<PartitionLag> {
        self.inner.as_mut().map(|s| s.lag()).unwrap_or_default()
    }

    fn connection_error(&self) -> Option<String> {
        self.inner.as_ref().and_then(|s| s.connection_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::fake::FakeSource;

    /// A fake that yields progress on commit and records where it was resumed from. Each open is a new one, as a
    /// real source would be, sharing what it was resumed from with the test.
    #[derive(Clone, Default)]
    struct Ledgered {
        records: FakeSource,
        resumed: Arc<Mutex<Vec<Progress>>>,
    }

    impl Source for Ledgered {
        fn poll_batch(&mut self, rows: usize, bytes: usize, timeout: Duration) -> Result<Vec<SourceRecord>, IoError> {
            self.records.poll_batch(rows, bytes, timeout)
        }

        fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
            self.records.commit(offsets)
        }

        fn take_progress(&mut self) -> Option<Progress> {
            let committed = self.records.committed();
            let last = committed.last()?;
            let (_, offset) = last.iter().next()?;
            Some(Progress {
                cursor: Some(offset.to_string()),
                added: vec![LedgerEntry {
                    item: format!("item-{offset}"),
                    entry: "read".to_string(),
                }],
                removed: vec![],
            })
        }

        fn resume(&mut self, from: Progress) -> Result<(), IoError> {
            self.resumed.lock().unwrap().push(from);
            Ok(())
        }

        fn held_bytes(&self) -> Option<u64> {
            Some(3)
        }
    }

    fn record(offset: i64) -> SourceRecord {
        SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(format!("r{offset}").into_bytes()),
            row: 0,
            offset,
            partition: 0,
            topic: Arc::from("listing"),
            headers: None,
        }
    }

    fn key() -> SourceKey {
        SourceKey {
            source: "bucket".to_string(),
            place: "s3://logs/a/".to_string(),
        }
    }

    /// A grant from `cursor` with an empty ledger file of its own, which the grant removes once read.
    fn hold(cursor: Option<&str>) -> Holding {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let file = std::env::temp_dir().join(format!(
            "smeltr-held-empty-{}-{}.ndjson",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&file, "").unwrap();
        Holding {
            hold: Hold {
                key: key(),
                cursor: cursor.map(str::to_string),
            },
            ledger_file: Some(file),
        }
    }

    /// A held source over `fake`, and how many times it has been opened.
    fn held_over(held: &Held, fake: &Ledgered) -> (HeldSource, Arc<Mutex<usize>>) {
        let opened = Arc::new(Mutex::new(0));
        let source = held.source(key(), {
            let (fake, opened) = (fake.clone(), opened.clone());
            Box::new(move || {
                *opened.lock().unwrap() += 1;
                Ok(Box::new(fake.clone()) as Box<dyn Source>)
            })
        });
        (source, opened)
    }

    fn poll(source: &mut HeldSource) -> Vec<i64> {
        source
            .poll_batch(10, 1 << 20, Duration::from_millis(5))
            .expect("a poll")
            .iter()
            .map(|r| r.offset)
            .collect()
    }

    fn offsets(offset: i64) -> CommitMap {
        CommitMap::from([(TopicPartition::new(Arc::from("listing"), 0), offset)])
    }

    #[test]
    fn progress_made_by_a_poll_reaches_the_outbox_without_a_commit() {
        /// A source whose first poll makes progress and reads nothing, as a listing's first does.
        struct Marks(bool);
        impl Source for Marks {
            fn poll_batch(&mut self, _: usize, _: usize, _: Duration) -> Result<Vec<SourceRecord>, IoError> {
                Ok(Vec::new())
            }
            fn commit(&mut self, _: &CommitMap) -> Result<(), IoError> {
                Ok(())
            }
            fn take_progress(&mut self) -> Option<Progress> {
                std::mem::take(&mut self.0).then(|| Progress {
                    cursor: Some("listed".to_string()),
                    ..Progress::default()
                })
            }
        }
        let held = Held::default();
        let mut source = held.source(key(), Box::new(|| Ok(Box::new(Marks(true)) as Box<dyn Source>)));
        held.apply(1, vec![hold(None)]);

        assert!(poll(&mut source).is_empty());
        let sent = held.take_progress(usize::MAX, usize::MAX);
        assert_eq!(
            sent.iter().map(|p| p.progress.cursor.as_deref()).collect::<Vec<_>>(),
            [Some("listed")],
            "a poll's progress waited for a commit"
        );
    }

    #[test]
    fn a_source_not_held_reads_nothing_and_commits_nothing() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1)]),
            ..Default::default()
        };
        let (mut source, opened) = held_over(&held, &fake);

        assert!(poll(&mut source).is_empty(), "an unheld source was read");
        source.commit(&offsets(1)).expect("an ignored commit");
        assert_eq!(*opened.lock().unwrap(), 0, "an unheld source was opened");
        assert!(fake.records.committed().is_empty());
        assert_eq!(
            held.keys(),
            vec![key()],
            "a deployed source was not offered for holding"
        );
    }

    #[test]
    fn a_grant_opens_the_source_and_resumes_it_from_the_grant() {
        let dir = std::env::temp_dir().join(format!("smeltr-held-grant-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("ledger.ndjson");
        std::fs::write(
            &file,
            "{\"item\":\"a.log\",\"entry\":\"v1\"}\n{\"item\":\"b.log\",\"entry\":\"v2\"}\n",
        )
        .unwrap();

        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1), record(2)]),
            ..Default::default()
        };
        let (mut source, opened) = held_over(&held, &fake);
        let mut grant = hold(Some("c7"));
        let _ = std::fs::remove_file(grant.ledger_file.replace(file.clone()).unwrap());
        held.apply(1, vec![grant]);

        assert_eq!(poll(&mut source), vec![1, 2]);
        assert_eq!(*opened.lock().unwrap(), 1);
        let resumed = fake.resumed.lock().unwrap().clone();
        assert_eq!(resumed.len(), 1, "{resumed:?}");
        assert_eq!(resumed[0].cursor.as_deref(), Some("c7"));
        let items: Vec<&str> = resumed[0].added.iter().map(|e| e.item.as_str()).collect();
        assert_eq!(items, ["a.log", "b.log"], "the ledger was not what it resumed from");
        assert!(!file.exists(), "a ledger file read was left behind");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_held_source_reports_what_its_source_holds_once_open() {
        let held = Held::default();
        let (mut source, _) = held_over(&held, &Ledgered::default());
        assert_eq!(source.held_bytes(), Some(0));
        held.apply(1, vec![hold(None)]);
        poll(&mut source);
        assert_eq!(source.held_bytes(), Some(3));
    }

    #[test]
    fn progress_a_commit_yields_reaches_the_outbox() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1)]),
            ..Default::default()
        };
        let (mut source, _) = held_over(&held, &fake);
        held.apply(1, vec![hold(None)]);
        assert_eq!(poll(&mut source), vec![1]);

        source.commit(&offsets(1)).expect("a commit");
        let sent = held.take_progress(usize::MAX, usize::MAX);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!(sent[0].key, key());
        assert_eq!(sent[0].progress.cursor.as_deref(), Some("1"));
        assert!(
            held.take_progress(usize::MAX, usize::MAX).is_empty(),
            "progress was offered twice"
        );
    }

    /// A revoke drops the source with a batch in flight. That batch never commits -- whoever holds the source now
    /// reads it again -- and the reader is told its partitions are gone.
    #[test]
    fn a_revoke_stops_the_read_and_ignores_what_was_in_flight() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1), record(2)]),
            ..Default::default()
        };
        let (mut source, _) = held_over(&held, &fake);
        held.apply(1, vec![hold(None)]);
        assert_eq!(poll(&mut source), vec![1, 2]);

        held.apply(1, vec![]);
        source.commit(&offsets(2)).expect("an ignored commit");
        assert!(
            fake.records.committed().is_empty(),
            "a batch read before the revoke committed"
        );
        assert_eq!(
            source.take_revoked(),
            vec![TopicPartition::new(Arc::from("listing"), 0)],
            "the reader was not told to forget what it still tracks"
        );
        fake.records.requeue([record(3)]);
        assert!(poll(&mut source).is_empty(), "a revoked source was still read");
        assert!(held.take_progress(usize::MAX, usize::MAX).is_empty());
    }

    /// Held again, the source is opened afresh from the new grant -- but only once the reader has forgotten the
    /// batches of the one it replaces, which read the same partitions.
    #[test]
    fn a_regrant_reopens_from_the_new_grant() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1)]),
            ..Default::default()
        };
        let (mut source, opened) = held_over(&held, &fake);
        held.apply(1, vec![hold(Some("first"))]);
        assert_eq!(poll(&mut source), vec![1]);

        held.apply(1, vec![]);
        held.apply(1, vec![hold(Some("second"))]);
        fake.records.requeue([record(2)]);
        assert!(
            poll(&mut source).is_empty(),
            "a regrant opened before the reader forgot the dropped source's batches"
        );
        assert_eq!(source.take_revoked().len(), 1);
        assert_eq!(poll(&mut source), vec![2]);
        assert_eq!(*opened.lock().unwrap(), 2);
        let cursors: Vec<Option<String>> = fake.resumed.lock().unwrap().iter().map(|p| p.cursor.clone()).collect();
        assert_eq!(cursors, [Some("first".to_string()), Some("second".to_string())]);
    }

    /// A source already held keeps reading when the list names it again: the leader's copy of its progress trails
    /// what it has read, and reopening there would read it again.
    #[test]
    fn a_hold_named_again_is_left_reading() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1)]),
            ..Default::default()
        };
        let (mut source, opened) = held_over(&held, &fake);
        held.apply(1, vec![hold(Some("first"))]);
        assert_eq!(poll(&mut source), vec![1]);
        held.apply(1, vec![hold(Some("later"))]);
        fake.records.requeue([record(2)]);
        assert_eq!(poll(&mut source), vec![2]);
        assert_eq!(*opened.lock().unwrap(), 1, "a hold named again was reopened");
    }

    fn progress(added: usize, removed: usize, cursor: Option<&str>) -> SourceProgress {
        SourceProgress {
            key: key(),
            progress: Progress {
                cursor: cursor.map(str::to_string),
                added: (0..added)
                    .map(|i| LedgerEntry {
                        item: format!("a{i}"),
                        entry: String::new(),
                    })
                    .collect(),
                removed: (0..removed).map(|i| format!("r{i}")).collect(),
            },
        }
    }

    /// Oldest first up to the cap. A progress straddling it is split, its cursor left with the rest, which is the
    /// part that completes it.
    #[test]
    fn taking_the_ledger_stops_at_the_cap_and_splits_what_straddles_it() {
        let mut pending = vec![
            progress(2, 0, Some("c1")),
            progress(3, 2, Some("c2")),
            progress(1, 0, None),
        ];
        let taken = take_ledger(&mut pending, 5, usize::MAX);
        assert_eq!(taken.len(), 2, "{taken:?}");
        assert_eq!(taken[0], progress(2, 0, Some("c1")));
        assert_eq!(taken[1].progress.removed, ["r0", "r1"]);
        assert_eq!(taken[1].progress.added.len(), 1);
        assert_eq!(taken[1].progress.cursor, None, "a cursor went ahead of what it covers");

        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].progress.added.len(), 2);
        assert!(pending[0].progress.removed.is_empty());
        assert_eq!(pending[0].progress.cursor.as_deref(), Some("c2"));

        // A cursor alone counts no entries, so a full beat still carries one.
        let mut cursors = vec![progress(0, 0, Some("c3"))];
        assert_eq!(take_ledger(&mut cursors, 0, usize::MAX).len(), 1);
    }

    /// Bytes bind as entries do. However small the cap, the first entry goes, so nothing in the queue can hold back
    /// what is behind it.
    #[test]
    fn taking_the_ledger_stops_at_its_bytes_and_always_takes_one_entry() {
        let big = |n: usize| SourceProgress {
            key: key(),
            progress: Progress {
                cursor: Some("c".to_string()),
                added: (0..n)
                    .map(|i| LedgerEntry {
                        item: format!("{i:0>1000}"),
                        entry: "e".repeat(200),
                    })
                    .collect(),
                removed: vec![],
            },
        };
        let mut pending = vec![big(10)];
        let taken = take_ledger(&mut pending, usize::MAX, 5_000);
        let carried: usize = taken.iter().map(|p| p.progress.added.len()).sum();
        assert!((1..10).contains(&carried), "{carried} entries of about 1.2 KB fit 5 KB");
        assert!(json_len(&taken) <= 5_000 + 1_300, "{} bytes", json_len(&taken));
        assert_eq!(pending[0].progress.added.len(), 10 - carried);

        let mut pending = vec![big(2)];
        let taken = take_ledger(&mut pending, usize::MAX, 1);
        assert_eq!(taken.iter().map(|p| p.progress.added.len()).sum::<usize>(), 1);
    }

    /// What is over the leader's limits never leaves this runtime: a report stays inside its frame, and the
    /// leader would refuse it anyway.
    #[test]
    fn progress_over_the_ledger_limits_is_dropped_before_it_is_reported() {
        let mut progress = progress(0, 0, Some(&"c".repeat(MAX_CURSOR + 1))).progress;
        progress.added = vec![
            LedgerEntry {
                item: "fits".to_string(),
                entry: "e".repeat(MAX_LEDGER_ENTRY),
            },
            LedgerEntry {
                item: "i".repeat(MAX_LEDGER_ITEM + 1),
                entry: String::new(),
            },
            LedgerEntry {
                item: "long".to_string(),
                entry: "e".repeat(MAX_LEDGER_ENTRY + 1),
            },
        ];
        progress.removed = vec!["gone".to_string(), "r".repeat(MAX_LEDGER_ITEM + 1)];
        let kept = admitted(&key(), progress);
        assert_eq!(kept.cursor, None);
        let items: Vec<&str> = kept.added.iter().map(|e| e.item.as_str()).collect();
        assert_eq!(items, ["fits"]);
        assert_eq!(kept.removed, ["gone"]);
    }

    /// A grant is made only with its ledger. Without one, or with one that cannot be read, the source stays
    /// unheld, and the runtime says so once it has applied the frame, for the supervisor to offer it again.
    #[test]
    fn a_grant_without_its_ledger_is_not_made() {
        let held = Held::default();
        let fake = Ledgered::default();
        let (mut source, opened) = held_over(&held, &fake);
        let with_ledger = |file: Option<std::path::PathBuf>| Holding {
            hold: Hold {
                key: key(),
                cursor: None,
            },
            ledger_file: file,
        };

        held.apply(4, vec![with_ledger(None)]);
        assert_eq!(held.applied(), 4);
        assert!(
            held.holding().is_empty(),
            "a source whose ledger was not handed over was granted"
        );

        held.apply(
            5,
            vec![with_ledger(Some(std::path::PathBuf::from(
                "/nonexistent/ledger.ndjson",
            )))],
        );
        assert_eq!(held.applied(), 5);
        assert!(
            held.holding().is_empty(),
            "a source whose ledger could not be read was granted"
        );
        assert!(poll(&mut source).is_empty());
        assert_eq!(*opened.lock().unwrap(), 0);

        held.apply(6, vec![hold(None)]);
        assert_eq!(held.holding(), vec![key()]);
    }

    /// Every ledger file a frame hands over is removed, whether or not it grants anything: one for a source this
    /// runtime does not deploy, and one for a source it already holds, included.
    #[test]
    fn a_ledger_file_is_removed_whatever_the_grant_comes_to() {
        let held = Held::default();
        let fake = Ledgered::default();
        let (_source, _) = held_over(&held, &fake);
        held.apply(1, vec![hold(None)]);

        let again = hold(Some("again"));
        let mut elsewhere = hold(None);
        elsewhere.hold.key.source = "not-deployed".to_string();
        let files: Vec<std::path::PathBuf> = [&again, &elsewhere]
            .iter()
            .filter_map(|h| h.ledger_file.clone())
            .collect();
        held.apply(2, vec![again, elsewhere]);
        for file in files {
            assert!(!file.exists(), "{} was left behind", file.display());
        }
    }

    /// A ledger is parsed a line at a time, blank lines skipped, and a line that is not an entry refuses the grant.
    #[test]
    fn a_ledger_is_read_line_by_line_and_refused_whole_on_a_bad_line() {
        let dir = std::env::temp_dir().join(format!("smeltr-held-lines-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good.ndjson");
        let lines: String = (0..1000)
            .map(|i| format!("{{\"item\":\"i{i}\",\"entry\":\"e\"}}\n\n"))
            .collect();
        std::fs::write(&good, lines).unwrap();
        let mut read = Vec::new();
        read_ledger(&good, &mut read).expect("a ledger reads");
        assert_eq!(read.len(), 1000);
        assert_eq!(read[999].item, "i999");

        let held = Held::default();
        let fake = Ledgered::default();
        let (_source, _) = held_over(&held, &fake);
        let bad = dir.join("bad.ndjson");
        std::fs::write(&bad, "{\"item\":\"a\",\"entry\":\"e\"}\nnot an entry\n").unwrap();
        let mut holding = hold(None);
        holding.ledger_file = Some(bad.clone());
        held.apply(1, vec![holding]);
        assert!(held.holding().is_empty(), "a ledger with a bad line was granted");
        assert!(!bad.exists(), "a refused ledger was left behind");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Past the bound, held sources read nothing new while what they read still commits and reports; they read
    /// again only once the backlog has eased to the lower mark, and while the supervisor says its own is full.
    #[test]
    fn a_full_backlog_stops_the_read_until_it_eases() {
        let held = Held::default();
        let fake = Ledgered {
            records: FakeSource::new([record(1)]),
            ..Default::default()
        };
        let (mut source, _) = held_over(&held, &fake);
        held.apply(1, vec![hold(None)]);
        assert_eq!(poll(&mut source), vec![1]);

        let weighing = |entries: usize| SourceProgress {
            key: key(),
            progress: Progress {
                cursor: None,
                added: (0..entries)
                    .map(|i| LedgerEntry {
                        item: i.to_string(),
                        entry: String::new(),
                    })
                    .collect(),
                removed: vec![],
            },
        };
        held.untake_progress(vec![weighing(BACKLOG_FULL)]);
        fake.records.requeue([record(2)]);
        assert!(poll(&mut source).is_empty(), "a full backlog still read");
        assert!(held.paused());
        source.commit(&offsets(1)).expect("a commit");
        assert_eq!(fake.records.committed().len(), 1, "a full backlog stopped a commit");

        // Taken down to between the marks: still paused, so a backlog at the bound does not flap.
        let _ = held.take_progress(BACKLOG_FULL - BACKLOG_EASED - 10, usize::MAX);
        assert!(poll(&mut source).is_empty(), "the read resumed above the lower mark");
        let _ = held.take_progress(usize::MAX, usize::MAX);
        assert_eq!(poll(&mut source), vec![2], "an eased backlog did not resume the read");

        held.set_upstream_backlogged(true);
        fake.records.requeue([record(3)]);
        assert!(
            poll(&mut source).is_empty(),
            "the supervisor's full backlog did not stop the read"
        );
        held.set_upstream_backlogged(false);
        assert_eq!(poll(&mut source), vec![3]);
    }

    #[test]
    fn a_backlog_is_full_from_the_bound_until_it_eases() {
        assert!(!backlogged(false, BACKLOG_FULL - 1));
        assert!(backlogged(false, BACKLOG_FULL));
        assert!(backlogged(true, BACKLOG_EASED + 1));
        assert!(!backlogged(true, BACKLOG_EASED));
        let cursors = vec![
            SourceProgress {
                key: key(),
                progress: Progress::default(),
            };
            3
        ];
        assert_eq!(backlog(&cursors), 3, "progress that is only a cursor weighed nothing");
    }
}
