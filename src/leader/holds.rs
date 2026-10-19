//! Who reads each single-worker source: one live worker of the group, for as long as it stays live.
//!
//! Every worker of a group runs the same bundle, so each lists the same single-worker sources on its heartbeat. On
//! each beat the leader settles that worker's share: a source it already holds stays with it, a source another live
//! worker holds stays with that one, and a free source goes to whichever worker listing it holds fewest. Who holds
//! what is kept in `leader.db` (`source_holds`), so a restart keeps every hold where it was. Who is live and what each
//! lists is only in memory, rebuilt by the next round of beats.
//!
//! A holder is live while it has beaten, and listed the source, within the hold time, so a redeploy that briefly
//! runs nothing keeps its holds. Past that the source is free, and the next worker to beat that lists it takes it
//! from the last progress stored. A holder that was only slow reads on until its next answer leaves the source out,
//! which is duplicates, never lost rows. A beat that says the worker is stopping has its progress kept, then frees
//! every hold it had.
//!
//! ```mermaid
//! flowchart LR
//!   B[beat from W: sources it runs, progress] --> A[progress kept for every source the store names W holder of]
//!   A --> S{stored holder}
//!   S -- W --> H[W holds it]
//!   S -- another worker, heard and listing it within the hold time --> N[W does not]
//!   S -- none, or one past the hold time --> F{a live worker listing it holds fewer?}
//!   F -- no --> G[give it to W] --> H
//!   F -- yes --> L[left for that worker's beat]
//!   H --> P[answered with its cursor]
//! ```

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::heartbeat::{Hold, MAX_CURSOR, MAX_LEDGER_ENTRY, MAX_LEDGER_ITEM, SourceKey, SourceProgress};
use crate::io::Progress;

use super::store::{HeldSource, Store, StoreError};

/// Who each worker is and what it last said, and which holds it was last answered with.
#[derive(Default)]
pub struct Holds(Mutex<Workers>);

#[derive(Default)]
struct Workers {
    heard: HashMap<String, Heard>,
    /// Workers forgotten, revoked or stopped since this leader started. Only asked while the leader is younger than
    /// the hold time, when a stored holder it has not heard from yet still counts as live; one of these is not.
    gone: HashSet<String>,
}

struct Heard {
    group: String,
    at: Instant,
    /// When this worker last listed each source, within the hold time. A holder keeps a source it stops listing
    /// for that long, so a redeploy or a runtime restart between two beats does not free its holds.
    listed: HashMap<SourceKey, Instant>,
    /// The holds this worker was last answered with.
    answered: Vec<Hold>,
    /// How long this worker may go unheard, or not list a source, before what it holds is free: the leader's hold
    /// time, or three of the worker's own intervals when it beats less often than the leader expects.
    hold_time: Duration,
}

impl Heard {
    fn within(&self, clock: Clock, at: Instant) -> bool {
        clock.now.saturating_duration_since(at) <= self.hold_time
    }
}

/// What this leader has heard from a stored holder ([`Holds::seen`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seen {
    /// Answered with the hold, this long ago.
    Answered(Duration),
    /// Not heard from since this leader started.
    Unheard,
}

/// What one heartbeat says about single-worker sources.
pub struct Beat<'a> {
    pub worker: &'a str,
    pub group: &'a str,
    /// Every single-worker source the worker's deployment runs: the ones it may be given.
    pub runs: Vec<SourceKey>,
    pub progress: Vec<SourceProgress>,
    /// The worker is going. Its progress is recorded, it is given nothing, and what it held is free.
    pub stopping: bool,
    /// How often the worker says it beats. `None` from a worker older than the field.
    pub interval: Option<Duration>,
}

/// How long a leader is up before it forgets progress: longer than any worker's heartbeat interval.
pub const FORGET_AFTER_UP: Duration = Duration::from_secs(3600);

/// The times one beat is judged by.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    pub now: Instant,
    /// How long this leader has been up. A holder it has not heard from yet counts as live until the hold time has
    /// passed since it started, so a restart never gives a source to a second worker while its holder still reads.
    pub up_for: Duration,
    /// How long a holder may go unheard, or not list a source, before the source is free.
    pub hold_time: Duration,
}

impl Clock {
    /// Now, for a leader up for `up_for`, holding a source for three heartbeat intervals: two missed beats are
    /// a slow worker, three are a gone one.
    pub fn at(up_for: Duration) -> Clock {
        Clock {
            now: Instant::now(),
            up_for,
            hold_time: 3 * crate::startup_config::worker_heartbeat_interval(),
        }
    }

    fn young(&self) -> bool {
        self.up_for <= self.hold_time
    }
}

impl Workers {
    /// Whether `holder` still reads `key` in `group`: heard within the hold time, and listed it within the hold
    /// time.
    fn reading(&self, holder: &str, group: &str, key: &SourceKey, clock: Clock) -> bool {
        match self.heard.get(holder) {
            Some(w) => {
                w.group == group && w.within(clock, w.at) && w.listed.get(key).is_some_and(|&at| w.within(clock, at))
            }
            None => clock.young() && !self.gone.contains(holder),
        }
    }
}

impl Holds {
    /// Records the progress `beat` reports for every source the store names its worker the holder of, settles
    /// which of the sources it runs it holds, and answers those with where each resumes. Progress for a source it
    /// does not hold is a former holder's late report and is dropped.
    ///
    /// An error is a beat whose progress may not all be recorded: the worker sends it again on its next beat.
    ///
    /// Serialised across workers, so two beats never both take one free source. Blocking: it reads and writes the
    /// store under the lock every other call here takes.
    pub fn beat(&self, store: &dyn Store, mut beat: Beat<'_>, clock: Clock) -> Result<Vec<Hold>, StoreError> {
        let mut workers = self.lock();
        if !clock.young() {
            workers.gone.clear();
        }
        let answered = settle(&workers, store, &mut beat, clock)?;
        let Beat {
            worker,
            group,
            runs,
            interval,
            ..
        } = beat;
        if beat.stopping {
            workers.heard.remove(worker);
            workers.gone.insert(worker.to_string());
            return Ok(answered);
        }
        let heard = workers.heard.entry(worker.to_string()).or_insert_with(|| Heard {
            group: group.to_string(),
            at: clock.now,
            listed: HashMap::new(),
            answered: Vec::new(),
            hold_time: clock.hold_time,
        });
        heard.hold_time = interval.map_or(clock.hold_time, |i| clock.hold_time.max(3 * i));
        if heard.group != group {
            heard.group = group.to_string();
            heard.listed.clear();
        }
        heard.at = clock.now;
        let hold_time = heard.hold_time;
        heard
            .listed
            .retain(|_, &mut at| clock.now.saturating_duration_since(at) <= hold_time);
        heard.listed.extend(runs.into_iter().map(|key| (key, clock.now)));
        heard.answered = answered.clone();
        Ok(answered)
    }

    /// The group `worker` holds `key` in, by its last answer. The store still has the last word: a hold can move
    /// while its former holder is silent. Blocking: it waits on any beat in progress.
    pub fn held_in(&self, worker: &str, key: &SourceKey) -> Option<String> {
        let workers = self.lock();
        let heard = workers.heard.get(worker)?;
        heard
            .answered
            .iter()
            .any(|h| &h.key == key)
            .then(|| heard.group.clone())
    }

    /// The worker of `group` whose last answer gave it the source named `source`. Blocking: it waits on any beat in
    /// progress.
    pub fn holder(&self, group: &str, source: &str) -> Option<String> {
        let workers = self.lock();
        workers
            .heard
            .iter()
            .find(|(_, heard)| heard.group == group && heard.answered.iter().any(|h| h.key.source == source))
            .map(|(worker, _)| worker.clone())
    }

    /// How the map may show `worker` as the holder of `key` in `group`, a hold the store keeps: `Answered` when its
    /// last answer named that hold, with how long ago it was heard; `Unheard` when this leader has not heard from it
    /// yet and is young enough that it still counts as live. `None` otherwise: the hold is at a place its source no
    /// longer reads, or its holder is gone. Blocking: it waits on any beat in progress.
    pub fn seen(&self, worker: &str, group: &str, key: &SourceKey, clock: Clock) -> Option<Seen> {
        let workers = self.lock();
        match workers.heard.get(worker) {
            Some(heard) => (heard.group == group && heard.answered.iter().any(|h| &h.key == key))
                .then(|| Seen::Answered(clock.now.saturating_duration_since(heard.at))),
            None => (clock.young() && !workers.gone.contains(worker)).then_some(Seen::Unheard),
        }
    }

    /// Drops `worker`, forgotten or revoked, so the sources it held are free on the next beat that lists them
    /// rather than after the hold time. Blocking: it waits on any beat in progress.
    pub fn forget(&self, worker: &str) {
        let mut workers = self.lock();
        workers.heard.remove(worker);
        workers.gone.insert(worker.to_string());
    }

    /// Forgets the hold and progress of every source the store keeps that is not in `read` and that no worker heard
    /// within its hold time listed: nothing reads it, so nothing would resume from it. Returns what was forgotten.
    ///
    /// Nothing until the leader has been up for [`FORGET_AFTER_UP`], since a worker it has not heard from yet may still
    /// be reading, and one that beats slower than this leader's own interval is not known until it beats. Under the
    /// lock beats take, so no beat records progress for a source while it is being forgotten.
    /// Blocking.
    pub fn forget_unread(
        &self,
        store: &dyn Store,
        read: &HashSet<(String, SourceKey)>,
        clock: Clock,
    ) -> Result<Vec<HeldSource>, StoreError> {
        let workers = self.lock();
        if clock.up_for <= clock.hold_time.max(FORGET_AFTER_UP) {
            return Ok(Vec::new());
        }
        let listed = |group: &str, key: &SourceKey| {
            workers.heard.values().any(|w| {
                w.group == group && w.within(clock, w.at) && w.listed.get(key).is_some_and(|&at| w.within(clock, at))
            })
        };
        let mut forgotten = Vec::new();
        for held in store.held_sources()? {
            if read.contains(&(held.group.clone(), held.key.clone())) || listed(&held.group, &held.key) {
                continue;
            }
            store.forget_source(&held.group, &held.key)?;
            forgotten.push(held);
        }
        Ok(forgotten)
    }

    /// The lock, even after a beat panicked holding it: what it guards is rebuilt by the next round of beats, and
    /// refusing every beat after one panic would stop every holder for good.
    fn lock(&self) -> MutexGuard<'_, Workers> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// [`Holds::beat`]'s work against the store. Takes the beat's progress; leaves the rest.
fn settle(workers: &Workers, store: &dyn Store, beat: &mut Beat<'_>, clock: Clock) -> Result<Vec<Hold>, StoreError> {
    let (worker, group) = (beat.worker, beat.group);
    // Before anything is settled, and whether or not this beat lists the source: a worker between two runtimes
    // lists nothing and still carries what the last one delivered, which it counts as recorded once answered.
    for p in std::mem::take(&mut beat.progress) {
        if store
            .source_hold(group, &p.key)?
            .is_some_and(|hold| hold.holder == worker)
        {
            let admitted = admitted(worker, &p.key, p.progress);
            store.apply_source_progress(group, &p.key, &admitted)?;
        }
    }
    if beat.stopping {
        return Ok(Vec::new());
    }

    let mut held: Vec<Hold> = Vec::new();
    let mut free = Vec::new();
    let runs: BTreeSet<&SourceKey> = beat.runs.iter().collect();
    for key in runs {
        match store.source_hold(group, key)? {
            Some(hold) if hold.holder == worker => held.push(Hold {
                key: key.clone(),
                cursor: hold.cursor,
            }),
            Some(hold) if workers.reading(&hold.holder, group, key, clock) => {}
            hold => free.push((key, hold.and_then(|h| h.cursor))),
        }
    }
    // After every source this worker keeps, so the count it is compared by is what it already reads.
    for (key, cursor) in free {
        let fewer = workers.heard.iter().any(|(other, w)| {
            other != worker && workers.reading(other, group, key, clock) && w.answered.len() < held.len()
        });
        if !fewer {
            store.set_source_holder(group, key, worker)?;
            held.push(Hold {
                key: key.clone(),
                cursor,
            });
        }
    }
    Ok(held)
}

/// `progress` without what is over the limits a ledger keeps. What is dropped is read again by the next holder:
/// duplicates, never lost rows.
fn admitted(worker: &str, key: &SourceKey, mut progress: Progress) -> Progress {
    let (cursor_refused, refused) = progress.admit();
    if cursor_refused {
        tracing::warn!(
            target: "leader.progress_refused",
            subject = %worker,
            source = %key.source,
            "a cursor longer than {MAX_CURSOR} bytes was refused, so the source resumes from the one before it"
        );
    }
    if refused > 0 {
        tracing::warn!(
            target: "leader.progress_refused",
            subject = %worker,
            source = %key.source,
            refused,
            "ledger entries over {MAX_LEDGER_ITEM} bytes of item or {MAX_LEDGER_ENTRY} of entry were refused, so \
             their items are read again by the next holder"
        );
    }
    progress
}

/// A store on disk whose holds table is gone, so every read or write of a hold fails.
#[cfg(test)]
pub(crate) fn store_without_holds(label: &str) -> super::sqlite_store::SqliteStore {
    let dir = std::env::temp_dir().join(format!("smeltr-holds-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("leader.db");
    let _ = std::fs::remove_file(&db);
    let store = super::sqlite_store::SqliteStore::open(&db, &dir.join("sealing.key")).unwrap();
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("DROP TABLE source_holds")
        .unwrap();
    store
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::heartbeat::LedgerEntry;
    use crate::leader::sqlite_store::SqliteStore;

    const BEAT: Duration = Duration::from_secs(5);

    fn store(label: &str) -> SqliteStore {
        let key = std::env::temp_dir().join(format!("smeltr-holds-{}-{label}", std::process::id()));
        SqliteStore::open(Path::new(":memory:"), &key.join("sealing.key")).unwrap()
    }

    fn key(source: &str) -> SourceKey {
        SourceKey {
            source: source.to_string(),
            place: format!("bucket/{source}"),
        }
    }

    /// A leader long up, at `secs` past `base`.
    fn at(base: Instant, secs: u64) -> Clock {
        Clock {
            now: base + Duration::from_secs(secs),
            up_for: Duration::from_secs(3600 + secs),
            hold_time: 3 * BEAT,
        }
    }

    fn beat(
        holds: &Holds,
        s: &SqliteStore,
        worker: &str,
        group: &str,
        runs: Vec<SourceKey>,
        progress: Vec<SourceProgress>,
        clock: Clock,
    ) -> Vec<Hold> {
        let beat = Beat {
            worker,
            group,
            runs,
            progress,
            stopping: false,
            interval: None,
        };
        holds.beat(s, beat, clock).unwrap()
    }

    /// Progress is forgotten only for a source no kept bundle reads and no live worker lists, and never while the
    /// leader is young enough that a worker it has not heard from yet may still be reading.
    #[test]
    fn only_progress_nothing_reads_is_forgotten() {
        let (holds, s, t) = (Holds::default(), store("forget-unread"), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("listed")], Vec::new(), at(t, 0));
        for source in ["read", "unread"] {
            s.set_source_holder("g", &key(source), "w9").unwrap();
        }
        let read = HashSet::from([("g".to_string(), key("read"))]);
        let young = Clock {
            up_for: FORGET_AFTER_UP,
            ..at(t, 1)
        };

        assert!(holds.forget_unread(&s, &read, young).unwrap().is_empty());
        let forgotten = holds.forget_unread(&s, &read, at(t, 1)).unwrap();
        assert_eq!(
            forgotten.iter().map(|h| h.key.source.as_str()).collect::<Vec<_>>(),
            ["unread"]
        );
        assert_eq!(s.source_hold("g", &key("unread")).unwrap(), None);
        assert!(s.source_hold("g", &key("read")).unwrap().is_some());

        // Once w1 has been silent past the hold time, what only it listed is forgotten too.
        let late = holds.forget_unread(&s, &read, at(t, 60)).unwrap();
        assert_eq!(
            late.iter().map(|h| h.key.source.as_str()).collect::<Vec<_>>(),
            ["listed"]
        );
    }

    fn held(holds: &[Hold]) -> Vec<String> {
        holds.iter().map(|h| h.key.source.clone()).collect()
    }

    fn cursor(c: &str) -> Progress {
        Progress {
            cursor: Some(c.to_string()),
            ..Progress::default()
        }
    }

    fn reported(source: &str, progress: Progress) -> Vec<SourceProgress> {
        vec![SourceProgress {
            key: key(source),
            progress,
        }]
    }

    #[test]
    fn a_lone_worker_holds_every_source_it_runs() {
        let (s, holds, t) = (store("lone"), Holds::default(), Instant::now());
        let answer = beat(&holds, &s, "w1", "g", vec![key("a"), key("b")], Vec::new(), at(t, 0));
        assert_eq!(held(&answer), ["a", "b"]);
        assert_eq!(s.source_hold("g", &key("a")).unwrap().unwrap().holder, "w1");
        // Sticky: the next beat answers the same.
        let again = beat(&holds, &s, "w1", "g", vec![key("a"), key("b")], Vec::new(), at(t, 5));
        assert_eq!(held(&again), ["a", "b"]);
    }

    /// Holds are sticky, so what is spread is what comes free: here, everything a silent holder had.
    #[test]
    fn two_workers_share_what_comes_free_between_them() {
        let (s, holds, t) = (store("spread"), Holds::default(), Instant::now());
        let runs = || vec![key("a"), key("b"), key("c"), key("d")];
        assert_eq!(beat(&holds, &s, "w1", "g", runs(), Vec::new(), at(t, 0)).len(), 4);
        assert!(beat(&holds, &s, "w2", "g", runs(), Vec::new(), at(t, 1)).is_empty());
        assert!(beat(&holds, &s, "w3", "g", runs(), Vec::new(), at(t, 1)).is_empty());
        let (mut two, mut three) = (Vec::new(), Vec::new());
        for secs in [16, 21, 26] {
            two = beat(&holds, &s, "w2", "g", runs(), Vec::new(), at(t, secs));
            three = beat(&holds, &s, "w3", "g", runs(), Vec::new(), at(t, secs + 1));
        }
        assert_eq!(
            (two.len(), three.len()),
            (2, 2),
            "{:?} / {:?}",
            held(&two),
            held(&three)
        );
        let mut all = [held(&two), held(&three)].concat();
        all.sort();
        assert_eq!(all, ["a", "b", "c", "d"]);
    }

    /// The map shows a holder by what it was last answered with, dated by when it beat; one this young leader has
    /// not heard yet, until it is gone.
    #[test]
    fn a_holder_is_seen_by_its_last_answer() {
        let (s, holds, t) = (store("seen"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        assert_eq!(
            holds.seen("w1", "g", &key("a"), at(t, 4)),
            Some(Seen::Answered(Duration::from_secs(4)))
        );
        assert_eq!(holds.seen("w1", "h", &key("a"), at(t, 4)), None, "another group");
        assert_eq!(holds.seen("w1", "g", &key("b"), at(t, 4)), None, "not answered with it");

        let young = Clock {
            up_for: Duration::from_secs(1),
            ..at(t, 4)
        };
        assert_eq!(holds.seen("w9", "g", &key("a"), young), Some(Seen::Unheard));
        assert_eq!(
            holds.seen("w9", "g", &key("a"), at(t, 4)),
            None,
            "unheard past the hold time"
        );
        holds.forget("w9");
        assert_eq!(holds.seen("w9", "g", &key("a"), young), None, "forgotten");
    }

    #[test]
    fn another_group_is_another_reader() {
        let (s, holds, t) = (store("groups"), Holds::default(), Instant::now());
        assert_eq!(
            held(&beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0))),
            ["a"]
        );
        assert_eq!(
            held(&beat(&holds, &s, "w2", "h", vec![key("a")], Vec::new(), at(t, 1))),
            ["a"]
        );
    }

    #[test]
    fn a_source_moves_once_its_holder_is_silent_past_the_hold_time() {
        let (s, holds, t) = (store("silent"), Holds::default(), Instant::now());
        beat(
            &holds,
            &s,
            "w1",
            "g",
            vec![key("a")],
            reported("a", cursor("7")),
            at(t, 0),
        );
        beat(
            &holds,
            &s,
            "w1",
            "g",
            vec![key("a")],
            reported("a", cursor("8")),
            at(t, 1),
        );
        assert!(beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 2)).is_empty());
        // Within the hold time of w1's last beat, w2 still waits.
        assert!(beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 16)).is_empty());
        let taken = beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 17));
        assert_eq!(held(&taken), ["a"]);
        assert_eq!(
            taken[0].cursor.as_deref(),
            Some("8"),
            "the new holder resumes where the old one stopped"
        );
        // The old holder, back, finds it gone.
        assert!(beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 18)).is_empty());
    }

    /// A redeploy or a runtime restart can leave a beat or two listing nothing; the source waits for its holder.
    #[test]
    fn a_holder_keeps_a_source_it_stops_listing_for_the_hold_time() {
        let (s, holds, t) = (store("unlisted"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        beat(&holds, &s, "w1", "g", Vec::new(), Vec::new(), at(t, 5));
        assert!(beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 6)).is_empty());
        beat(&holds, &s, "w1", "g", Vec::new(), Vec::new(), at(t, 10));
        assert!(beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 15)).is_empty());
        assert_eq!(
            held(&beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 16))),
            ["a"]
        );
    }

    /// A holder listing it again within the hold time still has it.
    #[test]
    fn a_holder_that_lists_a_source_again_still_holds_it() {
        let (s, holds, t) = (store("relisted"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        assert!(beat(&holds, &s, "w1", "g", Vec::new(), Vec::new(), at(t, 5)).is_empty());
        assert_eq!(
            held(&beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 10))),
            ["a"]
        );
    }

    /// A worker between two runtimes lists nothing, and still carries what the last one delivered.
    #[test]
    fn progress_from_the_holder_counts_when_its_beat_lists_nothing() {
        let (s, holds, t) = (store("between"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        let answer = beat(&holds, &s, "w1", "g", Vec::new(), reported("a", cursor("5")), at(t, 5));
        assert!(answer.is_empty(), "only what it lists is answered");
        assert_eq!(
            s.source_hold("g", &key("a")).unwrap().unwrap().cursor.as_deref(),
            Some("5")
        );
        let back = beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 10));
        assert_eq!(back[0].cursor.as_deref(), Some("5"));
    }

    /// A stopping holder's last progress lands, and its source goes to the next worker at once.
    #[test]
    fn a_stopping_holder_records_its_progress_and_frees_its_sources() {
        let (s, holds, t) = (store("stopping"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        assert!(beat(&holds, &s, "w2", "g", vec![key("a"), key("b")], Vec::new(), at(t, 1)).len() == 1);
        let last = Beat {
            worker: "w1",
            group: "g",
            runs: vec![key("a"), key("c")],
            progress: reported("a", cursor("9")),
            stopping: true,
            interval: None,
        };
        assert!(
            holds.beat(&s, last, at(t, 2)).unwrap().is_empty(),
            "a stopping worker is given nothing"
        );
        assert_eq!(s.source_hold("g", &key("c")).unwrap(), None);
        let taken = beat(&holds, &s, "w2", "g", vec![key("a"), key("b")], Vec::new(), at(t, 3));
        assert_eq!(held(&taken), ["b", "a"]);
        assert_eq!(taken[1].cursor.as_deref(), Some("9"));
    }

    /// A beat the store cannot settle is an error, so the heartbeat fails and the worker sends its progress again.
    #[test]
    fn a_store_that_cannot_be_read_fails_the_beat() {
        let s = store_without_holds("broken");
        let broken = Beat {
            worker: "w1",
            group: "g",
            runs: vec![key("a")],
            progress: reported("a", cursor("1")),
            stopping: false,
            interval: None,
        };
        assert!(Holds::default().beat(&s, broken, at(Instant::now(), 0)).is_err());
    }

    #[test]
    fn a_restarted_leader_counts_the_stored_holder_as_live_for_the_hold_time() {
        let (s, t) = (store("restart"), Instant::now());
        beat(&Holds::default(), &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));

        let restarted = Holds::default();
        let young = |secs| Clock {
            now: t + Duration::from_secs(100 + secs),
            up_for: Duration::from_secs(secs),
            hold_time: 3 * BEAT,
        };
        assert!(beat(&restarted, &s, "w2", "g", vec![key("a")], Vec::new(), young(1)).is_empty());
        assert!(beat(&restarted, &s, "w2", "g", vec![key("a")], Vec::new(), young(15)).is_empty());
        assert_eq!(
            held(&beat(&restarted, &s, "w2", "g", vec![key("a")], Vec::new(), young(16))),
            ["a"]
        );
    }

    #[test]
    fn a_forgotten_holder_frees_its_sources_at_once() {
        let (s, t) = (store("forgotten"), Instant::now());
        beat(&Holds::default(), &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        let restarted = Holds::default();
        let young = Clock {
            now: t + Duration::from_secs(100),
            up_for: Duration::from_secs(1),
            hold_time: 3 * BEAT,
        };
        restarted.forget("w1");
        assert_eq!(
            held(&beat(&restarted, &s, "w2", "g", vec![key("a")], Vec::new(), young)),
            ["a"]
        );
    }

    #[test]
    fn progress_counts_only_from_the_holder() {
        let (s, holds, t) = (store("progress"), Holds::default(), Instant::now());
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        let late = Progress {
            cursor: Some("99".to_string()),
            added: vec![LedgerEntry {
                item: "x".to_string(),
                entry: "e".to_string(),
            }],
            removed: Vec::new(),
        };
        beat(&holds, &s, "w2", "g", vec![key("a")], reported("a", late), at(t, 1));
        assert_eq!(s.source_hold("g", &key("a")).unwrap().unwrap().cursor, None);
        assert!(s.ledger_page("g", &key("a"), None, 1).unwrap().is_empty());

        let answer = beat(
            &holds,
            &s,
            "w1",
            "g",
            vec![key("a")],
            reported("a", cursor("3")),
            at(t, 2),
        );
        assert_eq!(answer[0].cursor.as_deref(), Some("3"));
        assert_eq!(
            s.source_hold("g", &key("a")).unwrap().unwrap().cursor.as_deref(),
            Some("3")
        );
    }

    #[test]
    fn progress_over_the_limits_is_dropped_and_the_rest_kept() {
        let (s, holds, t) = (store("limits"), Holds::default(), Instant::now());
        // Progress counts from a beat after the one that grants the hold.
        beat(&holds, &s, "w1", "g", vec![key("a")], Vec::new(), at(t, 0));
        beat(
            &holds,
            &s,
            "w1",
            "g",
            vec![key("a")],
            reported("a", cursor("1")),
            at(t, 1),
        );
        let entry = |item: String, entry: String| LedgerEntry { item, entry };
        let over = Progress {
            cursor: Some("c".repeat(MAX_CURSOR + 1)),
            added: vec![
                entry("fits".to_string(), "e".repeat(MAX_LEDGER_ENTRY)),
                entry("i".repeat(MAX_LEDGER_ITEM + 1), "e".to_string()),
                entry("long".to_string(), "e".repeat(MAX_LEDGER_ENTRY + 1)),
            ],
            removed: Vec::new(),
        };
        let answer = beat(&holds, &s, "w1", "g", vec![key("a")], reported("a", over), at(t, 2));
        assert_eq!(
            answer[0].cursor.as_deref(),
            Some("1"),
            "the cursor before the refused one stands"
        );
        let page = s.ledger_page("g", &key("a"), None, 10).unwrap();
        assert_eq!(page.iter().map(|e| e.item.as_str()).collect::<Vec<_>>(), ["fits"]);
    }

    /// A worker that beats less often than the leader expects keeps its holds for three of its own intervals.
    #[test]
    fn a_holder_that_beats_slowly_keeps_its_sources_for_three_of_its_intervals() {
        let (s, holds, t) = (store("slow"), Holds::default(), Instant::now());
        let slow = Beat {
            worker: "w1",
            group: "g",
            runs: vec![key("a")],
            progress: Vec::new(),
            stopping: false,
            interval: Some(Duration::from_secs(10)),
        };
        assert_eq!(holds.beat(&s, slow, at(t, 0)).unwrap().len(), 1);
        assert!(
            beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 20)).is_empty(),
            "past the leader's hold time, inside three of the holder's intervals"
        );
        assert_eq!(
            beat(&holds, &s, "w2", "g", vec![key("a")], Vec::new(), at(t, 31)).len(),
            1
        );
    }
}
