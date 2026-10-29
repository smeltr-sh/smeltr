//! Capture on a worker: what a source is delivering, filtered, bounded, and handed to the leader.
//!
//! A worker holds nothing until a heartbeat names a session. Then, per source:
//!
//! - **Running it already** — the transform thread copies matching records past on their way through
//!   (`runtime::Transform`), after decode and before any pipeline. Nothing about the flow changes: a full buffer
//!   drops rather than blocking, so a capture cannot slow what it is watching.
//! - **Not running it** — the worker opens the source itself and never commits. That is the whole of "read without
//!   advancing": no `CommitTracker` exists on this path, so no offset can move. It reads under a group id of the
//!   session's own, so several workers doing this split the partitions between them and production's consumer group
//!   is never joined.
//!
//! A filter is a `Bool` expression over the record and what the source declares, compiled exactly as a route condition
//! is. It runs before any pipeline, so it reads the record as it arrived and not a parsed field.
//!
//! At a destination, the transform thread copies each row a route's pipeline finished for it, as the pipeline left it,
//! before it is handed to egress. Its filter reads that row, compiled exactly as a `deliver` rule's `when` is.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::core::batch::{Batch, SLOT_LIVE};
use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType};
use crate::core::ctx::Ctx;
use crate::core::error::ErrorSink;
use crate::expr::eval::eval;
use crate::expr::ir::{Ir, flatten};
use crate::heartbeat::{CaptureBatch, CaptureColumn, CaptureEnd, CaptureInvitation, CaptureReport, CapturedRow};
use crate::io::SourceRecord;
use crate::plan::compile::parse;
use crate::plan::compile::{compile_patterns, ingest_fields};
use crate::plan::resolve::{ResourceBuilder, SymbolTable, resolve};
use crate::plan::topology::SourceMeta;

/// A compiled filter, or nothing when the invitation carried none.
#[derive(Debug)]
pub struct Filter {
    ir: Option<Ir>,
    patterns: Vec<crate::core::ctx::CompiledPattern>,
    meta: SourceMeta,
    /// The captured source's name, what `__source.name` reads.
    source: String,
}

impl Filter {
    /// Compile `src` against the record and what `meta` declares -- the same symbol table a route condition resolves
    /// against, because both run at the same moment: before a pipeline exists to have parsed anything. `source` is the
    /// name of the source it filters.
    pub fn compile(src: Option<&str>, meta: SourceMeta, source: &str) -> Result<Filter, String> {
        let Some(src) = src else {
            return Ok(Filter::none(meta, source));
        };
        let mut res = ResourceBuilder::default();
        let ast = parse(src, None).map_err(|e| e.to_string())?;
        let mut syms = SymbolTable::new(&ingest_fields(&meta));
        let ir = flatten(resolve(&ast, &mut syms, &mut res).map_err(|e| e.to_string())?);
        let ir = crate::plan::topology::condition(ir, src, "a capture filter").map_err(|e| e.to_string())?;
        let patterns = compile_patterns(&res.patterns).map_err(|e| e.to_string())?;
        Ok(Filter {
            ir: Some(ir),
            patterns,
            meta,
            source: source.to_string(),
        })
    }

    /// A filter that keeps every record.
    fn none(meta: SourceMeta, source: &str) -> Filter {
        Filter {
            ir: None,
            patterns: Vec::new(),
            meta,
            source: source.to_string(),
        }
    }

    /// Which of `records` the filter keeps, given their decoded text. All of them when there is no filter.
    ///
    /// A row the filter errors on is not kept: a capture that cannot say a record matched must not claim it did.
    pub fn keeps(&self, records: &[SourceRecord], text: &[&str]) -> Vec<bool> {
        let Some(ir) = &self.ir else {
            return vec![true; records.len()];
        };
        let rows = records.len();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();

        let layout: Vec<DataType> = ingest_fields(&self.meta).into_iter().map(|(_, t)| t).collect();
        let mut batch = Batch::new(&layout, rows, 0);

        let mut live = Column::alloc(&mut pool, &DataType::Bool, rows);
        bits::fill_valid(live.bool_mut().unwrap().values, rows);
        batch.put(SLOT_LIVE, live, &mut pool);

        // A filter keeps or leaves a record whole; what ingest notes is the pipeline's to note.
        self.meta
            .ingest(&mut batch, records, text, Some(&self.source), |_| true, &mut pool);

        let mut keep = vec![false; rows];
        {
            let mut ctx = Ctx::new(&mut pool, &mut errors);
            ctx.patterns = &self.patterns;
            let mask = eval(ir, &batch, &mut ctx);
            if let Ok(b) = mask.bool() {
                for (row, k) in keep.iter_mut().enumerate() {
                    *k = b.validity.is_valid(row) && bits::get_row(b.values, row);
                }
            }
            mask.reclaim(&mut pool);
        }
        batch.reclaim(&mut pool);
        keep
    }
}

/// One capture in progress on this worker: what it keeps, what it has kept, and what stops it.
pub struct Session {
    invitation: CaptureInvitation,
    /// Which admission of its id this is. See [`Admitted`].
    admission: u64,
    /// Compiled once, when the session is admitted. A transform thread reaches this on every batch, and compiling
    /// there would put a parser on the hot path. Shared so a filter can run outside the lock that found it. `None` on
    /// a capture at a destination, whose filter reads a pipeline's output and is compiled where that is known.
    filter: Option<Arc<Filter>>,
    started: Instant,
    /// What it has kept: one batch per distinct column list, in the order each first appeared.
    batches: Vec<CaptureBatch>,
    /// How many rows `batches` holds in all.
    rows: usize,
    bytes: usize,
    /// What this session already holds, where the source stamps a record with an identity. A source can be read
    /// twice at once -- a cold capture whose source a redeploy makes hot is tapped by the transform threads while
    /// its own consumer is still polling -- and the same record must appear in the sample once.
    seen: Option<HashSet<String>>,
    end: Option<CaptureEnd>,
    /// How many of its rows a progress report has already carried. A session with nothing past this has nothing
    /// new to say, so [`Session::progress`] is silent until it grows again.
    reported: usize,
    /// What Run found out about an SQL source's query, and whether a progress report has carried it since it
    /// changed.
    sql: Option<crate::heartbeat::SqlRun>,
    sql_reported: bool,
    /// The newest Avro writer's schema among the records it was offered, the fields that schema declares, and whether
    /// a progress report has carried them since they changed.
    avro_writer: Option<Arc<crate::io::avro::Parsed>>,
    avro: Vec<crate::heartbeat::AvroField>,
    avro_reported: bool,
    /// Set while this worker holds rows a block keeps for what the session captures and has not shown them yet. Until
    /// they are shown the session takes no other row and sends no report: the leader waits for this worker's first,
    /// and the rows it holds are what the capture is opened to see.
    awaiting_held: bool,
    /// Whether a progress report has gone to the leader. The first goes as soon as nothing is awaited, rows or none.
    reported_once: bool,
}

impl Session {
    /// A session over `invitation`. A filter that will not compile makes a session that is finished before it
    /// starts: the leader hears why rather than an empty sample that looks like a quiet source.
    fn new(invitation: CaptureInvitation, admission: u64) -> Session {
        let (filter, seen, end) = match (&invitation.source, &invitation.destination) {
            (Some(source), None) => {
                let meta = source.kind.meta();
                // A source with no fields yet has its filter compiled once its query is described (`describe`).
                let filter = if source.kind.undeclared() {
                    None
                } else {
                    invitation.filter.as_deref()
                };
                let (filter, end) = match Filter::compile(filter, meta.clone(), &source.id) {
                    Ok(filter) => (filter, None),
                    Err(e) => {
                        tracing::warn!(
                            target: "capture.filter_not_compiled",
                            subject = %invitation.session,
                            error = %e,
                            "a capture's filter would not compile, so it keeps every record it reads"
                        );
                        (Filter::none(meta.clone(), &source.id), Some(CaptureEnd::Failed(e)))
                    }
                };
                // A record's identity is the source's to declare. Where one is declared it is `(topic, partition,
                // offset)`, which is intrinsic to the record rather than to whoever read it, so two readers agree
                // on it.
                let seen = meta
                    .source_fields()
                    .iter()
                    .any(|f| f.0 == "__source.offset")
                    .then(HashSet::new);
                (Some(Arc::new(filter)), seen, end)
            }
            // A route run offers each row it finished once, so there is nothing to tell apart. Its filter is compiled
            // by each transform thread, against each route feeding the destination (`AtDestination`).
            (None, Some(_)) => (None, None, None),
            _ => {
                let why = "the capture names neither one source nor one destination".to_string();
                (None, None, Some(CaptureEnd::Failed(why)))
            }
        };
        Session {
            invitation,
            admission,
            filter,
            started: Instant::now(),
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            seen,
            end,
            reported: 0,
            sql: None,
            sql_reported: true,
            avro_writer: None,
            avro: Vec::new(),
            avro_reported: true,
            awaiting_held: false,
            reported_once: false,
        }
    }

    /// Where this session would be shown rows a block keeps: the source it taps, or the destination it captures.
    fn held_at(&self) -> Option<HeldAt> {
        match (self.invitation.tapped(), &self.invitation.destination) {
            (Some(source), _) => Some(HeldAt::Source(source.id.clone())),
            (None, Some(destination)) => Some(HeldAt::Destination(destination.clone())),
            (None, None) => None,
        }
    }

    /// Whether this session still wants records. A deadline that has passed ends it here rather than waiting for a
    /// record to arrive and notice -- a capture on a silent source has to finish too.
    fn open(&mut self) -> bool {
        if self.end.is_some() {
            return false;
        }
        if self.started.elapsed() >= Duration::from_secs(self.invitation.deadline_secs) {
            self.end = Some(CaptureEnd::Deadline);
            return false;
        }
        true
    }

    /// Whether this session takes the records a deployment reads from the source named `source`.
    fn reads(&self, source: &str) -> bool {
        self.invitation.tapped().is_some_and(|s| s.id == source)
    }

    /// Whether this session captures the destination named `destination`.
    fn sends(&self, destination: &str) -> bool {
        self.invitation.destination.as_deref() == Some(destination)
    }

    /// Take one row, under `columns`, if there is still room. Returns whether the session is still open afterward.
    ///
    /// A row already held is skipped and the session stays open: it is the same record, not a second one.
    ///
    /// A record a block holds is taken whether or not it is held already: the copy that was read as it arrived does not
    /// say why it blocked.
    fn offer(&mut self, columns: &[CaptureColumn], row: CapturedRow, held: bool) -> bool {
        if !self.open() {
            return false;
        }
        // Room is kept for what a block holds here, which is offered once it is shown.
        if self.awaiting_held && !held {
            return true;
        }
        if let Some(seen) = &mut self.seen
            && !held
        {
            let at = |name: &str| columns.iter().position(|c| c.name == name).map(|c| &row.values[c]);
            let identity = ["__source.topic", "__source.partition", "__source.offset"].map(at);
            if !seen.insert(serde_json::json!(identity).to_string()) {
                return true;
            }
        }
        let held = self.batches.iter().position(|b| b.columns == columns);
        let header = match held {
            Some(_) => 0,
            None => CaptureBatch::new(columns.to_vec()).header_size(),
        };
        let cost = header + row.size();
        if self.bytes + cost > self.invitation.max_bytes {
            self.end = Some(CaptureEnd::Bytes);
            return false;
        }
        self.bytes += cost;
        let batch = match held {
            Some(i) => &mut self.batches[i],
            None => {
                self.batches.push(CaptureBatch::new(columns.to_vec()));
                self.batches.last_mut().expect("just pushed")
            }
        };
        batch.rows.push(row);
        self.rows += 1;
        if self.rows >= self.invitation.max_matches {
            self.end = Some(CaptureEnd::Matches);
            return false;
        }
        true
    }

    /// This session's growth since its last progress report, or nothing when it is closed or has caught nothing
    /// new. A closed session is [`Captures::finished`]'s to report, once, with why it stopped -- this is only for
    /// one still filling.
    fn progress(&mut self, session: &str, worker_id: &str) -> Option<CaptureReport> {
        if !self.open() || self.awaiting_held {
            return None;
        }
        if self.reported_once && self.rows <= self.reported && self.sql_reported && self.avro_reported {
            return None;
        }
        self.reported_once = true;
        self.reported = self.rows;
        self.sql_reported = true;
        self.avro_reported = true;
        Some(CaptureReport {
            session: session.to_string(),
            worker_id: worker_id.to_string(),
            batches: self.batches.clone(),
            finished: None,
            sql: self.sql.clone(),
            avro: self.avro.clone(),
        })
    }

    /// Takes `writer` as the newest Avro writer's schema the session was offered records of.
    fn read_with(&mut self, writer: &Arc<crate::io::avro::Parsed>) {
        if self.avro_writer.as_ref().is_some_and(|w| Arc::ptr_eq(w, writer)) {
            return;
        }
        self.avro = crate::io::avro::fields_of(writer);
        self.avro_writer = Some(writer.clone());
        self.avro_reported = false;
    }
}

/// One admission of a capture session: the invitation it was admitted for, and which admission of that id it is. A
/// cold driver drives exactly this one. Its session id alone would not do: a session let go of can be admitted again
/// under the same id while the driver that served it is still in a poll, and that driver must end rather than take
/// the new admission for its own.
#[derive(Debug, Clone)]
pub struct Admitted {
    pub invitation: CaptureInvitation,
    admission: u64,
}

/// How long a worker keeps offering a finished capture to the leader before dropping it. Several heartbeats: a
/// leader unreachable for this long is not one still holding the session the records belong to.
const REPORT_GRACE: Duration = Duration::from_secs(60);

/// Every capture this worker is running, by session id.
///
/// A transform thread reaches this on every batch. Nobody is capturing in the common case, and `watching` answers
/// that from an atomic without taking the lock; when someone is, the filter runs between lock holds rather than
/// inside one, so evaluating a session's expression never paces another lane.
#[derive(Default)]
pub struct Captures {
    sessions: Mutex<HashMap<String, Session>>,
    /// How many sessions in the map read a source, and how many watch a destination, written under the same lock that
    /// changes it. Kept apart so a capture of one kind costs the other kind's threads nothing.
    reading: AtomicUsize,
    at_destinations: AtomicUsize,
    /// The names in what this worker's deployment runs. Its sources decide whether a capture is served by tapping
    /// what is already flowing or by opening a consumer of its own, and a capture of a metrics source labels by it.
    deployed: Mutex<Arc<crate::metrics::Subjects>>,
    /// How many sessions this process has admitted. Numbers each admission.
    admissions: AtomicU64,
    /// The destinations an egress thread in this process is sending to, by the author's name. A capture at any other
    /// has nothing to read here.
    sending: Mutex<HashSet<String>>,
    /// How many things here hold rows a block keeps, by where a capture is shown them. Taken inside the sessions lock
    /// wherever both are, so a session admitted as a holder lets go is never left awaiting it.
    holders: Mutex<HashMap<HeldAt, usize>>,
}

/// Where a worker holds rows a block keeps: a source whose block keeps records, or a destination that keeps rows it
/// refused. A capture of the one, or at the other, is shown them.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HeldAt {
    Source(String),
    Destination(String),
}

/// That something here holds rows a block keeps at `at`, from [`Captures::holding`] until this is dropped. A capture
/// there admitted meanwhile takes nothing else and reports nothing until it is shown them.
pub struct Holding {
    captures: Arc<Captures>,
    at: HeldAt,
}

impl Drop for Holding {
    fn drop(&mut self) {
        let mut sessions = self.captures.sessions.lock().unwrap();
        let mut holders = self.captures.holders.lock().unwrap();
        let Some(count) = holders.get_mut(&self.at) else {
            return;
        };
        *count -= 1;
        if *count > 0 {
            return;
        }
        holders.remove(&self.at);
        // Nothing here holds rows for them any more, so nothing will show them any.
        for s in sessions.values_mut() {
            if s.held_at().as_ref() == Some(&self.at) {
                s.awaiting_held = false;
            }
        }
    }
}

/// The session held under `id`, while it is admission `admission` of that id.
fn admitted_session<'a>(
    sessions: &'a mut HashMap<String, Session>,
    id: &str,
    admission: u64,
) -> Option<&'a mut Session> {
    sessions.get_mut(id).filter(|s| s.admission == admission)
}

impl Captures {
    /// Start any session in `invitations` this worker does not hold. An invitation repeats on every heartbeat while
    /// the leader lists it, so this is idempotent.
    ///
    /// Returns the sessions that were new, for the caller to open a source for where one is needed.
    pub fn admit(&self, invitations: &[CaptureInvitation]) -> Vec<Admitted> {
        let mut sessions = self.sessions.lock().unwrap();
        let mut started = Vec::new();
        for invitation in invitations {
            if !sessions.contains_key(&invitation.session) {
                let admission = self.admissions.fetch_add(1, Ordering::Relaxed);
                let mut session = Session::new(invitation.clone(), admission);
                session.awaiting_held = session
                    .held_at()
                    .is_some_and(|at| self.holders.lock().unwrap().contains_key(&at));
                sessions.insert(invitation.session.clone(), session);
                started.push(Admitted {
                    invitation: invitation.clone(),
                    admission,
                });
            }
        }
        self.recount(&sessions);
        started
    }

    /// Record what this worker's deployment reads, as `deploy` left it.
    ///
    /// A session still filling on a source the deployment now reads is dropped rather than reported, so the runtime
    /// serves it. It stopped filling the moment the deployment took the source, and the deployer hands the runtime
    /// only a session this process no longer holds: kept, it would sit unread until its deadline and answer with a
    /// partial sample. The leader lists a session to a worker until that worker answers or the capture is over, so
    /// the runtime is handed the same invitation on the next beat.
    ///
    /// One that already reached a bound is a finished answer and stays: it is what the leader asked for, nothing
    /// else is going to produce it, and the session it belongs to may expire before a replacement could.
    pub fn set_deployed(&self, deployed: &Arc<crate::metrics::Subjects>) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, s| {
            !s.open()
                || !s
                    .invitation
                    .tapped()
                    .is_some_and(|source| deployed.sources.contains(&source.id))
        });
        self.recount(&sessions);
        *self.deployed.lock().unwrap() = deployed.clone();
    }

    /// Note that something here holds rows a block keeps at `at`, until the returned [`Holding`] is dropped.
    pub fn holding(self: &Arc<Self>, at: HeldAt) -> Holding {
        *self.holders.lock().unwrap().entry(at.clone()).or_default() += 1;
        Holding {
            captures: self.clone(),
            at,
        }
    }

    /// Note that an egress thread here sends to `destination`, so a capture there reads what it takes.
    pub fn send_to(&self, destination: &str) {
        self.sending.lock().unwrap().insert(destination.to_string());
    }

    /// Note that nothing here sends to `destination` any more. A capture still filling there ends with what it has:
    /// it would otherwise wait out its deadline on a destination nothing feeds.
    pub fn stop_sending_to(&self, destination: &str) {
        self.sending.lock().unwrap().remove(destination);
        for s in self.sessions.lock().unwrap().values_mut() {
            if s.open() && s.sends(destination) {
                s.end = Some(CaptureEnd::Failed(format!(
                    "this worker stopped sending to destination {destination:?}"
                )));
            }
        }
    }

    /// Whether an egress thread here sends to `destination`.
    pub fn sends_to(&self, destination: &str) -> bool {
        self.sending.lock().unwrap().contains(destination)
    }

    /// Every capture at a destination still taking rows. Asked by a transform thread on every attempt, so a worker
    /// running none answers from one atomic load and takes no lock.
    pub fn at_destinations(&self) -> Vec<AtDestination> {
        if self.at_destinations.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let mut sessions = self.sessions.lock().unwrap();
        sessions
            .iter_mut()
            .filter_map(|(id, s)| {
                let destination = s.invitation.destination.clone()?;
                s.open().then(|| AtDestination {
                    session: id.clone(),
                    admission: s.admission,
                    destination,
                    filter: s.invitation.filter.clone(),
                    wanted: s.invitation.max_matches.saturating_sub(s.rows),
                    room: s.invitation.max_bytes.saturating_sub(s.bytes),
                })
            })
            .collect()
    }

    /// Offer `rows`, each under its columns, to `at`'s session, while it is still that admission.
    pub fn offer_sent(&self, at: &AtDestination, rows: Vec<(Vec<CaptureColumn>, CapturedRow)>) {
        let mut sessions = self.sessions.lock().unwrap();
        let Some(s) = admitted_session(&mut sessions, &at.session, at.admission) else {
            return;
        };
        for (columns, row) in rows {
            if !s.offer(&columns, row, false) {
                break;
            }
        }
    }

    /// Offer `rows`, which `destination` refused and keeps to send again, to every capture at it whose admission is not
    /// in `shown`, each under [`EXCEPTION_MESSAGE`], `why` of the same row, and add those admissions to `shown`. An
    /// admission no longer open leaves `shown`: rows can be held for as long as the destination refuses them, and
    /// captures come and go meanwhile.
    ///
    /// Every row is offered, whatever the capture's filter: that is compiled against a route's output on a transform
    /// thread, and these rows are what holds the destination up. Asked by an egress thread on every pass while it keeps
    /// refused rows, so a worker running no capture at a destination answers from one atomic load.
    pub fn offer_held_at(
        &self,
        destination: &str,
        shown: &mut HashSet<u64>,
        rows: &[crate::io::OwnedRecord],
        why: &[String],
    ) {
        if self.at_destinations.load(Ordering::Relaxed) == 0 {
            shown.clear();
            return;
        }
        let owed = self.owed(shown, |s| s.sends(destination));
        if owed.is_empty() {
            return;
        }
        // Outside the lock, which covers every session on this worker: each row's document is parsed here.
        let held: Vec<_> = rows
            .iter()
            .enumerate()
            .map(|(i, row)| held_row(row, why.get(i).map_or("", String::as_str)))
            .collect();
        let mut sessions = self.sessions.lock().unwrap();
        for (session, admission) in &owed {
            let Some(s) = admitted_session(&mut sessions, session, *admission) else {
                continue;
            };
            for (columns, row) in &held {
                if !s.offer(columns, row.clone(), true) {
                    break;
                }
            }
            s.awaiting_held = false;
        }
        shown.extend(owed.into_iter().map(|(_, admission)| admission));
    }

    /// The open sessions `at` picks whose admission is not in `shown`, by id and admission: those held rows are still
    /// owed to. An admission no longer open leaves `shown`.
    fn owed(&self, shown: &mut HashSet<u64>, at: impl Fn(&Session) -> bool) -> Vec<(String, u64)> {
        let mut sessions = self.sessions.lock().unwrap();
        let mut open = HashSet::new();
        let owed = sessions
            .iter_mut()
            .filter_map(|(id, s)| {
                if !(s.open() && at(s)) {
                    return None;
                }
                open.insert(s.admission);
                (!shown.contains(&s.admission)).then(|| (id.clone(), s.admission))
            })
            .collect();
        shown.retain(|admission| open.contains(admission));
        owed
    }

    /// End `at`'s session with `why`, for a filter that will not compile against a route feeding its destination. A
    /// later admission under the same id is left alone.
    pub fn fail_at(&self, at: &AtDestination, why: String) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = admitted_session(&mut sessions, &at.session, at.admission) {
            s.end = Some(CaptureEnd::Failed(why));
        }
    }

    fn recount(&self, sessions: &HashMap<String, Session>) {
        let at_destinations = sessions.values().filter(|s| s.invitation.destination.is_some()).count();
        self.at_destinations.store(at_destinations, Ordering::Relaxed);
        self.reading.store(sessions.len() - at_destinations, Ordering::Relaxed);
    }

    /// Whether a deployment is reading `source` right now.
    pub fn is_deployed(&self, source: &str) -> bool {
        self.deployed.lock().unwrap().sources.iter().any(|s| s == source)
    }

    /// Whether `admitted` needs no reader of its own: it names no source, so it was admitted finished, or a
    /// deployment reading its source serves it, as one does any session that does not read its source itself.
    pub fn served(&self, admitted: &Admitted) -> bool {
        let invitation = &admitted.invitation;
        invitation.source.is_none() || invitation.tapped().is_some_and(|source| self.is_deployed(&source.id))
    }

    /// Whether any session is watching `source`, by the name the author gave it. Checked before a batch's text is
    /// gathered, so a worker running no capture answers from one atomic load.
    pub fn watching(&self, source: &str) -> bool {
        if self.reading.load(Ordering::Relaxed) == 0 {
            return false;
        }
        self.sessions
            .lock()
            .unwrap()
            .values_mut()
            .any(|s| s.open() && s.reads(source))
    }

    /// Offer one batch to every session watching `source`, each keeping what its own filter matches.
    ///
    /// Called from a transform thread on the batch it is about to run. `text` is the decoded records, which is what
    /// a filter reads and what a captured record carries -- the decode has already happened for this batch, so a
    /// capture never makes a worker decode twice.
    pub fn tap(&self, source: &str, records: &[SourceRecord], text: &[&str]) {
        let watching: Vec<(String, u64)> = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions
                .iter_mut()
                .filter_map(|(id, s)| (s.reads(source) && s.open()).then(|| (id.clone(), s.admission)))
                .collect()
        };
        for (session, admission) in watching {
            self.fill(&session, admission, records, text, None);
        }
    }

    /// Whether a session watching `source` has an admission not in `shown`: one a block's records are still owed to.
    pub fn owed_held(&self, source: &str, shown: &HashSet<u64>) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .values_mut()
            .any(|s| s.open() && s.reads(source) && !shown.contains(&s.admission))
    }

    /// Offer the records a block holds on `source` to every session watching it whose admission is not in `shown`,
    /// each keeping what its own filter matches under [`EXCEPTION_MESSAGE`], `why` of the same record, and add those
    /// admissions to `shown`. An admission no longer open leaves `shown`.
    pub fn tap_held(
        &self,
        source: &str,
        shown: &mut HashSet<u64>,
        records: &[SourceRecord],
        text: &[&str],
        why: &[String],
    ) {
        let watching = self.owed(shown, |s| s.reads(source));
        for (session, admission) in &watching {
            self.fill(session, *admission, records, text, Some(why));
        }
        let mut sessions = self.sessions.lock().unwrap();
        for (session, admission) in &watching {
            if let Some(s) = admitted_session(&mut sessions, session, *admission) {
                s.awaiting_held = false;
            }
        }
        shown.extend(watching.into_iter().map(|(_, admission)| admission));
    }

    /// Offer records to one admission of a session, for the cold path, which polls a source of its own.
    pub fn offer(&self, admitted: &Admitted, records: &[SourceRecord], text: &[&str]) {
        self.fill(&admitted.invitation.session, admitted.admission, records, text, None);
    }

    /// Keep what `session`'s filter matches, while the session held under that id is admission `admission`.
    ///
    /// The filter runs between the two lock holds rather than inside either. It builds a batch and evaluates an
    /// expression over every row, and one lock covers every session on this worker: held across that, a capture on
    /// one source would pace the transform threads of all the others.
    ///
    /// `why`, for records a block holds, says why each blocked, and is kept beside it as [`EXCEPTION_MESSAGE`].
    fn fill(&self, session: &str, admission: u64, records: &[SourceRecord], text: &[&str], why: Option<&[String]>) {
        let filter = {
            let mut sessions = self.sessions.lock().unwrap();
            let Some(s) = admitted_session(&mut sessions, session, admission) else {
                return;
            };
            if !s.open() {
                return;
            }
            let Some(filter) = s.filter.clone() else {
                return;
            };
            filter
        };
        let keep = filter.keeps(records, text);

        let mut sessions = self.sessions.lock().unwrap();
        let Some(s) = admitted_session(&mut sessions, session, admission) else {
            return;
        };
        if let Some(writer) = records.iter().rev().find_map(|r| r.chunk.avro_writers().last()) {
            s.read_with(writer);
        }
        // A record polled before the reader saw this capture had its headers left behind. Kept, it would say it had
        // none; the capture starts with the first record that kept them instead.
        let has_headers = filter
            .meta
            .source_fields()
            .iter()
            .any(|(name, _)| *name == "__source.headers");
        let fields = filter.meta.objects(records, Some(&filter.source));
        let mut columns = source_columns(&filter.meta);
        // First, since why a record blocked is what a capture of a blocked source is opened to see.
        if why.is_some() {
            columns.insert(
                0,
                CaptureColumn {
                    name: EXCEPTION_MESSAGE.to_string(),
                    ty: DataType::Str.spell(),
                },
            );
        }
        for (i, source) in fields.into_iter().enumerate() {
            // A held record was polled before any capture of it, so it never kept its headers. It is shown without
            // them: it is the record that has to be found.
            if !keep.get(i).copied().unwrap_or(false) || (why.is_none() && has_headers && records[i].headers.is_none())
            {
                continue;
            }
            let mut row = source_row(
                &filter.meta,
                &records[i],
                text.get(i).copied().unwrap_or_default(),
                &source,
            );
            if let Some(why) = why {
                row.values.insert(0, why.get(i).cloned().unwrap_or_default().into());
                for field in &mut row.failed {
                    *field += 1;
                }
            }
            if !s.offer(&columns, row, why.is_some()) {
                break;
            }
        }
    }

    /// Whether `admitted` is still held and still wants records. The cold driver's loop condition: false once its
    /// session is let go, even while a later admission holds the same id.
    pub fn open(&self, admitted: &Admitted) -> bool {
        let mut sessions = self.sessions.lock().unwrap();
        admitted_session(&mut sessions, &admitted.invitation.session, admitted.admission).is_some_and(|s| s.open())
    }

    /// Keep what Run found out about `admitted`'s query. `meta`, where given, is the record the worker declared from
    /// the query's columns for a source that declared none, and the session's filter is compiled against it.
    pub fn describe(&self, admitted: &Admitted, run: crate::heartbeat::SqlRun, meta: Option<SourceMeta>) {
        let mut sessions = self.sessions.lock().unwrap();
        let Some(s) = admitted_session(&mut sessions, &admitted.invitation.session, admitted.admission) else {
            return;
        };
        if let (Some(meta), Some(source)) = (meta, &s.invitation.source) {
            match Filter::compile(s.invitation.filter.as_deref(), meta, &source.id) {
                Ok(filter) => s.filter = Some(Arc::new(filter)),
                Err(e) => s.end = Some(CaptureEnd::Failed(e)),
            }
        }
        s.sql = Some(run);
        s.sql_reported = false;
    }

    /// Mark `admitted` finished with `reason`, for a source that would not open or a poll that failed. A later
    /// admission under the same id is left alone.
    pub fn fail(&self, admitted: &Admitted, reason: impl Into<String>) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = admitted_session(&mut sessions, &admitted.invitation.session, admitted.admission) {
            s.end = Some(CaptureEnd::Failed(reason.into()));
        }
    }

    /// End `admitted` with what it has, for a capture that has read all its source gives it. It ends as at its match
    /// count, the bound a leader already knows: the sample is as large as it will get.
    pub fn end(&self, admitted: &Admitted) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(s) = admitted_session(&mut sessions, &admitted.invitation.session, admitted.admission)
            && s.open()
        {
            s.end = Some(CaptureEnd::Matches);
        }
    }

    /// End every session still filling, keeping what it has. For a runtime that is stopping: its records are
    /// worth more delivered short than lost with the process, and a leader that invites the session again gets a
    /// fresh one in the replacement, never these.
    pub fn end_all(&self, reason: &str) {
        for s in self.sessions.lock().unwrap().values_mut() {
            if s.open() {
                s.end = Some(CaptureEnd::Failed(reason.to_string()));
            }
        }
    }

    /// Every finished session's report. A session that has hit a bound or run out of time is done; one still
    /// filling is left alone -- [`Captures::progress`] is its report, in the meantime.
    ///
    /// The session stays until [`Captures::sent`] says the leader has it, so a report that does not arrive is
    /// offered again on the next heartbeat. The leader keeps the first answer from each worker and ignores the
    /// rest, so sending one twice cannot double a sample. A session still unacknowledged [`REPORT_GRACE`] past its
    /// deadline is dropped.
    pub fn finished(&self, worker_id: &str) -> Vec<CaptureReport> {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, s| s.started.elapsed() < Duration::from_secs(s.invitation.deadline_secs) + REPORT_GRACE);
        self.recount(&sessions);

        sessions
            .iter_mut()
            .filter_map(|(id, s)| {
                let done = !s.open();
                done.then(|| CaptureReport {
                    session: id.clone(),
                    worker_id: worker_id.to_string(),
                    batches: s.batches.clone(),
                    finished: Some(s.end.clone().unwrap_or(CaptureEnd::Deadline)),
                    sql: s.sql.clone(),
                    avro: s.avro.clone(),
                })
            })
            .collect()
    }

    /// Every open session's growth since it last reported, one `CaptureReport` per session that has caught
    /// something new. Polled on the same schedule as [`Captures::finished`]; a session with nothing new since its
    /// last progress report is left out rather than repeating a list nothing changed.
    pub fn progress(&self, worker_id: &str) -> Vec<CaptureReport> {
        let mut sessions = self.sessions.lock().unwrap();
        sessions
            .iter_mut()
            .filter_map(|(id, s)| s.progress(id, worker_id))
            .collect()
    }

    /// Forget a session the leader has acknowledged.
    pub fn sent(&self, session: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.remove(session);
        self.recount(&sessions);
    }

    /// Let go of every session still filling that `listed` leaves out. The leader lists every capture it still
    /// wants from this worker on every heartbeat answer, so it wants nothing more from this worker of one it leaves
    /// out. The transform tap stops offering to it at once; a cold driver sees it gone at its next poll and drops its
    /// source and its copy of the lent credentials; the invitation, and what it lent, go with the session. A finished
    /// session stays until its report is sent: it is this worker's answer, and the leader still takes its reason.
    pub fn drop_unlisted(&self, listed: &HashSet<String>) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|id, s| !s.open() || listed.contains(id));
        self.recount(&sessions);
    }

    /// Whether this process holds `session`, filling or finished and waiting for its report to be sent.
    pub fn holds(&self, session: &str) -> bool {
        self.sessions.lock().unwrap().contains_key(session)
    }
}

/// The column a record a block holds says why it blocked in: an exception's failure, or why the batch was held. Not a
/// field of the record, so a sample's preview leaves it out.
pub const EXCEPTION_MESSAGE: &str = "__exception.message";

/// A source batch's columns: the record's fields, then the `__source` fields, each as the source declares it.
fn source_columns(meta: &SourceMeta) -> Vec<CaptureColumn> {
    let record = meta
        .record_fields()
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty));
    let source = meta
        .source_fields()
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.clone()));
    record
        .chain(source)
        .map(|(name, ty)| match name == UNCLAIMED {
            // Spelled as a destination's open record spells it (`written_columns`).
            true => CaptureColumn {
                name,
                ty: "{...}".to_string(),
            },
            false => CaptureColumn { name, ty: ty.spell() },
        })
        .collect()
}

/// One record as a row of `source_columns(meta)`: its fields from its record object, a value that failed its type as
/// the source spelled it, then its `__source` fields from `source`, the record's `__source` object.
fn source_row(meta: &SourceMeta, record: &SourceRecord, text: &str, source: &crate::io::SourceFields) -> CapturedRow {
    let object = meta.record_object(record, text);
    let fields = meta.record_fields();
    let mut values: Vec<serde_json::Value> = fields
        .iter()
        .map(|(name, _)| match *name {
            UNCLAIMED => crate::io::unclaimed_of(&object, &fields).unwrap_or_default(),
            name => crate::io::value_at(&object, name).cloned().unwrap_or_default(),
        })
        .collect();
    let mut failed = Vec::new();
    for (_, field, original) in record.chunk.failed().iter().filter(|(r, _, _)| *r == record.row) {
        values[*field] = serde_json::Value::String(original.clone());
        failed.push(*field);
    }
    // A payload that did not read has no field of its own to blame, so its record's first holds why, as the
    // pipeline's exception does.
    if let Some(why) = record.chunk.unread(record.row)
        && !values.is_empty()
    {
        values[0] = serde_json::Value::String(why.to_string());
        failed.push(0);
    }
    values.extend(meta.source_fields().iter().map(|(name, _)| {
        let name = name
            .strip_prefix("__source.")
            .expect("a source field is under `__source`");
        crate::io::value_at(source, name).cloned().unwrap_or_default()
    }));
    CapturedRow {
        values,
        failed,
        worker: None,
    }
}

/// A capture at a destination, as a transform thread serves it: its session, the destination by the author's name,
/// its filter, and how many more rows and bytes it takes.
#[derive(Debug, Clone)]
pub struct AtDestination {
    pub session: String,
    /// Which admission of `session` this is. See [`Admitted`].
    pub(crate) admission: u64,
    pub destination: String,
    /// A condition over the row as its pipeline finished it, as a `deliver` rule's `when` reads it.
    pub filter: Option<String>,
    pub wanted: usize,
    /// How many bytes of rows it still takes. A transform thread copies no more than this, since the session would
    /// drop the rest.
    pub room: usize,
}

/// The column an open record's unclaimed keys go in, as one object.
pub use crate::plan::tree::UNCLAIMED;

/// The fields a pipeline writes (`plan::dictionary::record_type`), each as a column, and [`UNCLAIMED`] for the keys
/// an open record leaves unclaimed. None for a record that is not a struct: its rows are shown by their own keys.
pub fn written_columns(record: &DataType) -> Vec<CaptureColumn> {
    let mut written: Vec<CaptureColumn> = match record {
        DataType::Struct(fields, _) => fields
            .iter()
            .map(|(name, ty)| CaptureColumn {
                name: name.clone(),
                ty: ty.spell(),
            })
            .collect(),
        _ => Vec::new(),
    };
    if matches!(record, DataType::Struct(_, true)) {
        written.push(CaptureColumn {
            name: UNCLAIMED.to_string(),
            ty: "{...}".to_string(),
        });
    }
    written
}

/// Every `__` field a pipeline's plan holds at its end, as a column with the slot that holds it: its `__source` fields
/// and any `__destination` field it set. Not `__live`, which is true on every row a destination is sent, and not a name
/// in `DESTINATION_COLUMNS`, which a destination capture fills with what the row was actually given.
pub fn internal_columns(plan: &crate::plan::compile::Plan) -> Vec<(CaptureColumn, crate::core::id::SlotId)> {
    plan.tree
        .leaves()
        .into_iter()
        .filter(|(name, _)| {
            name.starts_with("__")
                && name != "__live"
                && !crate::heartbeat::DESTINATION_COLUMNS.contains(&name.as_str())
        })
        .map(|(name, slot)| {
            let ty = plan.slot_layout[slot].spell();
            (CaptureColumn { name, ty }, slot)
        })
        .collect()
}

/// A tapped row's document as its record object, empty where it does not read as one.
pub fn tapped_object(doc: &[u8]) -> crate::io::RecordObject {
    serde_json::from_slice(doc).unwrap_or_default()
}

/// A tapped row's value of an internal field, as `format::json::write_value` wrote it: null where it wrote none.
pub fn tapped_value(doc: Option<&[u8]>) -> serde_json::Value {
    doc.and_then(|doc| serde_json::from_slice(doc).ok()).unwrap_or_default()
}

/// One row a pipeline finished, as a capture at a destination shows it: `object`, the row's document, under
/// `written` (`written_columns`), then `fields`, its values of `internal` (`internal_columns`), then its
/// `__destination` values in `DESTINATION_COLUMNS` order. Returns the columns and the row under them.
///
/// A document `written` does not describe is shown by its own keys, each a variant.
pub fn sent_row(
    mut object: crate::io::RecordObject,
    written: &[CaptureColumn],
    internal: &[(CaptureColumn, crate::core::id::SlotId)],
    fields: Vec<serde_json::Value>,
    destination: [Option<String>; crate::heartbeat::DESTINATION_COLUMNS.len()],
) -> (Vec<CaptureColumn>, CapturedRow) {
    let fits =
        written.iter().any(|c| c.name == UNCLAIMED) || object.keys().all(|k| written.iter().any(|c| &c.name == k));
    let (mut columns, mut values) = match fits && !written.is_empty() {
        true => {
            let mut values: Vec<serde_json::Value> = written
                .iter()
                .filter(|c| c.name != UNCLAIMED)
                .map(|c| object.remove(&c.name).unwrap_or_default())
                .collect();
            if written.iter().any(|c| c.name == UNCLAIMED) {
                values.push(match object.is_empty() {
                    true => serde_json::Value::Null,
                    false => serde_json::Value::Object(object),
                });
            }
            (written.to_vec(), values)
        }
        false => {
            let columns = object
                .keys()
                .map(|k| CaptureColumn {
                    name: k.clone(),
                    ty: "variant".to_string(),
                })
                .collect();
            (columns, object.into_iter().map(|(_, v)| v).collect())
        }
    };
    // A root key is a column's name as a path spells it, so a key holding a `.` stays one key when the row is read back.
    for column in columns.iter_mut().filter(|c| c.name != UNCLAIMED) {
        column.name = crate::plan::tree::spell(&column.name).into_owned();
    }
    columns.extend(internal.iter().map(|(c, _)| c.clone()));
    values.extend(fields);
    for (name, value) in crate::heartbeat::DESTINATION_COLUMNS.iter().zip(destination) {
        columns.push(CaptureColumn {
            name: name.to_string(),
            ty: "str".to_string(),
        });
        values.push(value.map_or(serde_json::Value::Null, serde_json::Value::String));
    }
    (
        columns,
        CapturedRow {
            values,
            failed: Vec::new(),
            worker: None,
        },
    )
}

/// A row a destination refused and keeps, as a capture at it shows it: [`EXCEPTION_MESSAGE`], then the row's document by
/// its own keys, then its `__destination` values. The route is not known past the transform thread, so it is left
/// null. A document that does not read as an object is shown as its text, under `_raw`.
fn held_row(record: &crate::io::OwnedRecord, why: &str) -> (Vec<CaptureColumn>, CapturedRow) {
    let object = serde_json::from_slice(&record.value)
        .unwrap_or_else(|_| crate::io::raw_object(String::from_utf8_lossy(&record.value)));
    let destination = [record.placement.clone(), record.doc_id.clone(), None];
    let (mut columns, mut row) = sent_row(object, &[], &[], Vec::new(), destination);
    columns.insert(
        0,
        CaptureColumn {
            name: EXCEPTION_MESSAGE.to_string(),
            ty: DataType::Str.spell(),
        },
    );
    row.values.insert(0, why.into());
    (columns, row)
}

/// The consumer group a cold capture reads under: the session's own, never the one the configuration names.
///
/// Joining the configured group would rebalance it the moment a real deployment existed, and this path is for a
/// source nothing is running -- which is exactly when a deployment is most likely to start underneath it. Several
/// workers capturing the same session share this id and so split the partitions rather than each reading all of
/// them.
pub fn capture_group_id(session: &str) -> String {
    format!("smeltr-capture-{session}")
}

/// The `thread` label every capture reports under. One label for all of them, never the session id: a label is
/// structural (design doc, Observability) and the key count has to stay bounded by the topology, where sessions are
/// unbounded over time. Which session a number came from is the capture's own panel to answer, not the series'.
pub const CAPTURE_LABEL: &str = "capture";

/// The invitation's source, rewritten to read without advancing anything: its own consumer group, and starting at
/// `from`, or at the end so a capture shows what is arriving now.
///
/// Nothing on this path commits -- there is no `CommitTracker` on it at all -- so the group id never accumulates an
/// offset and the source is left exactly as it was found.
pub fn cold_source_config(
    source: &crate::config_file::SourceDto,
    session: &str,
    from: Option<crate::config_file::StartPositionDto>,
) -> crate::config_file::SourceDto {
    let mut source = source.clone();
    use crate::config_file::SourceKindDto;
    if let SourceKindDto::Kafka {
        group_id: group,
        start,
        stop,
        ..
    }
    | SourceKindDto::EventHubs {
        consumer_group: group,
        start,
        stop,
        ..
    } = &mut source.kind
    {
        *group = capture_group_id(session);
        *start = from.unwrap_or(crate::config_file::StartPositionDto::Latest);
        *stop = None;
    }
    source
}

/// Run `admitted` against a source of its own: one this worker is not already reading, or any source for a capture
/// that reads the source itself (`CaptureInvitation::reads_source_itself`).
///
/// Opens the source, polls it, and never commits: there is no `CommitTracker` on this path at all, so nothing this
/// does can move an offset. It stops as soon as its admission is closed -- a bound reached, the deadline, or the
/// leader no longer listing it -- and the source is dropped with it. A later admission under the same id is not its
/// to drive.
///
/// Blocking, so it runs on a thread of its own. Failing to open the source ends the session as `Failed` rather than
/// leaving whoever asked watching an empty sample fill up forever.
///
/// Unless it reads the source itself, it also stops when a deployment starts reading the source, which a redeploy
/// mid-capture can do: from that moment the transform threads are tapping the same flow into the same session, and
/// one reader is what the sample needs.
pub fn run_cold_capture(
    captures: &Captures,
    admitted: &Admitted,
    registry: &Arc<crate::metrics::Metrics>,
    secrets: &crate::plan::topology::HeldSecrets,
) {
    let invitation = &admitted.invitation;
    let session = invitation.session.clone();
    let Some(source) = &invitation.source else {
        // Admitted finished: `Session::new` said why.
        return;
    };
    let cold = Cold {
        captures,
        admitted,
        config: cold_source_config(source, &session, invitation.start),
    };
    if cold.config.kind.sql().is_some() {
        return run_sql_capture(&cold, registry, secrets);
    }

    let kind = match source_kind_of(cold.config.clone()) {
        Ok(kind) => kind,
        Err(e) => {
            tracing::warn!(
                target: "capture.source_kind_unknown",
                subject = %session,
                error = %e,
                "a capture named a source this worker cannot build"
            );
            cold.fail(e);
            return;
        }
    };
    // Opened here it would take requests and answer them, with nothing delivered: the sender would drop what it
    // sent. A receiver is captured where it is deployed, from the records its reader takes.
    if matches!(kind, crate::plan::topology::SourceKind::Receiver(_)) {
        cold.fail("a receiver is captured only on a worker that runs it; deploy it, then capture again".to_string());
        return;
    }
    let meta = kind.meta();
    let metrics = crate::metrics::MetricsHandle::new(registry.clone(), CAPTURE_LABEL);

    // The worker's own credentials, plus the ones this invitation brought for this source alone. A local copy: it
    // goes out of scope with the capture, so nothing the capture was lent outlives the session or is ever reported
    // back as held.
    let mut secrets = secrets.clone();
    secrets.take(invitation.secrets.clone());

    // The capture's own certificate arrived with this invitation, lent for this session alone, so it is checked
    // here as a deploy checks the ones its commit names.
    if let Some(tls) = kind.tls()
        && let Some(why) = crate::plan::topology::certificate_error(tls, &secrets)
    {
        cold.fail(why);
        return;
    }

    // A metrics or logs source reads the process this runs in: the runtime when one was running as the capture
    // arrived, the supervisor otherwise.
    let subjects = captures.deployed.lock().unwrap().clone();
    let mut source = match crate::io::build_source(&kind, registry, &subjects, &secrets, None) {
        Ok(source) => source,
        Err(e) => {
            tracing::warn!(
                target: "capture.source_not_opened",
                subject = %session,
                error = %e,
                "a capture could not open its source"
            );
            cold.fail(format!("opening the source failed: {e}"));
            return;
        }
    };
    drive(&cold, source.as_mut(), &meta, &metrics, None);
}

/// Poll `source` into `cold`'s session until the session closes or a deployment starts reading the source. A session
/// that reads its source itself (`CaptureInvitation::reads_source_itself`) reads on whatever is deployed. An SQL Run
/// passes `run`, and ends once `run` says it has read all it reads; a connection error ends it with that as the
/// reason.
fn drive<S: crate::io::Source + ?Sized>(
    cold: &Cold,
    source: &mut S,
    meta: &SourceMeta,
    metrics: &crate::metrics::MetricsHandle,
    run: Option<fn(&S) -> bool>,
) {
    let session = &cold.admitted.invitation.session;
    // A sample holds everything its source set, read or not.
    source.keep_headers(true);

    // A deployment that starts reading the source serves the session from then on.
    while cold.open() && !cold.served() {
        let records = match source.poll_batch(
            crate::worker::TICK_MAX_ROWS,
            crate::worker::TICK_MAX_BYTES,
            POLL_TIMEOUT,
        ) {
            Ok(records) => records,
            Err(e) => {
                tracing::warn!(
                    target: "capture.poll_failed",
                    subject = %session,
                    error = %e,
                    "a capture could not poll its source, so it ends with what it has"
                );
                cold.fail(format!("polling failed: {e}"));
                return;
            }
        };
        // An SQL source reports a refused poll here rather than as an error, so it can try again next interval. A
        // capture shows the refusal instead of waiting out its deadline on it.
        if let Some(why) = run.and_then(|_| source.connection_error()) {
            cold.fail(why);
            return;
        }
        if records.is_empty() {
            if run.is_some_and(|read| read(source)) {
                cold.end();
                return;
            }
            continue;
        }
        // The decode ingest would have done, with this source's own encoding: a captured record is text, however it
        // was captured.
        let decoded = crate::runtime::decode_records(&records, meta);
        let text: Vec<&str> = decoded.text.iter().map(AsRef::as_ref).collect();
        // Labeled by the source's own name rather than a positional id, which `Subjects::resolve` leaves as it is:
        // the source may be in no deployment.
        metrics.incr(
            "smeltr_capture_records_total",
            &[("source", &cold.config.id)],
            records.len() as u64,
        );
        cold.offer(&records, &text);
    }
}

/// Run: the capture of an SQL source. The worker describes the query's result before it reads, and reports its
/// columns, and what the database plans for a poll, beside the rows. A source that declares no fields yet, or fields its
/// query no longer returns, reads every column as its suggested type, so Run can show a query's rows before its author
/// has chosen anything.
///
/// It reads from the beginning, at most the rows the capture wants a poll, and never commits.
fn run_sql_capture(cold: &Cold, registry: &Arc<crate::metrics::Metrics>, secrets: &crate::plan::topology::HeldSecrets) {
    use crate::plan::topology::SqlDatabase;
    let invitation = &cold.admitted.invitation;
    let (kind, sql) = cold.config.kind.sql().expect("only an SQL source is run here");
    // A source with fields declared is loaded as a save loads it, own order and all (`source_kind_of`, below).
    let query = match crate::io::sql::Query::parse_for_run(&sql.query, kind.dialect()) {
        Ok(query) => query,
        Err(why) => return cold.fail(format!("the query: {why}")),
    };

    let mut secrets = secrets.clone();
    secrets.take(invitation.secrets.clone());
    let database = crate::config_file::sql_connection(&cold.config).expect("an SQL source names a database");
    let place = database.address();
    let opened = match database {
        SqlDatabase::Postgres(connection) => crate::io::postgres::PostgresDriver::new(connection, &secrets)
            .map(|driver| run_sql_capture_on(cold, registry, driver, query, place)),
        SqlDatabase::MySql(connection) => crate::io::mysql::MySqlDriver::new(connection, &secrets)
            .map(|driver| run_sql_capture_on(cold, registry, driver, query, place)),
    };
    if let Err(e) = opened {
        cold.fail(format!("opening the source failed: {e}"));
    }
}

/// `run_sql_capture` once its driver is open, `place` the database it reads.
fn run_sql_capture_on<D: crate::io::sql_source::Driver + 'static>(
    cold: &Cold,
    registry: &Arc<crate::metrics::Metrics>,
    mut driver: D,
    query: crate::io::sql::Query,
    place: String,
) {
    use crate::io::sql::Poll;
    use crate::io::sql_source::{Mode, SqlSource, SqlSourceConfig};
    let invitation = &cold.admitted.invitation;
    let dto = &cold.config;
    let (kind, sql) = dto.kind.sql().expect("only an SQL source is run here");
    // Every mode reads what the capture wants a poll: Run shows the query's first rows, never its whole result.
    let wanted = u32::try_from(invitation.max_matches).unwrap_or(u32::MAX).max(1);
    let declared: Vec<String> = sql.fields.iter().map(|f| f.name.clone()).collect();
    let poll_interval = std::time::Duration::from_secs(sql.poll_interval_secs.0);
    let typed: Vec<(String, DataType)> = (sql.fields.iter())
        .filter_map(|f| {
            crate::expr::parse::parse_type(&f.ty)
                .ok()
                .map(|ty| (f.name.clone(), ty))
        })
        .collect();
    let whole = crate::io::sql::statement(
        &query,
        &Poll {
            position: &[],
            after: false,
            start: false,
            settle_secs: None,
            batch: wanted,
            fields: &typed,
        },
    );
    let described = match crate::io::sql_source::Driver::describe(&mut driver, &whole) {
        Ok(described) => described,
        Err(why) => return cold.fail(why),
    };
    let columns: Vec<crate::heartbeat::SqlColumn> = described
        .iter()
        .map(|d| crate::heartbeat::SqlColumn {
            name: d.name.clone(),
            database_type: d.database_type.clone(),
            suggested: d.suggested.as_ref().unwrap_or(&DataType::Str).spell(),
            table: d.table.clone(),
            unique: d.unique,
        })
        .collect();

    // A query edited since its fields were declared may no longer return them. Run reads it as one that declares
    // none, so its author sees the new columns rather than a poll that refuses them.
    let declares_none = declared.is_empty() || !declared.iter().all(|name| described.iter().any(|d| &d.name == name));

    // The source as it would run, but from the beginning and a capture's worth at a time.
    let (mut config, meta) = if declares_none {
        let fields: Vec<(String, DataType)> = described
            .iter()
            .map(|d| (d.name.clone(), d.suggested.clone().unwrap_or(DataType::Str)))
            .collect();
        let meta = match kind.meta().declaring(fields.clone()) {
            Ok(meta) => meta,
            Err(why) => return cold.fail(format!("the query's columns cannot be fields: {why}")),
        };
        let config = SqlSourceConfig {
            query,
            mode: Mode::EveryPoll,
            fields: fields.into(),
            poll_interval,
            batch: wanted,
            place: place.into(),
        };
        (config, meta)
    } else {
        match source_kind_of(dto.clone()) {
            Ok(crate::plan::topology::SourceKind::Sql(sql)) => {
                let mut config = sql.source;
                if let Mode::Incrementing(p) | Mode::UpdatedAt(p) = &mut config.mode {
                    p.start = crate::io::sql_source::Start::Beginning;
                }
                // The batch stays the source's own until the plan is asked, so the plan is a real poll's.
                (config, sql.meta)
            }
            Ok(_) => unreachable!("an SQL source loads as an SQL one"),
            Err(why) => return cold.fail(why),
        }
    };
    let reads_past = matches!(config.mode, Mode::Incrementing(_) | Mode::UpdatedAt(_));
    if !reads_past {
        // A mode that reads the whole result is bounded by how large a result may be, and refuses one past it. With
        // no position, a poll is the query's first `wanted` rows, unordered: what Run shows of any query.
        config.mode = Mode::Incrementing(crate::io::sql_source::Position {
            columns: Vec::new(),
            settle_secs: None,
            start: crate::io::sql_source::Start::Beginning,
        });
    }
    let mut source = SqlSource::new(driver, config);
    // Only a mode that reads past the last row can be served by an index; the others read the whole result by design.
    let verdict = match reads_past.then(|| source.plan()).flatten() {
        Some(Ok(crate::io::sql_source::Verdict::Indexed { indexes, rows })) => {
            Some(crate::heartbeat::SqlVerdict::Indexed { indexes, rows })
        }
        Some(Ok(crate::io::sql_source::Verdict::FullScan { relation, rows })) => {
            Some(crate::heartbeat::SqlVerdict::FullScan { relation, rows })
        }
        Some(Err(why)) => {
            tracing::warn!(
                target: "capture.plan_failed",
                subject = %invitation.session,
                error = %why,
                "Run could not learn what the database plans for a poll"
            );
            None
        }
        None => None,
    };
    source.read_at_most(wanted);
    cold.describe(
        crate::heartbeat::SqlRun { columns, verdict },
        declares_none.then(|| meta.clone()),
    );
    let metrics = crate::metrics::MetricsHandle::new(registry.clone(), CAPTURE_LABEL);
    drive(cold, &mut source, &meta, &metrics, Some(SqlSource::read_once));
}

/// A session this worker reads a source of its own for, and the source as it reads it (`cold_source_config`). Built
/// once the invitation is known to name a source, so nothing after asks again.
struct Cold<'a> {
    captures: &'a Captures,
    admitted: &'a Admitted,
    config: crate::config_file::SourceDto,
}

impl Cold<'_> {
    fn open(&self) -> bool {
        self.captures.open(self.admitted)
    }

    fn served(&self) -> bool {
        self.captures.served(self.admitted)
    }

    fn offer(&self, records: &[SourceRecord], text: &[&str]) {
        self.captures.offer(self.admitted, records, text)
    }

    fn describe(&self, run: crate::heartbeat::SqlRun, meta: Option<SourceMeta>) {
        self.captures.describe(self.admitted, run, meta)
    }

    fn fail(&self, reason: impl Into<String>) {
        self.captures.fail(self.admitted, reason)
    }

    fn end(&self) {
        self.captures.end(self.admitted)
    }
}

/// How long a cold capture waits on one poll. Short enough that a deadline is noticed promptly on a silent source.
const POLL_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// One source DTO as the compiler's `SourceKind`.
///
/// Loaded as a one-source configuration rather than converted by hand, so a capture's source is validated exactly as
/// a deployment's is and there is one conversion in the codebase rather than two.
pub fn source_kind_of(dto: crate::config_file::SourceDto) -> Result<crate::plan::topology::SourceKind, String> {
    let file = crate::config_file::FileConfig {
        version: crate::config_file::CURRENT_VERSION,
        sources: vec![dto],
        destinations: Vec::new(),
        pipelines: Vec::new(),
        routes: Vec::new(),
        lookup_tables: Vec::new(),
        grok_patterns: Vec::new(),
        on_exception: None,
        name: None,
        schemas: vec![],
    };
    let json = serde_json::to_string(&file).map_err(|e| e.to_string())?;
    let system = crate::config_file::load_str(&json).map_err(|e| e.to_string())?;
    system
        .topology
        .sources
        .into_iter()
        .next()
        .map(|s| s.kind)
        .ok_or_else(|| "the capture's source did not load".to_string())
}

/// A worker's share of a capture, tracked so `worker::run_worker` can drive cold sources without the transform
/// threads knowing they exist.
pub type SharedCaptures = Arc<Captures>;

#[cfg(test)]
mod tests {
    use super::*;

    /// A deployment reading `sources`.
    fn deployed(sources: &[&str]) -> Arc<crate::metrics::Subjects> {
        Arc::new(crate::metrics::Subjects {
            sources: sources.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
    }
    use crate::config_file::{SourceDto, SourceKindDto};

    fn kafka_source(id: &str) -> SourceDto {
        SourceDto {
            id: id.to_string(),
            kind: SourceKindDto::Kafka {
                schema_registry: None,
                read: Default::default(),
                brokers: "b1:9092".to_string(),
                topics: vec!["orders".to_string()],
                group_id: "production-group".to_string(),
                tls: Default::default(),
                start: Default::default(),
                stop: None,
                fetch_bytes: Default::default(),
                encoding: Default::default(),
                props: Default::default(),
            },
        }
    }

    /// A capture's certificate is delivered fresh with the invitation, so it is checked here as a deploy checks the
    /// ones its commit names, before the source is ever opened: a Kafka source presents it as a client.
    #[test]
    fn a_capture_whose_certificate_cannot_be_presented_as_a_client_is_refused() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let server_only = params.self_signed(&key).unwrap().pem();

        let mut source = kafka_source("orders");
        if let SourceKindDto::Kafka { tls, props, .. } = &mut source.kind {
            tls.certificate = Some("acme".to_string());
            // The operator's, and refused without it: librdkafka defaults to plaintext, where nothing the TLS
            // settings say is used.
            props
                .properties
                .insert("security.protocol".to_string(), "SSL".to_string());
        }

        let mut inv = invitation("s1", 10, 1_000_000, 60);
        inv.source = Some(source);
        inv.secrets = vec![crate::heartbeat::SecretDelivery {
            name: "acme".to_string(),
            version: 1,
            value: key.serialize_pem(),
            chain: Some(server_only),
        }];

        let captures = Captures::default();
        let admitted = captures.admit(&[inv]).remove(0);
        run_cold_capture(
            &captures,
            &admitted,
            &Arc::new(crate::metrics::Metrics::new()),
            &Default::default(),
        );

        let reports = captures.finished("w1");
        assert_eq!(reports.len(), 1);
        match &reports[0].finished {
            Some(CaptureEnd::Failed(reason)) => {
                assert!(
                    reason.contains("orders") && reason.contains("client authentication"),
                    "{reason}"
                )
            }
            other => panic!("expected Failed to name the source, got {other:?}"),
        }
    }

    fn invitation(session: &str, max_matches: usize, max_bytes: usize, deadline_secs: u64) -> CaptureInvitation {
        CaptureInvitation {
            session: session.to_string(),
            group: "prod-west".to_string(),
            source: Some(kafka_source("orders")),
            destination: None,
            secrets: Vec::new(),
            filter: None,
            start: None,
            max_matches,
            max_bytes,
            deadline_secs,
        }
    }

    /// Every row's value in the column `name`, across `batches`, and null for a row whose batch has no such column.
    fn column(batches: &[CaptureBatch], name: &str) -> Vec<serde_json::Value> {
        batches
            .iter()
            .flat_map(|b| {
                let at = b.column(name);
                b.rows
                    .iter()
                    .map(move |r| at.map_or(serde_json::Value::Null, |i| r.values[i].clone()))
            })
            .collect()
    }

    fn rows(batches: &[CaptureBatch]) -> usize {
        batches.iter().map(|b| b.rows.len()).sum()
    }

    /// What `session` holds so far.
    fn held(captures: &Captures, session: &str) -> Vec<CaptureBatch> {
        captures.sessions.lock().unwrap()[session].batches.clone()
    }

    /// Records as a broker delivers them: consecutive offsets from `base`, which is what tells one apart from
    /// another.
    fn records(base: i64, raws: &[&str]) -> Vec<SourceRecord> {
        raws.iter()
            .enumerate()
            .map(|(i, r)| SourceRecord {
                chunk: crate::io::RecordChunk::of_bytes(r.as_bytes()),
                row: 0,
                offset: base + i as i64,
                partition: 0,
                topic: std::sync::Arc::from("orders"),
                headers: Some(Vec::new()),
            })
            .collect()
    }

    #[test]
    fn a_filter_keeps_only_matching_records() {
        let filter = Filter::compile(Some(r#"includes(_raw, "keep")"#), SourceMeta::KAFKA, "orders").expect("compiles");
        let recs = records(0, &["{\"a\":\"keep me\"}", "{\"a\":\"not this\"}"]);
        let text: Vec<&str> = vec!["{\"a\":\"keep me\"}", "{\"a\":\"not this\"}"];
        assert_eq!(filter.keeps(&recs, &text), vec![true, false]);
    }

    #[test]
    fn no_filter_keeps_everything() {
        let filter = Filter::compile(None, SourceMeta::KAFKA, "orders").expect("compiles");
        let recs = records(0, &["a", "b"]);
        assert_eq!(filter.keeps(&recs, &["a", "b"]), vec![true, true]);
    }

    /// A filter runs before any pipeline, so it reads the record as it arrived. Naming a parsed field is a compile
    /// error rather than a filter that silently matches nothing.
    #[test]
    fn a_filter_naming_a_parsed_field_is_refused() {
        let err = Filter::compile(Some("customer_id == \"c1\""), SourceMeta::KAFKA, "orders").expect_err("refused");
        assert!(err.contains("customer_id"), "{err}");
    }

    #[test]
    fn a_filter_that_is_not_a_predicate_is_refused() {
        let err = Filter::compile(Some("_raw"), SourceMeta::KAFKA, "orders").expect_err("refused");
        assert!(err.contains("must be bool"), "{err}");
    }

    #[test]
    fn a_session_stops_at_its_match_count() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 2, 1_000_000, 60)]).remove(0);
        let recs = records(0, &["one", "two", "three"]);
        captures.offer(&s1, &recs, &["one", "two", "three"]);

        let reports = captures.finished("w1");
        assert_eq!(reports.len(), 1);
        assert_eq!(rows(&reports[0].batches), 2, "the third was past the bound");
        assert_eq!(reports[0].finished, Some(CaptureEnd::Matches));
    }

    /// What `raws` cost a capture with no bound but its count: its batch's names once, and each row.
    fn captured(raws: &[&str], headers: Option<Vec<(String, Vec<u8>)>>) -> CaptureBatch {
        let captures = Captures::default();
        let s0 = captures.admit(&[invitation("s0", raws.len(), 1_000_000, 60)]).remove(0);
        let mut recs = records(0, raws);
        recs.iter_mut().for_each(|r| r.headers = headers.clone());
        captures.offer(&s0, &recs, raws);
        captures.finished("w1").remove(0).batches.remove(0)
    }

    /// Bytes bound the buffer whatever the records weigh: a hundred small records and one huge one are the same
    /// question to a worker's memory.
    #[test]
    fn a_session_stops_at_its_byte_ceiling() {
        let recs = records(0, &["12345", "67890"]);
        let one = captured(&["12345"], Some(Vec::new()));
        let ceiling = one.size() + one.rows[0].size() - 1;

        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 1000, ceiling, 60)]).remove(0);
        captures.offer(&s1, &recs, &["12345", "67890"]);

        let reports = captures.finished("w1");
        assert_eq!(rows(&reports[0].batches), 1, "the second would have passed the ceiling");
        assert_eq!(reports[0].finished, Some(CaptureEnd::Bytes));
    }

    /// A batch's names travel with it, so they weigh against the ceiling as its values do.
    #[test]
    fn a_batch_s_names_count_against_the_byte_ceiling() {
        let one = captured(&["12345"], Some(Vec::new()));
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 1000, one.size() - 1, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["12345"]), &["12345"]);

        let reports = captures.finished("w1");
        assert!(reports[0].batches.is_empty(), "{:?}", reports[0].batches);
        assert_eq!(reports[0].finished, Some(CaptureEnd::Bytes));
    }

    /// A record's headers travel with it, so they weigh against the ceiling as its text does.
    #[test]
    fn a_records_headers_count_against_the_byte_ceiling() {
        let bare = captured(&["1"], Some(Vec::new()));
        let captures = Captures::default();
        let s1 = captures
            .admit(&[invitation("s1", 1000, bare.size() + 100, 60)])
            .remove(0);
        let mut recs = records(0, &["1"]);
        recs[0].headers = Some(vec![("k".to_string(), vec![b'v'; 200])]);
        captures.offer(&s1, &recs, &["1"]);

        let reports = captures.finished("w1");
        assert!(reports[0].batches.is_empty(), "{:?}", reports[0].batches);
        assert_eq!(reports[0].finished, Some(CaptureEnd::Bytes));
    }

    /// A capture on a source delivering nothing still finishes: the deadline is checked when asked, not when a
    /// record happens to arrive.
    #[test]
    fn a_session_on_a_silent_source_finishes_at_its_deadline() {
        let captures = Captures::default();
        captures.admit(&[invitation("s1", 100, 1_000_000, 0)]);
        assert!(!captures.watching("orders"), "already past its deadline");

        let reports = captures.finished("w1");
        assert_eq!(reports.len(), 1);
        assert!(reports[0].batches.is_empty());
        assert_eq!(reports[0].finished, Some(CaptureEnd::Deadline));
    }

    /// The source-open failure `run_cold_capture` meets when the source itself refuses to build, carried as
    /// `CaptureEnd::Failed`'s reason.
    #[test]
    fn a_capture_whose_source_will_not_open_reports_why() {
        let captures = Captures::default();
        let mut bad = invitation("s1", 10, 1_000_000, 60);
        let SourceKindDto::Kafka { topics, .. } = &mut bad.source.as_mut().unwrap().kind else {
            panic!("still a kafka source");
        };
        topics.clear();
        let admitted = captures.admit(&[bad]).remove(0);

        let registry = Arc::new(crate::metrics::Metrics::new());
        run_cold_capture(&captures, &admitted, &registry, &Default::default());

        let reports = captures.finished("w1");
        assert_eq!(reports.len(), 1);
        match &reports[0].finished {
            Some(CaptureEnd::Failed(reason)) => assert!(reason.contains("topic"), "{reason}"),
            other => panic!("expected Failed to name the reason, got {other:?}"),
        }
    }

    /// A metrics source reads the process the capture runs in, and each capture holds its own cursor: a second
    /// capture on the same registry still reads every count.
    #[test]
    fn a_cold_capture_of_a_metrics_source_reports_the_process_counters() {
        let captures = Captures::default();
        let mut inv = invitation("s1", 1, 1_000_000, 10);
        inv.source = Some(SourceDto {
            id: "self".to_string(),
            kind: SourceKindDto::Metrics { bucket_seconds: 1 },
        });
        let first = captures.admit(&[inv.clone()]).remove(0);

        let registry = Arc::new(crate::metrics::Metrics::new());
        registry.incr("smeltr_rows_in_total", &[("thread", "reader-0")], 5);
        run_cold_capture(&captures, &first, &registry, &Default::default());

        let reports = captures.finished("w1");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].finished, Some(CaptureEnd::Matches), "one wanted, one caught");
        let parsed: serde_json::Value =
            serde_json::from_str(column(&reports[0].batches, "_raw")[0].as_str().unwrap()).unwrap();
        assert_eq!(parsed["metric"], "smeltr_rows_in_total");
        assert_eq!(parsed["value"], 5);

        // The first capture counted its own records too, and a bucket comes out in no set order, so the filter
        // pins which record the second one keeps.
        let mut second = inv;
        second.session = "s2".into();
        second.filter = Some(r#"includes(_raw, "smeltr_rows_in_total")"#.into());
        let second = captures.admit(&[second]).remove(0);
        run_cold_capture(&captures, &second, &registry, &Default::default());

        let reports = captures.finished("w1");
        let report = reports
            .iter()
            .find(|r| r.session == "s2")
            .expect("the second capture answered");
        assert_eq!(report.finished, Some(CaptureEnd::Matches), "one wanted, one caught");
        assert_eq!(rows(&report.batches), 1);
        let parsed: serde_json::Value =
            serde_json::from_str(column(&report.batches, "_raw")[0].as_str().unwrap()).unwrap();
        assert_eq!(parsed["metric"], "smeltr_rows_in_total");
        assert_eq!(
            parsed["value"], 5,
            "the first capture's read took the count from the second"
        );
    }

    /// An invitation repeats on every heartbeat until the leader has the report, so admitting one twice must not
    /// restart it and lose what it has.
    #[test]
    fn admitting_the_same_session_twice_starts_it_once() {
        let captures = Captures::default();
        let mut admitted = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]);
        assert_eq!(admitted.len(), 1);
        let s1 = admitted.remove(0);
        let one = records(0, &["one"]);
        captures.offer(&s1, &one, &["one"]);
        assert!(
            captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).is_empty(),
            "the second admit is a no-op"
        );

        let two = records(1, &["two"]);
        captures.offer(&s1, &two, &["two"]);
        assert_eq!(rows(&held(&captures, "s1")), 2, "nothing was lost");
    }

    /// A session still filling reports its growth, with no reason -- there isn't one yet -- and stays open
    /// afterward: a progress report is not a finished one.
    #[test]
    fn an_open_session_reports_its_growth_as_progress() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["one"]), &["one"]);

        let progress = captures.progress("w1");
        assert_eq!(progress.len(), 1);
        assert_eq!(rows(&progress[0].batches), 1);
        assert_eq!(progress[0].finished, None);
        assert!(captures.open(&s1), "a progress report does not close the session");
        assert!(captures.finished("w1").is_empty(), "not done, so nothing to finish yet");
    }

    /// Nothing new since the last progress report is nothing to say: repeating an unchanged list on every beat
    /// would be a frame spent on records the leader already has.
    #[test]
    fn a_session_with_nothing_new_reports_no_progress() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["one"]), &["one"]);

        assert_eq!(captures.progress("w1").len(), 1, "the first record is news");
        assert!(captures.progress("w1").is_empty(), "nothing has arrived since");

        captures.offer(&s1, &records(1, &["two"]), &["two"]);
        let second = captures.progress("w1");
        assert_eq!(second.len(), 1, "the second record is news again");
        assert_eq!(
            rows(&second[0].batches),
            2,
            "the whole of what is kept, not just what is new"
        );
    }

    /// A session reports the fields of the newest Avro writer's schema its records were read with.
    #[test]
    fn a_session_reports_the_fields_of_the_avro_schema_it_read_with() {
        let writer = |text: &str| Arc::new(crate::io::avro::Parsed::new(text, &[]).unwrap());
        let read = |base: i64, writer: &Arc<crate::io::avro::Parsed>| {
            let mut out = records(base, &["one"]);
            let chunk = Arc::try_unwrap(crate::io::RecordChunk::of_bytes("one")).ok().unwrap();
            out[0].chunk = Arc::new(chunk.read_with(vec![writer.clone()]));
            out
        };
        let a = writer(r#"{"type":"record","name":"A","fields":[{"name":"id","type":"long"}]}"#);
        let b = writer(r#"{"type":"record","name":"B","fields":[{"name":"n","type":"string"}]}"#);
        let names = |r: &CaptureReport| r.avro.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 3, 1_000_000, 60)]).remove(0);

        captures.offer(&s1, &read(0, &a), &["one"]);
        assert_eq!(names(&captures.progress("w1")[0]), ["id"]);
        assert!(captures.progress("w1").is_empty(), "nothing new");
        captures.offer(&s1, &read(1, &a), &["one"]);
        assert_eq!(
            names(&captures.progress("w1")[0]),
            ["id"],
            "a report carries the fields it holds"
        );

        captures.offer(&s1, &read(2, &b), &["one"]);
        assert!(!captures.open(&s1), "hit its match count");
        assert_eq!(names(&captures.finished("w1")[0]), ["n"], "the newest schema's");
    }

    /// A payload that did not read is captured with why, in its record's first field.
    #[test]
    fn a_payload_that_did_not_read_is_captured_with_why() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        let mut unread = records(0, &["one"]);
        let chunk = Arc::try_unwrap(crate::io::RecordChunk::of_bytes("one")).ok().unwrap();
        unread[0].chunk = Arc::new(chunk.with_unread(vec![(0, "no schema 9".to_string())]));
        captures.offer(&s1, &unread, &["one"]);

        let row = &held(&captures, "s1")[0].rows[0];
        assert_eq!(row.values[0], "no schema 9");
        assert_eq!(row.failed, [0]);
    }

    /// A closed session is `finished`'s to report, once, with why it stopped -- not `progress`'s, whatever it
    /// caught on the way.
    #[test]
    fn a_finished_session_reports_no_further_progress() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 1, 1_000_000, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["one"]), &["one"]);
        assert!(!captures.open(&s1), "hit its match count");

        assert!(captures.progress("w1").is_empty(), "finished, not filling");
        assert_eq!(captures.finished("w1").len(), 1, "finished still answers once");
    }

    /// A cold capture never joins the group the configuration names: doing so would rebalance production the moment
    /// a deployment existed, and this path exists for a source no deployment is running yet.
    #[test]
    fn a_cold_capture_reads_under_its_own_consumer_group() {
        let invitation = invitation("s1", 10, 1_000_000, 60);
        let source = cold_source_config(
            invitation.source.as_ref().unwrap(),
            &invitation.session,
            invitation.start,
        );
        let SourceKindDto::Kafka { group_id, start, .. } = &source.kind else {
            panic!("still a kafka source");
        };
        assert_eq!(group_id, "smeltr-capture-s1");
        assert_ne!(group_id, "production-group");
        assert_eq!(*start, crate::config_file::StartPositionDto::Latest);
    }

    /// A start the capture names is where it reads from, whatever the configuration's own start says.
    #[test]
    fn a_cold_capture_starts_where_it_asks() {
        let mut invitation = invitation("s1", 10, 1_000_000, 60);
        let at = crate::config_file::StartPositionDto::Timestamp {
            millis: 1_760_000_000_000,
        };
        invitation.start = Some(at);
        let source = cold_source_config(
            invitation.source.as_ref().unwrap(),
            &invitation.session,
            invitation.start,
        );
        let SourceKindDto::Kafka { group_id, start, .. } = &source.kind else {
            panic!("still a kafka source");
        };
        assert_eq!(group_id, "smeltr-capture-s1");
        assert_eq!(*start, at);
    }

    /// A report that does not reach the leader is offered again. Removing the session as the report was built would
    /// lose the records to a dropped connection, and let the leader's next invitation start the capture over.
    #[test]
    fn a_finished_capture_is_offered_until_the_leader_has_it() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 1, 1_000_000, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["one"]), &["one"]);

        let first = captures.finished("w1");
        assert_eq!(rows(&first[0].batches), 1);
        let again = captures.finished("w1");
        assert_eq!(again.len(), 1, "the send failed, so it is still owed");
        assert_eq!(rows(&again[0].batches), 1, "with the records it caught");

        captures.sent("s1");
        assert!(captures.finished("w1").is_empty(), "the leader has it");
    }

    /// A redeploy can make a cold capture's source hot, and for a moment both its own consumer and the transform
    /// threads read the same partition. A record is one record however many readers saw it.
    #[test]
    fn a_record_already_held_is_not_captured_twice() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        let recs = records(7, &["one", "two"]);

        captures.offer(&s1, &recs, &["one", "two"]);
        captures.tap("orders", &recs, &["one", "two"]);

        let held = held(&captures, "s1");
        assert_eq!(rows(&held), 2, "two records, read twice");
        assert_eq!(column(&held, "__source.offset"), [7, 8]);
    }

    /// A captured record keeps everything its source set, whether a pipeline reads it or not.
    #[test]
    fn a_captured_record_keeps_its_source_fields() {
        let captures = Captures::default();
        captures.admit(&[invitation("s1", 10, 1_000_000, 60)]);
        let mut stamped = crate::io::RecordChunkBuilder::new();
        let headers = Some(vec![("trace".to_string(), b"t1".to_vec())]);
        stamped.push_stamped(b"one", 7, 0, "orders".into(), headers, Some(1_700_000_000_250_000));
        let recs = stamped.finish();

        captures.tap("orders", &recs, &["one"]);

        let [batch] = &held(&captures, "s1")[..] else {
            panic!("one record, one batch");
        };
        let row: serde_json::Map<_, _> = batch
            .columns
            .iter()
            .map(|c| c.name.clone())
            .zip(batch.rows[0].values.clone())
            .collect();
        assert_eq!(
            serde_json::Value::Object(row),
            serde_json::json!({"_raw": "one", "__source.name": "orders", "__source.type": "kafka",
                "__source.topic": "orders", "__source.partition": 0, "__source.offset": 7,
                "__source.time": "2023-11-14T22:13:20.250000Z", "__source.headers": {"trace": "t1"},
                "__source.bytes": "b25l"})
        );
    }

    /// A record polled before the reader knew of the capture has no headers to show, so it is not kept.
    #[test]
    fn a_record_polled_without_its_headers_is_not_captured() {
        let captures = Captures::default();
        captures.admit(&[invitation("s1", 10, 1_000_000, 60)]);
        let mut recs = records(7, &["before", "after"]);
        recs[0].headers = None;

        captures.tap("orders", &recs, &["before", "after"]);

        assert_eq!(column(&held(&captures, "s1"), "_raw"), ["after"]);
    }

    /// The cold path exists for a source no deployment reads. Once one does, the cold consumer's loop ends and the
    /// session goes with it: the runtime is invited again and answers with a session of its own, and two answers
    /// for one worker means the leader keeps whichever lands first rather than the fuller one.
    #[test]
    fn a_cold_capture_is_retired_once_a_deployment_reads_its_source() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        assert!(!captures.is_deployed("orders"), "nothing is running it yet");

        captures.set_deployed(&deployed(&["orders"]));
        assert!(captures.is_deployed("orders"));
        assert!(
            !captures.open(&s1),
            "the cold session outlived the source it was reading"
        );
        assert!(
            captures.finished("w1").is_empty(),
            "the cold session would still have answered for this worker"
        );
        assert!(
            !captures.watching("orders"),
            "the retired session is still being filled"
        );
    }

    /// A capture that has already answered is not a race with anything: no replacement is going to produce it,
    /// and the leader's session can expire before one could try.
    #[test]
    fn a_finished_cold_capture_survives_the_deployment_taking_its_source() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 1, 1_000_000, 60)]).remove(0);
        captures.fail(&s1, "the source would not open");
        assert!(!captures.open(&s1), "the session is finished");

        captures.set_deployed(&deployed(&["orders"]));
        assert_eq!(
            captures.finished("w1").len(),
            1,
            "a finished capture was dropped instead of handed back"
        );
    }

    /// Only the sessions whose source was taken: a capture on a source the deployment does not read is still this
    /// worker's to serve.
    #[test]
    fn a_cold_capture_on_another_source_survives_a_deployment() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);

        captures.set_deployed(&deployed(&["payments"]));
        assert!(
            captures.open(&s1),
            "a session on an undeployed source was retired anyway"
        );
    }

    /// An SQL source's capture is Run, which reads the query as its author has it, so a deployment reading the source
    /// neither retires it nor fills it. A filter on a query with no fields yet waits for the query's columns.
    #[test]
    fn an_sql_run_reads_its_own_query_where_the_source_is_deployed() {
        let source: SourceDto = serde_json::from_value(serde_json::json!({
            "id": "orders", "type": "postgres", "host": "db", "database": "d", "user": "u",
            "query": "SELECT 1", "mode": {"type": "everyPoll"}, "fields": []
        }))
        .unwrap();
        let run = CaptureInvitation {
            source: Some(source),
            filter: Some("customer == \"c1\"".to_string()),
            ..invitation("s1", 10, 1_000_000, 60)
        };
        let captures = Captures::default();
        let s1 = captures.admit(&[run]).remove(0);
        assert!(
            captures.open(&s1),
            "the filter names a column the query has not been described to have yet"
        );

        captures.set_deployed(&deployed(&["orders"]));
        assert!(captures.open(&s1), "a deployment reading the source retired the Run");
        assert!(
            !captures.watching("orders"),
            "the deployment's records would fill the Run"
        );
    }

    /// What a deployment taps is only what arrives now, so a capture that starts elsewhere reads the source itself
    /// where it is deployed: the deployment neither retires it nor fills it.
    #[test]
    fn a_capture_with_a_start_reads_its_source_where_it_is_deployed() {
        let captures = Captures::default();
        let from_earliest = CaptureInvitation {
            start: Some(crate::config_file::StartPositionDto::Earliest),
            ..invitation("s1", 10, 1_000_000, 60)
        };
        assert!(from_earliest.reads_source_itself());
        let s1 = captures.admit(&[from_earliest]).remove(0);
        assert!(!captures.watching("orders"), "the deployment's records would fill it");

        captures.set_deployed(&deployed(&["orders"]));
        assert!(captures.open(&s1), "a deployment reading the source retired it");
        captures.tap("orders", &records(0, &["now"]), &["now"]);
        assert!(held(&captures, "s1").is_empty(), "it took what the deployment read");
    }

    /// The lock is worth taking only when someone is capturing, which is the question a transform thread asks on
    /// every batch.
    #[test]
    fn a_worker_running_no_capture_is_watching_nothing() {
        let captures = Captures::default();
        assert!(!captures.watching("orders"));

        captures.admit(&[invitation("s1", 10, 1_000_000, 60)]);
        assert!(captures.watching("orders"));
        captures.sent("s1");
        assert!(!captures.watching("orders"));
    }

    /// The leader wants nothing more of a session it leaves off its list, so the worker lets go of it while it is
    /// still filling. A finished one stays until its report is sent: the leader still takes its reason.
    #[test]
    fn a_session_the_leader_no_longer_lists_is_let_go() {
        let captures = Captures::default();
        let s1 = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        let mut payments = invitation("s2", 10, 1_000_000, 60);
        payments.source = Some(kafka_source("payments"));
        captures.admit(&[payments]);
        let s3 = captures.admit(&[invitation("s3", 1, 1_000_000, 60)]).remove(0);
        captures.offer(&s1, &records(0, &["one"]), &["one"]);
        captures.offer(&s3, &records(1, &["two"]), &["two"]);
        assert!(!captures.open(&s3), "s3 reached its match count");

        captures.drop_unlisted(&["s2".to_string()].into());

        assert!(!captures.holds("s1"), "an unlisted session still filling was kept");
        assert!(captures.holds("s2"), "a listed session was let go");
        assert!(
            captures.holds("s3"),
            "a finished session was let go before its report was sent"
        );
        assert!(!captures.watching("orders"), "the tap still offers to a session let go");
        assert!(captures.watching("payments"));
        assert!(
            captures.progress("w1").iter().all(|r| r.session != "s1"),
            "a session let go still reports progress"
        );
        let finished = captures.finished("w1");
        assert!(
            finished.iter().any(|r| r.session == "s3"),
            "a finished session lost its report"
        );
        assert!(finished.iter().all(|r| r.session != "s1"));
    }

    /// A cold driver sees its session let go at its next poll, and ends.
    #[test]
    fn a_cold_capture_ends_when_the_leader_stops_listing_it() {
        let captures = Arc::new(Captures::default());
        let mut inv = invitation("s1", 1_000_000, 1_000_000, 30);
        inv.source = Some(SourceDto {
            id: "self".into(),
            kind: SourceKindDto::Metrics { bucket_seconds: 1 },
        });
        let admitted = captures.admit(&[inv]).remove(0);

        let (tx, rx) = std::sync::mpsc::channel();
        let driving = captures.clone();
        std::thread::spawn(move || {
            let registry = Arc::new(crate::metrics::Metrics::new());
            run_cold_capture(&driving, &admitted, &registry, &Default::default());
            let _ = tx.send(());
        });

        std::thread::sleep(Duration::from_millis(100));
        captures.drop_unlisted(&HashSet::new());
        assert!(
            rx.recv_timeout(Duration::from_secs(3)).is_ok(),
            "the cold driver kept polling a session the leader let go"
        );
        assert!(!captures.holds("s1"));
    }

    /// A cold driver drives the admission it was started for. Once its session is let go the same id can be
    /// admitted again while the driver is still in a poll: its next look finds its own admission gone, and nothing
    /// it does -- a record, a failure -- reaches the new one.
    #[test]
    fn a_driver_of_a_session_let_go_never_drives_a_later_admission_of_it() {
        let captures = Captures::default();
        let first = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);
        captures.drop_unlisted(&HashSet::new());
        let second = captures.admit(&[invitation("s1", 10, 1_000_000, 60)]).remove(0);

        assert!(
            !captures.open(&first),
            "a driver let go of took a later admission for its own"
        );
        captures.offer(&first, &records(0, &["one"]), &["one"]);
        captures.fail(&first, "polling failed");

        assert!(
            captures.open(&second),
            "the old driver's failure ended the later admission"
        );
        assert!(
            captures.progress("w1").iter().all(|r| rows(&r.batches) == 0),
            "the old driver's records reached the later admission"
        );
    }

    /// A cold logs capture the leader stops listing ends, and its end closes its queue.
    #[test]
    fn a_cold_logs_capture_closes_its_queue_when_the_leader_stops_listing_it() {
        let _taps = crate::logs::TAPS_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let captures = Arc::new(Captures::default());
        let mut inv = invitation("s1", 10, 1_000_000, 30);
        inv.source = Some(SourceDto {
            id: "own-logs".into(),
            kind: SourceKindDto::Logs {
                min_level: Default::default(),
            },
        });
        let admitted = captures.admit(&[inv]).remove(0);

        let (tx, rx) = std::sync::mpsc::channel();
        let driving = captures.clone();
        std::thread::spawn(move || {
            let registry = Arc::new(crate::metrics::Metrics::new());
            run_cold_capture(&driving, &admitted, &registry, &Default::default());
            let _ = tx.send(());
        });

        let opened = Instant::now();
        while crate::logs::taps_open() == 0 {
            assert!(
                opened.elapsed() < Duration::from_secs(5),
                "the cold capture never opened a queue"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        captures.drop_unlisted(&HashSet::new());
        assert!(
            rx.recv_timeout(Duration::from_secs(3)).is_ok(),
            "the cold driver kept polling a session the leader let go"
        );
        assert_eq!(crate::logs::taps_open(), 0, "the capture's end left its queue open");
    }

    /// A row a destination refused and keeps, `{"n": n}`.
    fn refused(n: u64) -> crate::io::OwnedRecord {
        crate::io::OwnedRecord::from(&crate::io::OutgoingRecord::new(format!(r#"{{"n":{n}}}"#).as_bytes()))
    }

    /// A capture admitted while this worker holds refused rows for its destination takes nothing else and reports
    /// nothing until it is shown them; then it holds them first, takes ordinary rows after, and reports.
    #[test]
    fn a_capture_waits_for_the_rows_held_for_it_before_anything_else() {
        let captures = Arc::new(Captures::default());
        let holding = captures.holding(HeldAt::Destination("es".to_string()));
        captures.admit(&[at_destination("s1", "es", 10)]);
        let at = captures.at_destinations().remove(0);
        let ordinary = || vec![sent(serde_json::json!({"n": 9}), &[("n", "i64")], "r1")];

        captures.offer_sent(&at, ordinary());
        assert!(
            captures.progress("w1").is_empty(),
            "no report before the held rows are shown"
        );

        let mut shown = HashSet::new();
        captures.offer_held_at("es", &mut shown, &[refused(1)], &["refused".to_string()]);
        assert_eq!(shown, HashSet::from([at.admission]));
        captures.offer_sent(&at, ordinary());
        let report = captures.progress("w1").remove(0);
        let values: Vec<&serde_json::Value> = report.batches.iter().map(|b| &b.rows[0].values[0]).collect();
        assert_eq!(
            values,
            [&serde_json::json!("refused"), &serde_json::json!(9)],
            "held first"
        );
        drop(holding);
    }

    /// A capture with nothing held for it reports at once, empty: the leader waits for a holding worker's first
    /// report, and a worker whose rows went before they were shown has nothing else to send.
    #[test]
    fn a_capture_reports_once_at_once_when_nothing_is_held_for_it() {
        let captures = Arc::new(Captures::default());
        let holding = captures.holding(HeldAt::Destination("es".to_string()));
        captures.admit(&[at_destination("s1", "es", 10)]);
        assert!(captures.progress("w1").is_empty(), "awaiting what is held");

        drop(holding);
        let report = captures.progress("w1");
        assert_eq!(report.len(), 1, "the rows went, so it reports what it has");
        assert_eq!(rows(&report[0].batches), 0);
        assert!(captures.progress("w1").is_empty(), "one report, not one a beat");
    }

    /// The admissions a held row was shown to keep only those still open: rows can be held for as long as a
    /// destination refuses them, and captures come and go.
    #[test]
    fn a_held_row_forgets_the_captures_that_ended() {
        let captures = Arc::new(Captures::default());
        captures.admit(&[at_destination("s1", "es", 10)]);
        let at = captures.at_destinations().remove(0);
        let mut shown = HashSet::from([at.admission + 100]);
        captures.offer_held_at("es", &mut shown, &[refused(1)], &["refused".to_string()]);
        assert_eq!(
            shown,
            HashSet::from([at.admission]),
            "the ended one is gone, the open one shown"
        );
    }

    /// A capture at `destination`, of up to `max_matches` rows.
    fn at_destination(session: &str, destination: &str, max_matches: usize) -> CaptureInvitation {
        CaptureInvitation {
            source: None,
            destination: Some(destination.to_string()),
            ..invitation(session, max_matches, 1_000_000, 60)
        }
    }

    fn columns(spelled: &[(&str, &str)]) -> Vec<CaptureColumn> {
        spelled
            .iter()
            .map(|(name, ty)| CaptureColumn {
                name: name.to_string(),
                ty: ty.to_string(),
            })
            .collect()
    }

    fn object(v: serde_json::Value) -> crate::io::RecordObject {
        v.as_object().unwrap().clone()
    }

    /// `object` as a capture at a destination shows it, under `written`, placed and identified, made by `route`.
    fn sent(v: serde_json::Value, written: &[(&str, &str)], route: &str) -> (Vec<CaptureColumn>, CapturedRow) {
        let destination = [
            Some("orders-2026".to_string()),
            Some("d1".to_string()),
            Some(route.to_string()),
        ];
        sent_row(object(v), &columns(written), &[], Vec::new(), destination)
    }

    /// A row a typed source could not read as its type is captured as the source spelled it, and marked failed.
    #[test]
    fn a_value_that_failed_its_type_is_captured_as_spelled() {
        let meta = SourceMeta::KAFKA
            .declaring(vec![("id".into(), DataType::I64), ("name".into(), DataType::Str)])
            .unwrap();
        let object = |v: serde_json::Value| v.as_object().unwrap().clone();
        let none = crate::io::SourceFields::new();
        let stored = [object(serde_json::json!({"id": true, "name": "ann"}))];
        let records = meta.read_stored(&[(&stored[0], &none)]).unwrap();

        let row = source_row(&meta, &records[0], "", &none);
        assert_eq!(&row.values[..2], [serde_json::json!("true"), serde_json::json!("ann")]);
        assert_eq!(row.failed, [0]);
        assert_eq!(source_columns(&meta)[0].ty, "i64");
    }

    /// A finished row is shown under the fields its pipeline writes, typed as declared, then where the destination
    /// files it and the route that made it.
    #[test]
    fn a_sent_row_is_its_pipeline_s_fields_then_its_destination_s() {
        let (columns, row) = sent(serde_json::json!({"a": 1}), &[("a", "i64")], "audit");
        let mut expected = vec!["a"];
        expected.extend(crate::heartbeat::DESTINATION_COLUMNS);
        assert_eq!(columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), expected);
        assert_eq!(columns[0].ty, "i64", "typed as its route writes it");
        assert_eq!(
            row.values,
            [
                serde_json::json!(1),
                serde_json::json!("orders-2026"),
                serde_json::json!("d1"),
                serde_json::json!("audit"),
            ]
        );
    }

    /// An open record's keys its pipeline does not declare go in one column, as one object.
    #[test]
    fn an_open_record_s_unclaimed_keys_are_one_column() {
        let written = &[("b", "str"), (UNCLAIMED, "{...}")];
        let (_, row) = sent(serde_json::json!({"b": "x", "c": true}), written, "db");
        assert_eq!(
            row.values[..2],
            [serde_json::json!("x"), serde_json::json!({"c": true})]
        );
        let (_, row) = sent(serde_json::json!({"b": "x"}), written, "db");
        assert_eq!(row.values[1], serde_json::Value::Null, "nothing unclaimed is null");
    }

    /// A document that is not what its route declares is shown by its own keys, not cut to the route's.
    #[test]
    fn a_document_its_route_does_not_describe_shows_its_own_keys() {
        let (columns, _) = sent(serde_json::json!({"a": 1, "z": 2}), &[("a", "i64")], "logs");
        assert_eq!(
            columns[..2].iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["a", "z"]
        );
        assert_eq!(columns[1].ty, "variant");
        let (columns, _) = sent(serde_json::json!({"a.b": 1}), &[], "logs");
        assert_eq!(columns[0].name, r#"["a.b"]"#, "a key with a `.` is one key");
    }

    /// The written columns are the record type's fields, and an open record adds the unclaimed column.
    #[test]
    fn a_closed_record_writes_its_fields_and_an_open_one_adds_the_rest() {
        let fields = vec![("a".to_string(), DataType::I64)];
        let closed = written_columns(&DataType::Struct(fields.clone(), false));
        assert_eq!(closed, columns(&[("a", "i64")]));
        let open = written_columns(&DataType::Struct(fields, true));
        assert_eq!(open, columns(&[("a", "i64"), (UNCLAIMED, "{...}")]));
        assert!(written_columns(&DataType::Variant).is_empty());
    }

    /// Only an open capture at a destination is served to the transform threads, with its filter and what it still
    /// takes; a worker running none answers from its count.
    #[test]
    fn an_open_capture_at_a_destination_is_served_with_its_filter() {
        let captures = Captures::default();
        assert!(captures.at_destinations().is_empty());
        captures.admit(&[
            CaptureInvitation {
                filter: Some("a > 1".to_string()),
                start: None,
                ..at_destination("s1", "es", 10)
            },
            invitation("s2", 10, 1_000_000, 60),
        ]);
        let at = captures.at_destinations();
        assert_eq!(at.len(), 1, "a source capture is not served here");
        assert_eq!(
            (at[0].destination.as_str(), at[0].filter.as_deref()),
            ("es", Some("a > 1"))
        );
        assert_eq!(at[0].wanted, 10);
    }

    /// A capture asks for a number of rows and is done when it has them, however many more are offered.
    #[test]
    fn a_destination_capture_stops_at_its_match_count() {
        let captures = Captures::default();
        captures.send_to("es");
        captures.admit(&[at_destination("s1", "es", 2)]);
        let at = captures.at_destinations().remove(0);
        let rows: Vec<_> = (0..5)
            .map(|i| sent(serde_json::json!({"n": i}), &[("n", "i64")], "r"))
            .collect();
        captures.offer_sent(&at, rows);
        let done = captures.finished("w1");
        assert_eq!(done[0].finished, Some(CaptureEnd::Matches));
        assert_eq!(column(&done[0].batches, "n"), [0, 1]);
        assert!(
            captures.at_destinations().is_empty(),
            "a finished capture copies nothing more"
        );
    }

    /// A filter that will not compile against a route ends the capture with why.
    #[test]
    fn a_capture_failed_at_its_destination_says_why() {
        let captures = Captures::default();
        captures.admit(&[at_destination("s1", "es", 10)]);
        let at = captures.at_destinations().remove(0);
        captures.fail_at(&at, "no field `b`".to_string());
        assert_eq!(
            captures.finished("w1")[0].finished,
            Some(CaptureEnd::Failed("no field `b`".to_string()))
        );
    }

    /// A capture at a destination this worker does not send to opens nothing, and says so.
    #[test]
    fn a_capture_at_a_destination_not_sent_to_here_answers_so() {
        let captures = std::sync::Arc::new(Captures::default());
        let registry = std::sync::Arc::new(crate::metrics::Metrics::default());
        crate::worker::serve_capture(
            &captures,
            at_destination("s1", "es", 10),
            &registry,
            &Default::default(),
        );
        let done = captures.finished("w1");
        assert!(
            matches!(&done[0].finished, Some(CaptureEnd::Failed(why)) if why.contains("\"es\"")),
            "{done:?}"
        );
    }

    /// A deployment that stops sending to a destination ends the capture there with what it has, rather than leaving
    /// it to wait out its deadline.
    #[test]
    fn a_destination_capture_ends_when_its_destination_stops() {
        let captures = Captures::default();
        captures.send_to("es");
        captures.admit(&[at_destination("s1", "es", 10)]);
        let at = captures.at_destinations().remove(0);
        captures.offer_sent(&at, vec![sent(serde_json::json!({"a": 1}), &[("a", "i64")], "r")]);
        captures.stop_sending_to("es");
        assert!(!captures.sends_to("es"));
        let done = captures.finished("w1");
        assert!(matches!(&done[0].finished, Some(CaptureEnd::Failed(_))));
        assert_eq!(rows(&done[0].batches), 1, "what it caught stays");
    }

    /// An invitation naming neither a source nor a destination is answered as such, not left to its deadline.
    #[test]
    fn a_capture_naming_nothing_is_finished_before_it_starts() {
        let captures = Captures::default();
        captures.admit(&[CaptureInvitation {
            source: None,
            ..invitation("s1", 10, 1_000_000, 60)
        }]);
        assert!(matches!(
            captures.finished("w1")[0].finished,
            Some(CaptureEnd::Failed(_))
        ));
    }
}
