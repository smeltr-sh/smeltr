//! Pipeline configuration: the compiler's input, entirely pre-resolution.
//!
//! `Step` nests, so a pipeline is a straight line with conditionals, never a graph with merge points; that is what lets
//! conditionals compile to predication.

use crate::core::column::DataType;
use crate::core::error::{Category, ExceptionKind};
use crate::core::id::{DestId, FunctionId};
use crate::plan::parse_call::split_dots_default;

#[derive(Clone, Debug)]
pub struct PipelineConfig {
    pub id: usize,
    pub name: String,
    pub body: Vec<Step>,
    /// Each step's own string id, indexed by the `FunctionId` `config_file::load` assigned it -- stable across a
    /// `call` step's inlining, where the position a `FunctionId` counts is not. Every step left once calls are inlined
    /// takes a `FunctionId` and an entry here, an `if` included.
    pub step_ids: Vec<String>,
    /// The bundle's lookup tables, by id, for a `lookup` in code to name. `Err` says why a table's source does not
    /// resolve; a lookup naming it reports that.
    pub lookup_tables: std::collections::HashMap<String, Result<LookupSource, String>>,
    /// The bundle's grok patterns, for any grok in the pipeline to name.
    pub grok_patterns: GrokPatterns,
}

/// A bundle's named grok patterns, by name: what any grok in the group's pipelines can name, between its own
/// `GrokConfig::custom_patterns` and the built-ins. `config_file::load` checked that each expands on its own and
/// that none takes a built-in's name.
pub type GrokPatterns = std::collections::BTreeMap<String, String>;

/// The field a tagged or delivered row carries its exceptions in, unless the group or the route names another.
pub const DEFAULT_TAG_FIELD: &str = "smeltr.exception";

/// The answer to every exception kind -- design doc, Exception taxonomy. A row takes the strongest of its kinds'
/// answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OnException {
    /// The answer to each kind, by `ExceptionKind::index`.
    pub answers: [Answer; ExceptionKind::KINDS.len()],
    /// The field a tagged or delivered row carries its exceptions in (`output::tag_type`), after the last step. Absent
    /// on every other row. A dotted name nests. Named by the group or the route, because the consumer is someone else's
    /// schema.
    pub tag_field: String,
}

impl OnException {
    /// One answer to every kind.
    pub fn every(answer: Answer) -> Self {
        OnException {
            answers: std::array::from_fn(|_| answer.clone()),
            tag_field: DEFAULT_TAG_FIELD.to_string(),
        }
    }

    pub fn answer(&self, kind: &ExceptionKind) -> &Answer {
        &self.answers[kind.index()]
    }
}

impl Default for OnException {
    /// An encoding exception kept its value, so its row goes on as it is. A delivery exception blocks: losing data
    /// should be something an operator chose. Every other is tagged.
    fn default() -> Self {
        OnException {
            answers: ExceptionKind::KINDS.map(|(_, category)| match category {
                Category::Encoding => Answer::Ignore,
                Category::Delivery => Answer::Block,
                _ => Answer::Tag,
            }),
            tag_field: DEFAULT_TAG_FIELD.to_string(),
        }
    }
}

/// What an exception makes of its row, weakest first: a row with several exceptions takes the strongest answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer {
    /// The row goes on as it is.
    Ignore,
    /// The row goes on with its tag field set, and the delivery rules place it like any other row.
    Tag,
    /// The row is counted and let go.
    Drop,
    /// The row goes to each destination instead of where it was going, with its tag field set, in that destination's own
    /// form. The commit waits for each.
    Deliver(Vec<DestId>),
    /// The source stops until the config changes. The same rows fail the same way on every retry.
    Block,
}

impl Answer {
    /// Where this answer stands among the others: a row takes the strongest.
    fn strength(&self) -> u8 {
        match self {
            Answer::Ignore => 0,
            Answer::Tag => 1,
            Answer::Drop => 2,
            Answer::Deliver(_) => 3,
            Answer::Block => 4,
        }
    }

    /// `self` and `other` as one row's answer: the stronger of the two, and every target of both where both deliver.
    pub fn join(&mut self, other: &Answer) {
        if other.strength() > self.strength() {
            *self = other.clone();
        } else if let (Answer::Deliver(mine), Answer::Deliver(theirs)) = (self, other) {
            for t in theirs {
                if !mine.contains(t) {
                    mine.push(*t);
                }
            }
        }
    }

    /// Where this answer sends a row: none unless it delivers.
    pub fn targets(&self) -> &[DestId] {
        match self {
            Answer::Deliver(to) => to,
            _ => &[],
        }
    }

    /// The answer's name, as a config spells it.
    pub fn label(&self) -> &'static str {
        match self {
            Answer::Ignore => "ignore",
            Answer::Tag => "tag",
            Answer::Drop => "drop",
            Answer::Deliver(_) => "deliver",
            Answer::Block => "block",
        }
    }
}

/// What a destination writes of each row, as its `format` names it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Payload {
    /// One `bytes` field of the source record as it was read, byte for byte, once per source record: by default
    /// `__source.bytes`, the payload as the source read it.
    Bytes { field: std::sync::Arc<str> },
    /// The record as far as it got: the pipeline's output, or, where no pipeline ran, its decoded text as `_raw`.
    Record,
    /// One field's text.
    Text { field: std::sync::Arc<str> },
}

impl Payload {
    /// Each source record's payload as the source read it: `Bytes` of `__source.bytes`.
    pub fn source_bytes() -> Payload {
        Payload::Bytes {
            field: crate::plan::topology::SOURCE_BYTES.into(),
        }
    }
}

impl Default for Payload {
    fn default() -> Self {
        Payload::source_bytes()
    }
}

#[derive(Clone, Debug)]
pub enum Step {
    Function(FunctionConfig),
    /// A guarded region; nested guards compose into one `And`. `id` is the `if`'s own step: its condition, and
    /// the guard built from it, are that step's.
    If {
        id: FunctionId,
        cond: String,
        then: Vec<Step>,
        else_: Vec<Step>,
    },
}

#[derive(Clone, Debug)]
pub struct FunctionConfig {
    pub id: FunctionId,
    /// The same mechanism as an enclosing `If`, shown by the GUI as a field on the function row.
    pub filter: Option<String>,
    pub disabled: bool,
    pub kind: FunctionKind,
}

impl FunctionConfig {
    pub fn new(id: FunctionId, kind: FunctionKind) -> FunctionConfig {
        FunctionConfig {
            id,
            filter: None,
            disabled: false,
            kind,
        }
    }

    pub fn with_filter(mut self, f: &str) -> Self {
        self.filter = Some(f.to_string());
        self
    }
}

#[derive(Clone, Debug)]
pub enum FunctionKind {
    /// Emits nothing.
    Comment { text: String },

    /// `target = expr`, in order. Clobber-always: a reassignment binds a fresh slot.
    Eval { assignments: Vec<(String, String)> },

    /// A statement block (design doc, Language), compiled to the same instructions `Eval` emits. `let` binds a
    /// block-scoped local, never
    /// a field; a bare assignment writes whatever the name resolves to.
    Code { body: String },

    /// `live' = live && !(cond && guard)`. A blank `cond` never drops.
    Drop { cond: String },

    /// Compile-time rebinding; emits no instruction.
    Rename { pairs: Vec<(String, String)> },

    /// Structured parse of a field. Its slots are exactly `declared`, each pinned to its type; a dynamic parser
    /// (`ParserKind::is_dynamic`) lays every other key into the unclaimed fields as it arrived.
    Parser {
        input: String,
        kind: ParserKind,
        declared: Vec<DeclaredField>,
    },

    /// Join `input` against the bundle's table `table`, writing each of `outputs` as its own field. A miss writes the
    /// output's `default` (or null), never an exception.
    Lookup {
        input: String,
        table: String,
        match_kind: MatchKind,
        multiplicity: Multiplicity,
        /// The table column holding the key. Unused for `Mmdb`.
        key_column: String,
        outputs: Vec<LookupOutput>,
    },

    /// Fan out one row per element of `field`, rebinding `field` to the element type. A null or empty array, or a
    /// dropped row, contributes no rows.
    ///
    /// `keep_parent_fields: false` (the default) releases every other user-visible field, `_raw` and the unclaimed
    /// fields included: keeping an O(N)-byte record on each of its N output rows is quadratic. `__`-prefixed control fields
    /// and the step's locals are always kept.
    Unroll { field: String, keep_parent_fields: bool },
}

/// One field of a `Parser`'s `declared` list. `ty` is always the type an expression reading the field sees --
/// `DataType::DateTime` for a `time_source` field, never the primitive it is staged through.
#[derive(Clone, Debug, PartialEq)]
pub struct DeclaredField {
    pub name: String,
    pub ty: DataType,
    /// `Some` only for a declared `datetime` field: how its text reads.
    pub time_source: Option<TimeSource>,
}

/// How a declared `datetime` field's text is read. A parser never produces `DateTime` itself
/// (`format::variant`/`format::spread` narrow to a primitive only): the field is pinned to `Str`, which a
/// number arrives as too, and the convert pass after the parse runs `ops::convert::parse_time` over it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TimeSource {
    /// `parseTime` with no format: a run of digits is an epoch, its unit by magnitude; anything else is tried
    /// against the common text shapes (`ops::timeparse`).
    Auto,
    /// `parseTime` with this format. Checked against `timeparse::FORMAT_SPECIFIERS` when the bundle loads.
    Format(String),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ParserKind {
    Json {
        /// Whether a key is split at its dots into a path, rather than read as one key.
        split_dots: bool,
    },
    Csv(CsvConfig),
    KeyValue(KvConfig),
    /// ArcSight's Common Event Format.
    Cef {
        split_dots: bool,
    },
    Grok(GrokConfig),
    /// A variant's own keys, on each row: `Object.assign(o, v)` and `o: T = v` spread a variant into an object's
    /// fields. No step makes one.
    Variant,
}

impl ParserKind {
    /// Whether this parser finds keys nobody declared, which it lays into the unclaimed fields.
    ///
    /// Only JSON, key-value and CEF take a field's name from the record. Grok reads its names off the pattern and CSV off
    /// its column table, so both produce exactly `declared`. A dynamic parser is refused under a guard, and each name a
    /// step after it writes claims the record's key of that name (the parse's `dropped`).
    pub fn is_dynamic(&self) -> bool {
        match self {
            ParserKind::Json { .. } | ParserKind::KeyValue(_) | ParserKind::Cef { .. } | ParserKind::Variant => true,
            ParserKind::Csv(_) | ParserKind::Grok(_) => false,
        }
    }

    /// Whether a key this parser reads is split at its dots into a path, rather than read as one key.
    pub fn split_dots(&self) -> bool {
        match self {
            ParserKind::Json { split_dots }
            | ParserKind::Cef { split_dots }
            | ParserKind::Csv(CsvConfig { split_dots, .. })
            | ParserKind::KeyValue(KvConfig { split_dots, .. })
            | ParserKind::Grok(GrokConfig { split_dots, .. }) => *split_dots,
            ParserKind::Variant => false,
        }
    }
}

/// One `_raw` is one CSV record, and a record carries no header row of its own, so `headers` is the only thing that
/// names a column: entry `i` names column `i`, and a column past the end has no name and is dropped.
///
/// A column that holds its place and makes no field is `None`: `null` in `csv()`'s headers, `config_file`'s `"skip"`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CsvConfig {
    pub delimiter: u8,
    pub headers: Vec<Option<String>>,
    pub split_dots: bool,
    pub quote: u8,
    /// The byte that escapes a quote or itself inside a quoted field; `None` where a quote is doubled.
    pub escape: Option<u8>,
    /// A run of delimiters is one.
    pub squeeze: bool,
    /// Spaces around each unquoted field are dropped.
    pub trim: bool,
}

impl Default for CsvConfig {
    /// Spelled out rather than derived: a derived `u8` delimiter is `0`, which splits nothing, so
    /// `CsvConfig { headers, ..Default::default() }` would read a comma-separated record as one field.
    fn default() -> Self {
        CsvConfig {
            delimiter: b',',
            headers: Vec::new(),
            split_dots: split_dots_default("csv"),
            quote: b'"',
            escape: None,
            squeeze: false,
            trim: false,
        }
    }
}

/// `key1=value1 key2=value2` by default; a `quote`d value may contain `pair_sep`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct KvConfig {
    pub pair_sep: u8,
    pub kv_sep: u8,
    pub quote: Option<u8>,
    /// A backslash escapes any character in any value, quoted or not.
    pub escapes: bool,
    /// An unquoted value runs to the `pair_sep` before the next key, not to the first `pair_sep`.
    pub next_key: bool,
    pub split_dots: bool,
}

impl Default for KvConfig {
    fn default() -> Self {
        KvConfig {
            pair_sep: b' ',
            kv_sep: b'=',
            quote: Some(b'"'),
            escapes: false,
            next_key: true,
            split_dots: split_dots_default("kv"),
        }
    }
}

/// Grok's schema is read off the pattern text, so it is not dynamic: the owning `Parser`'s `declared` must list every
/// field a pattern captures. `patterns` are tried in order, first match wins; a pattern the `regex` crate cannot
/// compile is a compile error.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GrokConfig {
    pub patterns: Vec<String>,
    /// Extra named patterns, shadowing `format::grok_pattern::BUILTINS`. None takes a name the bundle's
    /// `GrokPatterns` has.
    pub custom_patterns: Vec<(String, String)>,
    pub split_dots: bool,
}

/// Where a lookup file a config names actually is.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LookupAt {
    /// A file on disk -- the config's text against the directory that declared it. What a worker's checkout gives.
    Path(std::path::PathBuf),
    /// A blob in a commit: the repository holding it, the commit, and the path within that commit. What a reader
    /// with no checkout gives.
    InCommit {
        repo: std::path::PathBuf,
        commit: String,
        path: String,
    },
}

/// A file a config names, and where it actually is.
///
/// `text` is what the config wrote and what a save writes back; `at` is where those bytes are. The two are separate
/// because a lookup file travels in the repository with the config naming it, so the same text is a different
/// absolute path on the leader and on every worker -- and, where nothing is checked out, no path at all.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LookupFile {
    pub text: String,
    pub at: LookupAt,
}

impl LookupFile {
    /// What to call the file when something about it has to be said.
    pub fn shown(&self) -> String {
        match &self.at {
            LookupAt::Path(path) => path.display().to_string(),
            LookupAt::InCommit { .. } => self.text.clone(),
        }
    }

    /// The file, mapped where it is. A lookup file is a whole document loaded once, at compile time, so this is that
    /// one load, and every plan on this host that names the same stored file shares its pages. A file on disk is its
    /// author's to change, so it is copied rather than mapped.
    pub fn map(&self) -> Result<memmap2::Mmap, crate::lookup::LookupError> {
        self.map_stored().map(|(map, _)| map)
    }

    /// `map`, and where the lookup store keeps the file, when it does.
    pub fn map_stored(&self) -> Result<(memmap2::Mmap, Option<std::path::PathBuf>), crate::lookup::LookupError> {
        let named = self.shown();
        let path = match &self.at {
            LookupAt::Path(path) => {
                return crate::lookup::store::copy(path)
                    .map(|copy| (copy, None))
                    .map_err(|e| crate::lookup::LookupError(format!("couldn't read lookup file `{named}`: {e}")));
            }
            LookupAt::InCommit { repo, commit, path } => {
                let pointer = commit_pointer(repo, commit, path, &named)?;
                let stored = crate::lookup::store::path_of(repo, &pointer.oid);
                if !stored.exists() {
                    return Err(crate::lookup::LookupError(format!(
                        "lookup file `{named}` (sha256 {}) is not in this host's lookup store",
                        pointer.oid
                    )));
                }
                stored
            }
        };
        crate::lookup::store::map(&path)
            .map(|map| (map, Some(path)))
            .map_err(|e| crate::lookup::LookupError(format!("couldn't read lookup file `{named}`: {e}")))
    }

    /// The pointer a commit holds for this file, or `None` for a file on disk, which is its own bytes.
    pub fn pointer(&self) -> Result<Option<crate::lookup::store::Pointer>, crate::lookup::LookupError> {
        match &self.at {
            LookupAt::Path(_) => Ok(None),
            LookupAt::InCommit { repo, commit, path } => commit_pointer(repo, commit, path, &self.shown()).map(Some),
        }
    }
}

fn commit_pointer(
    repo: &std::path::Path,
    commit: &str,
    path: &str,
    named: &str,
) -> Result<crate::lookup::store::Pointer, crate::lookup::LookupError> {
    let failed = |e: String| crate::lookup::LookupError(format!("couldn't read lookup file `{named}`: {e}"));
    let blob = crate::leader::git::read_blobs(repo, commit, &[path.to_string()])
        .map_err(|e| failed(e.0))?
        .pop()
        .flatten()
        .ok_or_else(|| crate::lookup::LookupError(format!("lookup file `{named}` is not in commit {commit}")))?;
    crate::lookup::store::pointer_in(&blob, named)
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LookupSource {
    /// A header-named file, loaded once at compile time.
    Csv { file: LookupFile, delimiter: u8 },
    /// A table authored in the config. `rows[i].len() == headers.len()`.
    Inline {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// A MaxMind DB file, keyed by its own IP trie: `MatchKind` must be `Cidr`, and `Multiplicity::All` is rejected.
    Mmdb { file: LookupFile },
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum MatchKind {
    /// The key column equals the input's stringified value.
    Exact,
    /// The key column holds a CIDR block; a malformed one is a compile error, an input that is not an IP is a miss.
    Cidr,
    /// The key column holds a regex, compiled at load.
    Regex,
}

/// How many rows a match returns. `Best`/`First` give `Str` outputs; `All` gives `Array(Str)`. `Best` is the longest
/// prefix for `Cidr` and the longest pattern text for `Regex`; `First` is table order; the two coincide for `Exact`.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Default)]
pub enum Multiplicity {
    #[default]
    Best,
    First,
    All,
}

/// One table column (or MMDB dot-path) mapped to an output field, with its own miss default. `None` writes null.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct LookupOutput {
    pub source: String,
    pub field: String,
    pub default: Option<String>,
    /// The field's type: a scalar, `Str` unless declared. A row table's cells and the default are checked against
    /// it before a worker runs; an MMDB value that does not fit is null on its row with a `UnparsableField`.
    pub ty: DataType,
    /// `Some` only for a declared `datetime`: how the convert pass after the lookup reads the text the match wrote.
    pub time_source: Option<TimeSource>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct CompileError {
    pub function: Option<FunctionId>,
    /// Which pipeline this came from. `FunctionId`s are local to one pipeline (`load::to_pipeline` restarts the
    /// count at every pipeline), so once more than one pipeline is compiled together a bare `function` is
    /// ambiguous without this.
    pub pipeline: Option<usize>,
    /// An eval step's assignment this error happened while compiling, by its position in the step's own
    /// assignment list. `None` for every other kind of error, including one raised elsewhere in an eval step (its
    /// filter, say) -- there is no assignment to place it at. The editor uses this to put the message on the one
    /// row it is about, rather than only on the step as a whole.
    ///
    /// By position, not by the assignment's own target field: `Eval` clobbers-always (a reassignment binds a
    /// fresh slot), so two assignments can share one target, and a name would then place every error naming it
    /// on all of them rather than the one that actually raised it.
    pub assignment_index: Option<u32>,
    /// The route this came from, by its position in the topology's routes.
    pub route: Option<usize>,
    /// The route's delivery rule whose `when` this came from, by its position in the route's own list. `None` on a
    /// route's error means its own condition.
    pub rule: Option<u32>,
    pub message: String,
}

impl CompileError {
    pub fn new(function: Option<FunctionId>, message: impl Into<String>) -> CompileError {
        CompileError {
            function,
            pipeline: None,
            assignment_index: None,
            route: None,
            rule: None,
            message: crate::expr::inline::user_names(&message.into()).into_owned(),
        }
    }

    pub fn in_pipeline(mut self, id: usize) -> Self {
        self.pipeline = Some(id);
        self
    }
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.function {
            Some(id) => write!(f, "function {id}: {}", self.message),
            None => write!(f, "{}", self.message),
        }
    }
}
