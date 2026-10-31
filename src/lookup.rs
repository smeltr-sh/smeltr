//! The Lookup function: enrich a row by matching a field's value against a table, writing one or more matched columns
//! out as ordinary top-level fields: hashmap, then MMDB CIDR, then regex — all three now live here: `RowTable`
//! covers the first and third (`MatchKind::Exact`/`Regex` over a `LookupSource::Csv`/`Inline` table), `mmdb::MmdbTable`
//! the second.
//!
//! Every table loads once, at `plan::compile::compile` time — never per batch, never per row. "Lookup files load once per
//! deployment, `Arc`" and "missing lookup file is a compile-time error" (design doc, Compiler): `Op::Call`
//! carries the already-loaded `Table` directly, so a compiled `Plan` never touches the filesystem again, and a bad
//! path, a malformed CIDR/regex row, or a corrupt MMDB file is a `CompileError`, never a row-time surprise.

pub mod mmdb;
pub mod store;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use ipnetwork::IpNetwork;
use regex::{Regex, RegexSet, RegexSetBuilder};

use crate::core::batch::Batch;
use crate::core::bits;
use crate::core::buffer::BufferPool;
use crate::core::column::{Column, DataType, StrBuilder, Validity, VarType};
use crate::core::id::SlotId;
use crate::format::csv_scan;
use crate::ops::convert::{parse_bool, parse_f64, parse_i64};
use crate::ops::timeparse;
use crate::plan::compile::Output;
use crate::plan::config::{MatchKind, Multiplicity, TimeSource};

#[derive(Debug)]
pub struct LookupError(pub String);

impl fmt::Display for LookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for LookupError {}

/// A table a lookup matches against: its cells where they already lie, and the index its `MatchKind` needs to answer
/// a key without scanning every row. Both are maps. A table read from a stored file keeps its index in the lookup
/// store beside that file, built by the first plan on a host to need it and mapped by every later plan that names the
/// same file, key column and match kind. Any other table builds its index into memory.
#[derive(Debug)]
pub struct RowTable {
    headers: Vec<String>,
    /// The CSV the table was read from. An inline table has none: its cells are all in the index.
    file: Option<memmap2::Mmap>,
    index: memmap2::Mmap,
    layout: Layout,
    match_kind: MatchKind,
    /// Pattern `i` is row `i`'s key, compiled from the cells at every load. A regex table is small.
    regex: Option<RegexSet>,
}

/// The end of a `same_key` chain, and an empty slot.
const NONE: u32 = u32::MAX;

/// The first eight bytes of an index. A change to its layout changes them, and so the index's name.
const MAGIC: &[u8; 8] = b"SMLTRIX1";

/// An index is a header of `u32` words, then these arrays, each starting on a 4-byte boundary:
/// every cell's `[start, len]` in the text, the text no file holds, each row's next row with the same key, the exact
/// table's `[hash, row]` slots, the IPv4 and IPv6 tries' `[child 0, child 1, row]` nodes, and for each column the
/// `FITS` types every one of its cells reads as.
const SPANS: usize = 0;
const OWNED: usize = 1;
const SAME_KEY: usize = 2;
const SLOTS: usize = 3;
const V4: usize = 4;
const V6: usize = 5;
const TYPES: usize = 6;
const SECTIONS: usize = 7;

/// The types an index records a column's fit to, each a bit. A datetime's fit depends on the format a lookup reads
/// it with, so it is checked cell by cell.
const FITS: [(u32, DataType); 3] = [(1, DataType::I64), (2, DataType::F64), (4, DataType::Bool)];
const ALL_FIT: u32 = 1 | 2 | 4;
/// Magic, match kind, key column, width, rows, text length, each section's length, the 64-bit ones as two words,
/// then the CRC-32 of everything after the header: damage that stays in bounds still changes it.
const HEADER_WORDS: usize = 2 + 4 + 2 + 2 * SECTIONS + 1;

/// Where an index's arrays lie, in bytes, and the shape they describe.
#[derive(Clone, Debug)]
struct Layout {
    key: usize,
    width: usize,
    rows: usize,
    sections: [std::ops::Range<usize>; SECTIONS],
}

fn kind_word(kind: MatchKind) -> u32 {
    match kind {
        MatchKind::Exact => 0,
        MatchKind::Cidr => 1,
        MatchKind::Regex => 2,
    }
}

impl Layout {
    /// `index`'s layout, if its header describes a table of this kind, key column and width over `text_len` bytes of
    /// file, and its sections fill it exactly.
    fn read(index: &[u8], kind: MatchKind, key: usize, width: usize, text_len: usize) -> Option<Layout> {
        let header: &[u32] = bytemuck::try_cast_slice(index.get(..HEADER_WORDS * 4)?).ok()?;
        let wide = |at: usize| (u64::from(header[at]) | u64::from(header[at + 1]) << 32) as usize;
        let ok = &index[..8] == MAGIC
            && header[2] == kind_word(kind)
            && header[3] as usize == key
            && header[4] as usize == width
            && wide(6) == text_len;
        let mut at = HEADER_WORDS * 4;
        let mut sections: [std::ops::Range<usize>; SECTIONS] = Default::default();
        for (i, section) in sections.iter_mut().enumerate() {
            let len = wide(8 + 2 * i);
            *section = at..at.checked_add(len)?;
            at = section.end.next_multiple_of(4);
        }
        let whole = || header[HEADER_WORDS - 1] == crc32fast::hash(&index[HEADER_WORDS * 4..]);
        (ok && at == index.len() && whole()).then_some(Layout {
            key,
            width,
            rows: header[5] as usize,
            sections,
        })
    }
}

/// `sections` behind a header for a table of this shape, ready to map.
fn pack(kind: MatchKind, key: usize, width: usize, rows: u32, text_len: usize, sections: [&[u8]; SECTIONS]) -> Vec<u8> {
    let wide = |n: usize| [n as u32, (n as u64 >> 32) as u32];
    let mut header = vec![0u32; 2];
    header.extend([kind_word(kind), key as u32, width as u32, rows]);
    header.extend(wide(text_len));
    header.extend(sections.iter().flat_map(|s| wide(s.len())));
    header.push(0);
    let mut out = bytemuck::cast_slice::<u32, u8>(&header).to_vec();
    out[..8].copy_from_slice(MAGIC);
    for section in sections {
        out.extend_from_slice(section);
        out.resize(out.len().next_multiple_of(4), 0);
    }
    let crc = crc32fast::hash(&out[HEADER_WORDS * 4..]);
    out[HEADER_WORDS * 4 - 4..HEADER_WORDS * 4].copy_from_slice(&crc.to_ne_bytes());
    out
}

/// A table's cells: each a span of the bytes it lies in, the file's text below its length and the owned text past it.
#[derive(Clone, Copy)]
struct Cells<'a> {
    text: &'a [u8],
    owned: &'a [u8],
    spans: &'a [[u32; 2]],
    width: usize,
}

impl<'a> Cells<'a> {
    fn get(&self, row: usize, col: usize) -> &'a [u8] {
        let [start, len] = self.spans[row * self.width + col];
        let (start, len) = (start as usize, len as usize);
        match start.checked_sub(self.text.len()) {
            None => &self.text[start..start + len],
            Some(owned) => &self.owned[owned..owned + len],
        }
    }

    /// Whether every span lies wholly in the text or wholly in the owned text.
    fn sound(&self) -> bool {
        let (text, owned) = (self.text.len() as u64, self.owned.len() as u64);
        self.spans.iter().all(|&[start, len]| {
            let (start, end) = (u64::from(start), u64::from(start) + u64::from(len));
            if start < text { end <= text } else { end - text <= owned }
        })
    }
}

/// Cells while a table is read, as spans of `text`.
#[derive(Default)]
struct CellsBuilder {
    owned: Vec<u8>,
    spans: Vec<[u32; 2]>,
}

impl CellsBuilder {
    /// Add `cell` as the next cell: a span of `text` when it lies there, else a copy.
    fn push(&mut self, text: &[u8], cell: &str) -> Result<(), LookupError> {
        let at = (cell.as_ptr() as usize).wrapping_sub(text.as_ptr() as usize);
        let start = if cell.is_empty() {
            0
        } else if at < text.len() {
            at
        } else {
            self.owned.extend_from_slice(cell.as_bytes());
            text.len() + self.owned.len() - cell.len()
        };
        let too_big = |_| LookupError("a lookup table holds more than 4 GB of text".to_string());
        self.spans.push([
            u32::try_from(start).map_err(too_big)?,
            u32::try_from(cell.len()).map_err(too_big)?,
        ]);
        Ok(())
    }
}

/// A table as its arrays, borrowed from its maps: what a lookup reads, row after row.
struct View<'a> {
    cells: Cells<'a>,
    key: usize,
    match_kind: MatchKind,
    same_key: &'a [u32],
    slots: &'a [[u32; 2]],
    v4: &'a [[u32; 3]],
    v6: &'a [[u32; 3]],
    types: &'a [u32],
    regex: Option<&'a RegexSet>,
}

/// The slot of an exact table holding `hash`'s key, or the empty one it would take. The table is open-addressed,
/// probed linearly from the top bits of the hash, and never full; `same` confirms a row whose low hash bits match.
fn probe(slots: &[[u32; 2]], hash: u64, same: impl Fn(u32) -> bool) -> usize {
    let mask = slots.len() - 1;
    let mut at = hash.checked_shr(64 - slots.len().trailing_zeros()).unwrap_or(0) as usize;
    loop {
        let [low, row] = slots[at];
        if row == NONE || (low == hash as u32 && same(row)) {
            return at;
        }
        at = (at + 1) & mask;
    }
}

/// The first row of each prefix in `nodes`, a binary trie walked from the top, containing the address whose top
/// bits are `bits`, shortest prefix first. Node 0 is the root, the `/0` prefix; a child of 0 is none, since nothing
/// points back at the root.
fn trie_hits(nodes: &[[u32; 3]], bits: u128, width: u8, out: &mut Vec<usize>) {
    let mut at = 0;
    for i in 0..=width {
        let [zero, one, first] = nodes[at];
        if first != NONE {
            out.push(first as usize);
        }
        let next = if i == width {
            0
        } else if (bits >> (127 - i)) & 1 == 0 {
            zero
        } else {
            one
        };
        if next == 0 {
            return;
        }
        at = next as usize;
    }
}

/// File `row` under the prefix made of the top `len` bits of `bits`.
fn trie_insert(nodes: &mut Vec<[u32; 3]>, chains: &mut Chains, bits: u128, len: u8, row: u32) {
    let mut at = 0;
    for i in 0..len {
        let bit = ((bits >> (127 - i)) & 1) as usize;
        at = match nodes[at][bit] {
            0 => {
                nodes.push([0, 0, NONE]);
                nodes[at][bit] = u32::try_from(nodes.len() - 1).expect("the trie fits in u32");
                nodes.len() - 1
            }
            next => next as usize,
        };
    }
    match nodes[at][2] {
        NONE => nodes[at][2] = row,
        first => chains.append(first, row),
    }
}

/// `same_key` while it is built, with each chain's last row so that appending is one step.
struct Chains {
    next: Vec<u32>,
    last: Vec<u32>,
}

impl Chains {
    fn new(rows: u32) -> Chains {
        Chains {
            next: vec![NONE; rows as usize],
            last: (0..rows).collect(),
        }
    }

    fn append(&mut self, first: u32, row: u32) {
        let last = &mut self.last[first as usize];
        self.next[*last as usize] = row;
        *last = row;
    }
}

/// The exact table's hash: the multiply-and-rotate rustc hashes its own short keys with. Nothing inserts into a table
/// at run time: it is built once from the author's table, and rows only probe it. A hostile key picks the slot it
/// lands in and cannot make a run longer, so the table needs no flood resistance, and SipHash was a third of the
/// lookup.
fn hash(bytes: &[u8]) -> u64 {
    let mut h = 0u64;
    let mut add = |word: u64| h = (h.rotate_left(5) ^ word).wrapping_mul(0x517c_c1b7_2722_0a95);
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for chunk in chunks {
        add(u64::from_le_bytes(*chunk));
    }
    for &b in remainder {
        add(u64::from(b));
    }
    add(bytes.len() as u64);
    h
}

impl RowTable {
    /// A table already in memory: `LookupSource::Inline`'s.
    pub fn build(
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
        key_column: &str,
        match_kind: MatchKind,
    ) -> Result<RowTable, LookupError> {
        let key = key_of(&headers, key_column)?;
        let mut cells = CellsBuilder::default();
        for (i, row) in rows.iter().enumerate() {
            check_width(i, row.len(), headers.len())?;
            for cell in row {
                cells.push(&[], cell)?;
            }
        }
        let index = anon(&index(match_kind, key, headers.len(), &[], cells)?)?;
        RowTable::open(headers, None, index, match_kind, key)
    }

    /// The table a header-named CSV file holds, `file` mapped, its cells left where they lie. `shown` names the file
    /// in errors. `stored` is the file's place in the lookup store, when it has one: its index is kept beside it. A
    /// file is a whole document read once, so the first line names the columns, unlike `ParserKind::Csv` over `_raw`,
    /// where every line is a record. Quoting is `format::csv_scan`'s RFC 4180.
    pub fn from_csv(
        shown: &str,
        file: memmap2::Mmap,
        stored: Option<&Path>,
        delimiter: u8,
        key_column: &str,
        match_kind: MatchKind,
    ) -> Result<RowTable, LookupError> {
        let (headers, rest) = csv_header(shown, &file, delimiter)?;
        let key = key_of(&headers, key_column)?;
        let kept = stored.map(|stored| index_path(stored, key_column, match_kind, delimiter));
        // An index this host kept is used when it reads as sound; anything else is built again.
        if let Some(index) = kept.as_deref().and_then(|at| store::map(at).ok())
            && Layout::read(&index, match_kind, key, headers.len(), file.len())
                .is_some_and(|layout| View::of(&file, &index, &layout, match_kind, None).is_some_and(|v| v.sound()))
        {
            return RowTable::open(headers, Some(file), index, match_kind, key);
        }
        let lines = csv_rows(shown, &file[rest..], delimiter)?;
        let mut cells = CellsBuilder::default();
        for (i, fields) in lines.enumerate() {
            let fields = fields?;
            check_width(i, fields.len(), headers.len())?;
            for field in &fields {
                cells.push(&file, &field.text)?;
            }
        }
        let built = index(match_kind, key, headers.len(), &file, cells)?;
        // A store that refuses the index costs every later compile a build, not this one its table.
        let index = match kept.and_then(|at| store::derive(&at, &built).ok()) {
            Some(index) => index,
            None => anon(&built)?,
        };
        RowTable::open(headers, Some(file), index, match_kind, key)
    }

    fn open(
        headers: Vec<String>,
        file: Option<memmap2::Mmap>,
        index: memmap2::Mmap,
        match_kind: MatchKind,
        key: usize,
    ) -> Result<RowTable, LookupError> {
        let text = file.as_deref().unwrap_or(&[]);
        let unsound = || LookupError("a lookup table's index does not read back".to_string());
        let layout = Layout::read(&index, match_kind, key, headers.len(), text.len()).ok_or_else(unsound)?;
        let view = View::of(text, &index, &layout, match_kind, None)
            .filter(View::sound)
            .ok_or_else(unsound)?;
        let regex = match match_kind {
            MatchKind::Regex => Some(regex_set(&view)?),
            MatchKind::Exact | MatchKind::Cidr => None,
        };
        Ok(RowTable {
            headers,
            file,
            index,
            layout,
            match_kind,
            regex,
        })
    }

    /// The table's arrays, for a run of lookups.
    fn view(&self) -> View<'_> {
        let text = self.file.as_deref().unwrap_or(&[]);
        View::of(text, &self.index, &self.layout, self.match_kind, self.regex.as_ref()).expect("checked at open")
    }

    pub fn column(&self, name: &str) -> Option<usize> {
        self.headers.iter().position(|h| h == name)
    }

    /// Every cell of column `col`, top to bottom.
    pub fn cells(&self, col: usize) -> impl Iterator<Item = &str> {
        let cells = self.view().cells;
        (0..self.layout.rows).map(move |r| std::str::from_utf8(cells.get(r, col)).unwrap_or("\u{fffd}"))
    }

    pub fn row_count(&self) -> usize {
        self.layout.rows
    }

    /// Whether every cell of column `col` is known to read as `ty`, without reading them. `false` says nothing.
    pub fn all_fit(&self, col: usize, ty: &DataType) -> bool {
        let fit = self.view().types[col];
        FITS.iter().any(|(bit, t)| t == ty && fit & bit != 0)
    }
}

impl<'a> View<'a> {
    fn of(
        text: &'a [u8],
        index: &'a [u8],
        layout: &Layout,
        match_kind: MatchKind,
        regex: Option<&'a RegexSet>,
    ) -> Option<View<'a>> {
        let section = |i: usize| index.get(layout.sections[i].clone());
        let view = View {
            cells: Cells {
                text,
                owned: section(OWNED)?,
                spans: bytemuck::try_cast_slice(section(SPANS)?).ok()?,
                width: layout.width,
            },
            key: layout.key,
            match_kind,
            same_key: bytemuck::try_cast_slice(section(SAME_KEY)?).ok()?,
            slots: bytemuck::try_cast_slice(section(SLOTS)?).ok()?,
            v4: bytemuck::try_cast_slice(section(V4)?).ok()?,
            v6: bytemuck::try_cast_slice(section(V6)?).ok()?,
            types: bytemuck::try_cast_slice(section(TYPES)?).ok()?,
            regex,
        };
        let shaped = layout.rows.checked_mul(layout.width) == Some(view.cells.spans.len())
            && view.same_key.len() == layout.rows
            && view.types.len() == layout.width;
        shaped.then_some(view)
    }

    /// Whether every index this view holds stays in bounds and every walk of it ends, so that an index a host kept
    /// and something since damaged gives no lookup a panic or a loop.
    fn sound(&self) -> bool {
        let rows = self.same_key.len() as u64;
        let row = |r: u32| r == NONE || u64::from(r) < rows;
        let trie = |nodes: &[[u32; 3]]| {
            let n = nodes.len() as u64;
            n > 0
                && nodes
                    .iter()
                    .all(|&[zero, one, first]| u64::from(zero) < n && u64::from(one) < n && row(first))
        };
        let shape = match self.match_kind {
            MatchKind::Exact => {
                self.slots.len().is_power_of_two()
                    && self.slots.iter().any(|s| s[1] == NONE)
                    && self.slots.iter().all(|s| row(s[1]))
            }
            MatchKind::Cidr => trie(self.v4) && trie(self.v6),
            MatchKind::Regex => true,
        };
        // A chain runs forward in table order, so it ends.
        shape
            && self.cells.sound()
            && self.types.iter().all(|&t| t & !ALL_FIT == 0)
            && self
                .same_key
                .iter()
                .enumerate()
                .all(|(r, &next)| next == NONE || next as usize > r && row(next))
    }

    /// The row indices `key` matches, into `out`, in the order/count `multiplicity` calls for (`Multiplicity`'s own
    /// doc). `Best`/`First` leave at most one; `All` can leave several, in table order. `scratch` is the caller's, so
    /// a batch of rows allocates nothing here for `Exact` and `Cidr`.
    fn match_rows(&self, key: &str, multiplicity: Multiplicity, scratch: &mut Vec<usize>, out: &mut Vec<usize>) {
        out.clear();
        match self.match_kind {
            MatchKind::Exact => {
                let h = hash(key.as_bytes());
                let row = self.slots[probe(self.slots, h, |r| {
                    self.cells.get(r as usize, self.key) == key.as_bytes()
                })][1];
                let first = (row != NONE).then_some(row as usize);
                match multiplicity {
                    Multiplicity::All => self.chain(first, out),
                    // No notion of one exact string match being "more specific" than another, so Best coincides with
                    // First here -- the first row of this key, table order.
                    Multiplicity::Best | Multiplicity::First => out.extend(first),
                }
            }
            MatchKind::Cidr => {
                scratch.clear();
                match crate::ops::netops::parse_ip(key.as_bytes()) {
                    Some(IpAddr::V4(a)) => trie_hits(self.v4, u128::from(u32::from(a)) << 96, 32, scratch),
                    Some(IpAddr::V6(a)) => trie_hits(self.v6, u128::from(a), 128, scratch),
                    None => return,
                }
                // `scratch` is the first row of each matching prefix, shortest prefix first.
                match multiplicity {
                    Multiplicity::All => {
                        scratch.iter().for_each(|&first| self.chain(Some(first), out));
                        out.sort_unstable();
                    }
                    Multiplicity::First => out.extend(scratch.iter().min()),
                    // Longest matching prefix wins -- the routing-table convention -- and the earliest of its rows.
                    Multiplicity::Best => out.extend(scratch.last()),
                }
            }
            MatchKind::Regex => {
                let set = self.regex.expect("a regex table has its set");
                let matches = set.matches(key);
                match multiplicity {
                    Multiplicity::All => out.extend(matches.iter()),
                    Multiplicity::First => out.extend(matches.iter().next()),
                    // "Best" has no universal meaning for a set of regexes the way longest-prefix does for CIDR; this
                    // engine's documented choice is the longest pattern *source text*, earliest row on a tie -- the
                    // same "more specific wins" intuition, measured the only way available without running every
                    // candidate against every other.
                    Multiplicity::Best => out.extend(
                        matches
                            .iter()
                            .max_by_key(|&i| (set.patterns()[i].len(), std::cmp::Reverse(i))),
                    ),
                }
            }
        }
    }

    /// Every row of the chain that starts at `first`, in table order.
    fn chain(&self, first: Option<usize>, out: &mut Vec<usize>) {
        let mut at = first;
        while let Some(row) = at {
            out.push(row);
            at = Some(self.same_key[row] as usize).filter(|&r| r != NONE as usize);
        }
    }
}

/// The index of a table whose cells lie in `text` and `cells`: its rows chained by key and the match kind's own
/// arrays, packed. A malformed CIDR block is refused here; a malformed regex is refused when the table opens.
fn index(
    match_kind: MatchKind,
    key: usize,
    width: usize,
    text: &[u8],
    cells: CellsBuilder,
) -> Result<Vec<u8>, LookupError> {
    let view = Cells {
        text,
        owned: &cells.owned,
        spans: &cells.spans,
        width,
    };
    let rows = (cells.spans.len().checked_div(width).unwrap_or(0) as u64)
        .try_into()
        .ok()
        .filter(|&r: &u32| r != NONE)
        .ok_or_else(|| LookupError("a lookup table has more than 4 billion rows".to_string()))?;
    let cell = |row: u32| view.get(row as usize, key);
    let mut chains = Chains::new(rows);
    let (mut slots, mut v4, mut v6) = (Vec::new(), Vec::new(), Vec::new());
    match match_kind {
        MatchKind::Exact => {
            let len = (rows as usize + rows as usize / 3 + 1).next_power_of_two();
            slots = vec![[0, NONE]; len];
            for row in 0..rows {
                let h = hash(cell(row));
                let at = probe(&slots, h, |r| cell(r) == cell(row));
                match slots[at][1] {
                    NONE => slots[at] = [h as u32, row],
                    first => chains.append(first, row),
                }
            }
        }
        MatchKind::Cidr => {
            (v4, v6) = (vec![[0, 0, NONE]], vec![[0, 0, NONE]]);
            for row in 0..rows {
                let text = std::str::from_utf8(cell(row)).unwrap_or_default().trim();
                let net: IpNetwork = text.parse().map_err(|e| {
                    LookupError(format!(
                        "lookup table row {}: `{text}` is not a valid CIDR block: {e}",
                        row + 1
                    ))
                })?;
                match net {
                    IpNetwork::V4(n) => trie_insert(
                        &mut v4,
                        &mut chains,
                        u128::from(u32::from(n.ip())) << 96,
                        n.prefix(),
                        row,
                    ),
                    IpNetwork::V6(n) => trie_insert(&mut v6, &mut chains, u128::from(n.ip()), n.prefix(), row),
                }
            }
        }
        MatchKind::Regex => {}
    }
    let types: Vec<u32> = (0..width)
        .map(|col| {
            let mut fit = ALL_FIT;
            for row in 0..rows as usize {
                let cell = std::str::from_utf8(view.get(row, col)).unwrap_or_default();
                for (bit, ty) in &FITS {
                    if fit & bit != 0 && !fits(cell, ty, None) {
                        fit &= !bit;
                    }
                }
                if fit == 0 {
                    break;
                }
            }
            fit
        })
        .collect();
    let mut sections: [&[u8]; SECTIONS] = [&[]; SECTIONS];
    sections[SPANS] = bytemuck::cast_slice(&cells.spans);
    sections[OWNED] = &cells.owned;
    sections[SAME_KEY] = bytemuck::cast_slice(&chains.next);
    sections[SLOTS] = bytemuck::cast_slice(&slots);
    sections[V4] = bytemuck::cast_slice(&v4);
    sections[V6] = bytemuck::cast_slice(&v6);
    sections[TYPES] = bytemuck::cast_slice(&types);
    Ok(pack(match_kind, key, width, rows, text.len(), sections))
}

/// A regex table's set, row `i`'s key as pattern `i`.
fn regex_set(view: &View<'_>) -> Result<RegexSet, LookupError> {
    let patterns: Vec<&str> = (0..view.same_key.len())
        .map(|r| std::str::from_utf8(view.cells.get(r, view.key)).unwrap_or_default())
        .collect();
    // Each pattern may be as large as one compiled alone.
    RegexSetBuilder::new(&patterns)
        .size_limit(patterns.len().max(1).saturating_mul(10 << 20))
        .build()
        .map_err(|e| {
            // The set says what failed, not which row; only a failure goes back over the rows to name it.
            patterns
                .iter()
                .enumerate()
                .find_map(|(i, p)| {
                    let e = Regex::new(p).err()?;
                    Some(LookupError(format!(
                        "lookup table row {}: `{p}` is not a valid regex: {e}",
                        i + 1
                    )))
                })
                .unwrap_or_else(|| LookupError(format!("lookup table's regexes don't compile as a set: {e}")))
        })
}

/// `bytes` in a private read-only map, for an index no store keeps.
fn anon(bytes: &[u8]) -> Result<memmap2::Mmap, LookupError> {
    let failed = |e: std::io::Error| LookupError(format!("couldn't hold a lookup table's index: {e}"));
    let mut map = memmap2::MmapMut::map_anon(bytes.len()).map_err(failed)?;
    map.copy_from_slice(bytes);
    map.make_read_only().map_err(failed)
}

/// Where the store keeps the index of `stored`, a CSV it holds, keyed by `key_column` under `match_kind`. The name
/// says everything the index was built from, so a kept one is never stale: the file's hash, then a hash of the rest.
fn index_path(stored: &Path, key_column: &str, match_kind: MatchKind, delimiter: u8) -> PathBuf {
    let mut built_from = MAGIC.to_vec();
    built_from.extend([kind_word(match_kind) as u8, delimiter]);
    built_from.extend_from_slice(key_column.as_bytes());
    let oid = stored.file_name().and_then(|n| n.to_str()).unwrap_or_default();
    stored.with_file_name(format!("{oid}.index-{:016x}", hash(&built_from)))
}

/// The key column's position, once the headers are known to name each column once.
fn key_of(headers: &[String], key_column: &str) -> Result<usize, LookupError> {
    // A column is named, so two of one name would leave `column` answering for both.
    if let Some(dup) = headers
        .iter()
        .enumerate()
        .find_map(|(i, h)| headers[..i].contains(h).then_some(h))
    {
        return Err(LookupError(format!("lookup table has two columns named `{dup}`")));
    }
    headers.iter().position(|h| h == key_column).ok_or_else(|| {
        LookupError(format!(
            "lookup key column `{key_column}` is not one of this table's columns ({headers:?})"
        ))
    })
}

fn check_width(row: usize, fields: usize, width: usize) -> Result<(), LookupError> {
    if fields == width {
        return Ok(());
    }
    Err(LookupError(format!(
        "lookup table row {} has {fields} field(s), expected {width} (one per header)",
        row + 1
    )))
}

/// Resolve a lookup table's `path` as its config wrote it, against the bundle that declared it.
///
/// A lookup table's `path` is an arbitrary string in the config file, and the file's contents come straight back out
/// through the table's output fields. Without this, anyone who can write a pipeline could read any file the process
/// can — `/etc/passwd` parses as a perfectly good colon-delimited table — turning "can edit a pipeline" into "can
/// read the secrets encryption key, the TLS key, and every other tenant's config".
///
/// The file travels in the repository beside the config naming it, so the same text has to work at whatever absolute
/// path the leader and each worker checked the commit out to. It resolves against `at` -- the directory of the
/// `lookup-tables.json` that declared this table, so a table in `shared/` names a file in `shared/` and a group's
/// names one in its own subtree -- and must stay inside that bundle's `lookups/`, where the GUI's upload endpoint
/// writes these files.
///
/// Both sides are canonicalized before comparing, so `..` segments and symlinks out of `lookups/` are resolved away
/// rather than trusted — the same reason `leader::git::repo_path` validates before joining. A missing file is
/// "couldn't read", not a policy error: the author's mistake is a typo.
///
/// An absolute path is refused: a table's file is committed beside the config that names it, so there is no lookup
/// file a deployment holds and the commit does not.
pub fn resolve_lookup_path(path: &str, at: &Path) -> Result<PathBuf, LookupError> {
    check_relative(path)?;
    let real = std::fs::canonicalize(at.join(path))
        .map_err(|e| LookupError(format!("couldn't read lookup file `{path}`: {e}")))?;
    let root = std::fs::canonicalize(at)
        .map_err(|e| LookupError(format!("couldn't read the bundle directory {}: {e}", at.display())))?;
    // Joined rather than canonicalized, so a `lookups` that is itself a link elsewhere confines nothing.
    let folder = root.join(crate::config_file::LOOKUPS_DIR);
    if real.starts_with(&folder) {
        return Ok(real);
    }
    Err(LookupError(format!(
        "lookup file `{path}` resolves outside the `{}/` folder of the bundle that names it ({})",
        crate::config_file::LOOKUPS_DIR,
        folder.display()
    )))
}

/// A lookup path names a file inside the bundle that declared the table, so an absolute one names a file that
/// travels with nothing. Checked wherever a path is resolved, on disk or in a commit.
pub fn check_relative(path: &str) -> Result<(), LookupError> {
    if Path::new(path).is_absolute() {
        return Err(LookupError(format!(
            "lookup file `{path}` is an absolute path. A table's file is committed beside the config that names \
             it, so name it relative to that bundle -- `lookups/<name>` is what an upload hands back"
        )));
    }
    Ok(())
}

/// A lookup table file's headers, every row checked and none kept: what an upload needs to know of a table that may
/// be gigabytes.
pub fn csv_headers(shown: &str, bytes: &[u8], delimiter: u8) -> Result<Vec<String>, LookupError> {
    let (headers, rest) = csv_header(shown, bytes, delimiter)?;
    for line in csv_rows(shown, &bytes[rest..], delimiter)? {
        line?;
    }
    Ok(headers)
}

/// A lookup file's headers, from its first line that is not blank, and where the rest begins. Only that line is read.
fn csv_header(shown: &str, bytes: &[u8], delimiter: u8) -> Result<(Vec<String>, usize), LookupError> {
    let mut at = 0;
    while at < bytes.len() {
        let end = bytes[at..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |n| at + n + 1);
        let line = std::str::from_utf8(&bytes[at..end])
            .map_err(|e| LookupError(format!("lookup file `{shown}` is not valid UTF-8: {e}")))?;
        if !line.trim().is_empty() {
            let fields = csv_scan::split_line(line.as_bytes(), delimiter)
                .ok_or_else(|| LookupError(format!("lookup file `{shown}` line 1: malformed quoting")))?;
            return Ok((fields.iter().map(|f| f.text.to_string()).collect(), end));
        }
        at = end;
    }
    Err(LookupError(format!("lookup file `{shown}` is empty")))
}

/// A lookup file's lines after its header, `rows`, each split into its fields. Blank lines are skipped.
fn csv_rows<'b>(
    shown: &str,
    rows: &'b [u8],
    delimiter: u8,
) -> Result<impl Iterator<Item = Result<Vec<csv_scan::Field<'b>>, LookupError>>, LookupError> {
    let text =
        std::str::from_utf8(rows).map_err(|e| LookupError(format!("lookup file `{shown}` is not valid UTF-8: {e}")))?;
    let shown = shown.to_string();
    let lines = text.lines().filter(|l| !l.trim().is_empty()).enumerate();
    Ok(lines.map(move |(n, line)| {
        csv_scan::split_line(line.as_bytes(), delimiter)
            .ok_or_else(|| LookupError(format!("lookup file `{shown}` line {}: malformed quoting", n + 2)))
    }))
}

/// What an `Op::Call` that looks up runs: a loaded table, how many of its rows a key takes, and each output's miss
/// value, in `outputs` order. `None` writes null.
#[derive(Clone, Debug)]
pub struct Lookup {
    pub table: std::sync::Arc<Table>,
    pub multiplicity: Multiplicity,
    pub defaults: Vec<Option<String>>,
}

/// Whether `text` reads as a value of `ty`, by the same reading the convert pass after a call gives it: a declared parser field's
/// for a number or bool, `parseTime`'s for a datetime. `Str` takes anything. The bundle loader asks this of a
/// default and the compiler of every cell in a typed row-table column, so what a worker converts always fits.
pub fn fits(text: &str, ty: &DataType, time_source: Option<&TimeSource>) -> bool {
    match ty {
        DataType::Str => true,
        DataType::I64 => parse_i64(text).is_some(),
        DataType::F64 => parse_f64(text).is_some(),
        DataType::Bool => parse_bool(text).is_some(),
        DataType::DateTime => match time_source {
            Some(TimeSource::Format(fmt)) => timeparse::parse_with_format(text, fmt).is_some(),
            _ => timeparse::parse_auto(text).is_some(),
        },
        _ => false,
    }
}

/// A loaded, ready-to-query table — `LookupSource` after compile-time loading, the same "resolve once, never touch the
/// filesystem again" shape a Grok parse's `matcher: Option<Arc<Matcher>>` already has.
#[derive(Debug)]
pub enum Table {
    Row(RowTable),
    Mmdb(mmdb::MmdbTable),
}

/// Every `DataType` a lookup key can come from — stringified the same way regardless, so a numeric field (a status
/// code, a port) is as usable a lookup key as a `Str` one without the pipeline author having to insert a `toString()`
/// cast before every numeric-keyed lookup. Not a call to `ops::convert::to_string`: that kernel stringifies a whole
/// column at once and, since a `Str` argument never type-checks against it (see its own doc), doesn't handle
/// `Str`/`Bytes` at all — the one case this function hits most, since a lookup key is usually already a string. This
/// needs one row, of any of the four types, at a time.
fn stringify_key(col: &Column, row: usize) -> Option<Cow<'_, str>> {
    match col.data_type() {
        DataType::Bool => {
            let b = col.bool().ok()?;
            b.validity
                .is_valid(row)
                .then(|| Cow::Owned(bits::get_row(b.values, row).to_string()))
        }
        DataType::I64 => {
            let c = col.i64().ok()?;
            c.validity.is_valid(row).then(|| Cow::Owned(c.values[row].to_string()))
        }
        DataType::F64 => {
            let c = col.f64().ok()?;
            c.validity.is_valid(row).then(|| Cow::Owned(c.values[row].to_string()))
        }
        // Borrowed: a `Str` column always holds text.
        DataType::Str => {
            let v = col.var().ok()?;
            v.validity.is_valid(row).then(|| String::from_utf8_lossy(v.get(row)))
        }
        // As bytes are spelled everywhere they become text.
        DataType::Bytes => {
            let v = col.var().ok()?;
            v.validity
                .is_valid(row)
                .then(|| Cow::Owned(crate::format::bytes_text::BytesForm::Base64.text(v.get(row))))
        }
        _ => None,
    }
}

/// Accumulates one output field's column, in whichever shape `Multiplicity` calls for: `Scalar` pushes at most one
/// value per row (`Str`), `Array` pushes every matched row's value per row (`Array (Str)`). An array's elements are
/// held as text until `finish`, since a batch can match more rows than it has and the child column is sized by its
/// element count.
enum OutBuilder {
    Scalar(StrBuilder),
    Array {
        /// Every element's bytes, back to back, and where each ends.
        data: Vec<u8>,
        ends: Vec<u32>,
        offsets: Vec<u32>,
    },
}

impl OutBuilder {
    fn new(pool: &mut BufferPool, rows: usize, all: bool) -> OutBuilder {
        if all {
            OutBuilder::Array {
                data: Vec::new(),
                ends: Vec::new(),
                offsets: vec![0u32; rows + 1],
            }
        } else {
            OutBuilder::Scalar(StrBuilder::new(pool, VarType::Str, rows))
        }
    }

    fn push_scalar(&mut self, pool: &mut BufferPool, value: Option<&[u8]>) {
        match (self, value) {
            (OutBuilder::Scalar(b), Some(v)) => b.push_grow(pool, v),
            (OutBuilder::Scalar(b), None) => b.push_null(),
            (OutBuilder::Array { .. }, _) => unreachable!("Scalar/Array never mixed for one field"),
        }
    }

    /// Ends one row for an `Array` builder: `values` is every matched row's text for this field, in match order
    /// (already `Best`/`First`/ `All`-filtered by the caller); an empty slice with `default` set pushes that one value
    /// instead, matching `Scalar`'s own miss behavior — otherwise the row's array is simply empty (present, not null:
    /// "an absent value is unremarkable," this engine's usual stance, `ops::arrayops::array_push`'s doc).
    fn push_array_row(&mut self, row: usize, values: &[&[u8]], default: Option<&[u8]>) {
        let OutBuilder::Array { data, ends, offsets } = self else {
            unreachable!("Array/Scalar never mixed for one field");
        };
        let mut push = |v: &[u8]| {
            data.extend_from_slice(v);
            ends.push(data.len() as u32);
        };
        if values.is_empty() {
            if let Some(d) = default {
                push(d);
            }
        } else {
            for v in values {
                push(v);
            }
        }
        offsets[row + 1] = offsets[row]
            + if values.is_empty() {
                u32::from(default.is_some())
            } else {
                values.len() as u32
            };
    }

    /// `live[r]` false makes row `r` null: a row the guard masks takes nothing, not an empty array.
    fn finish(self, pool: &mut BufferPool, live: &[bool]) -> Column {
        match self {
            OutBuilder::Scalar(b) => b.finish(),
            OutBuilder::Array { data, ends, offsets } => {
                let mut child = StrBuilder::new(pool, VarType::Str, ends.len());
                child.reserve(pool, data.len());
                let mut start = 0;
                for &end in &ends {
                    child.push(&data[start..end as usize]);
                    start = end as usize;
                }
                let rows = offsets.len() - 1;
                let mut buf = pool.take(offsets.len() * 4);
                buf.as_mut_slice::<u32>().copy_from_slice(&offsets);
                let mut validity = Validity::alloc_null(pool, rows);
                for (r, &l) in live.iter().enumerate() {
                    validity.set_valid(r, l);
                }
                Column::list_from_parts(buf, child.finish(), validity, rows)
            }
        }
    }
}

/// Run one lookup: stringify every row's `input` value (see [`stringify_key`]), match it against the table per its
/// multiplicity, and write each of `outputs`' matched column or path, as text, straight into its own slot. A row with
/// no match writes that output's own default, or null (an empty array under `All`), never an exception, as an absent
/// JSON key does.
pub fn run(
    input: SlotId,
    lookup: &Lookup,
    outputs: &[Output],
    guard: Option<SlotId>,
    batch: &mut Batch,
    pool: &mut BufferPool,
) {
    let Lookup {
        table,
        multiplicity,
        defaults,
    } = lookup;
    let (table, multiplicity) = (&**table, *multiplicity);
    let rows = batch.rows();
    let live = crate::format::spread::selected(batch, guard);
    // The key is read by row, and a `__source` field every record shares is one value.
    batch.get_mut(input).expand(pool);
    let col = batch.peek(input);

    let all = multiplicity == Multiplicity::All;
    // Resolved once, outside the row loop: which source column (Row) or dot-path segments (Mmdb) each output reads, so
    // the per-row work is pure lookup/extraction, no repeated string comparison or splitting.
    let row_columns: Vec<Option<usize>> = match table {
        Table::Row(t) => outputs.iter().map(|o| t.column(&o.name)).collect(),
        Table::Mmdb(_) => Vec::new(),
    };
    let view = match table {
        Table::Row(t) => Some(t.view()),
        Table::Mmdb(_) => None,
    };
    let paths = match table {
        Table::Row(_) => None,
        Table::Mmdb(_) => Some(mmdb::Paths::new(outputs.iter().map(|o| o.name.as_str()))),
    };
    let mut builders: Vec<OutBuilder> = outputs.iter().map(|_| OutBuilder::new(pool, rows, all)).collect();
    let mut indices: Vec<usize> = Vec::new();
    let mut scratch: Vec<usize> = Vec::new();
    // Per batch, each MMDB record already decoded: rows of one batch fall in few networks, and a record is named
    // by its offset in the file. One entry per output, in `outputs` order.
    let mut decoded: HashMap<usize, Vec<Option<String>>> = HashMap::new();

    for (row, &live) in live.iter().enumerate() {
        // A row the guard masks takes nothing: the write's blend keeps its prior there.
        if !live {
            for builder in &mut builders {
                match all {
                    true => builder.push_array_row(row, &[], None),
                    false => builder.push_scalar(pool, None),
                }
            }
            continue;
        }
        let key = stringify_key(col, row);
        match table {
            Table::Row(_) => {
                let t = view.as_ref().expect("a row table has its view");
                match key.as_deref() {
                    Some(k) => t.match_rows(k, multiplicity, &mut scratch, &mut indices),
                    None => indices.clear(),
                }
                for (i, builder) in builders.iter_mut().enumerate() {
                    let Some(col_idx) = row_columns[i] else {
                        // Validated once at compile time (`plan::compile`'s Lookup arm) -- unreachable here.
                        unreachable!("output column resolved at compile time");
                    };
                    let default = defaults[i].as_deref().map(str::as_bytes);
                    if all {
                        let values: Vec<&[u8]> = indices.iter().map(|&r| t.cells.get(r, col_idx)).collect();
                        builder.push_array_row(row, &values, default);
                    } else {
                        let value = indices.first().map(|&r| t.cells.get(r, col_idx)).or(default);
                        builder.push_scalar(pool, value);
                    }
                }
            }
            Table::Mmdb(m) => {
                let record = key
                    .as_deref()
                    .and_then(|k| crate::ops::netops::parse_ip(k.as_bytes()))
                    .and_then(|ip| m.find(ip));
                let values: Option<&Vec<Option<String>>> = record.map(|record| {
                    let offset = record.offset().expect("a found record has data");
                    let paths = paths.as_ref().expect("an MMDB table has its paths");
                    &*decoded.entry(offset).or_insert_with(|| paths.read(&record))
                });
                for (i, builder) in builders.iter_mut().enumerate() {
                    let value = values.and_then(|v| v[i].as_deref()).map(str::as_bytes);
                    let default = defaults[i].as_deref().map(str::as_bytes);
                    if all {
                        // Multiplicity is moot for Mmdb (its trie finds at most one record) -- `plan::compile` rejects
                        // `All` for this source as a compile error, so this arm is unreachable in practice; kept
                        // exhaustive rather than assumed away.
                        let texts: Vec<&[u8]> = value.into_iter().collect();
                        builder.push_array_row(row, &texts, default);
                    } else {
                        builder.push_scalar(pool, value.or(default));
                    }
                }
            }
        }
    }

    for (out, builder) in outputs.iter().zip(builders) {
        let col = builder.finish(pool, &live);
        batch.put(out.slot, col, pool);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::batch::{SLOT_RAW, raw_layout};
    use crate::core::column::{StrBuilder as SB, VarType as VT};
    use Multiplicity::{All, Best};

    fn seed_str_slot(pool: &mut BufferPool, batch: &mut Batch, slot: SlotId, values: &[Option<&str>]) {
        let mut b = SB::new(pool, VT::Str, values.len());
        b.reserve(pool, values.iter().flatten().map(|s| s.len()).sum());
        for v in values {
            match v {
                Some(s) => b.push(s.as_bytes()),
                None => b.push_null(),
            }
        }
        batch.put(slot, b.finish(), pool);
    }

    /// `raw_layout()` plus `n` extra `Str`-typed slots for this test's own output field(s) -- `Batch::put` doesn't
    /// check a slot's declared layout type against what's actually written (only its row count), so `Str` here is just
    /// a placeholder `Batch::new` fills in before `run` overwrites it, including for the `All`-mode test's `Array(Str)`
    /// output.
    fn layout_with_extra(n: usize) -> Vec<DataType> {
        let mut l = raw_layout();
        for _ in 0..n {
            l.push(DataType::Str);
        }
        l
    }

    /// Runs `table` over every row, writing each `(source, slot, default)` output as text.
    fn run_on(
        table: &std::sync::Arc<Table>,
        multiplicity: Multiplicity,
        outputs: &[(&str, SlotId, Option<&str>)],
        batch: &mut Batch,
        pool: &mut BufferPool,
    ) {
        let lookup = Lookup {
            table: std::sync::Arc::clone(table),
            multiplicity,
            defaults: outputs.iter().map(|o| o.2.map(str::to_string)).collect(),
        };
        let outputs: Vec<Output> = outputs
            .iter()
            .map(|&(name, slot, _)| Output {
                name: name.into(),
                slot,
                ty: DataType::Str,
                time_source: None,
            })
            .collect();
        run(SLOT_RAW, &lookup, &outputs, None, batch, pool);
    }

    // -- from_csv -----------------------------------------------------

    /// `text` as a CSV reaches a table: a file, read by `store::copy`.
    fn mapped(text: &[u8]) -> memmap2::Mmap {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("smeltr-csv-{}-{n}", std::process::id()));
        std::fs::write(&path, text).unwrap();
        let map = store::copy(&path).unwrap();
        std::fs::remove_file(&path).ok();
        map
    }

    fn csv(shown: &str, text: &[u8]) -> Result<RowTable, LookupError> {
        let key = text.split(|&b| b == b',' || b == b'\n').next().unwrap_or_default();
        let key = std::str::from_utf8(key).unwrap();
        RowTable::from_csv(shown, mapped(text), None, b',', key, MatchKind::Exact)
    }

    fn rows_of(t: &RowTable) -> Vec<Vec<String>> {
        (0..t.row_count())
            .map(|r| {
                (0..t.headers.len())
                    .map(|c| t.cells(c).nth(r).unwrap().to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn a_csv_reads_the_header_row_and_data_rows() {
        let t = csv("t.csv", b"code,message\n200,ok\n404,not found\n").unwrap();
        assert_eq!(t.headers, vec!["code", "message"]);
        assert_eq!(rows_of(&t), vec![vec!["200", "ok"], vec!["404", "not found"]]);
    }

    #[test]
    fn a_csv_skips_blank_lines() {
        let t = csv("t.csv", b"a,b\n1,2\n\n   \n3,4\n").unwrap();
        assert_eq!(t.row_count(), 2);
    }

    /// A cell is a span of the file when its text lies there, and a copy only when unescaping changed it.
    #[test]
    fn a_quoted_cell_reads_as_its_text() {
        let t = csv("t.csv", b"k,v\n\"a,b\",\"say \"\"hi\"\"\"\n,\"\"\n").unwrap();
        assert_eq!(rows_of(&t), vec![vec!["a,b", "say \"hi\""], vec!["", ""]]);
        assert_eq!(t.view().cells.owned, b"say \"hi\"", "only the unescaped cell is copied");
    }

    // -- RowTable::build ------------------------------------------------

    #[test]
    fn build_rejects_an_unknown_key_column() {
        let err = RowTable::build(
            vec!["a".into(), "b".into()],
            vec![vec!["1".into(), "2".into()]],
            "nosuch",
            MatchKind::Exact,
        )
        .unwrap_err();
        assert!(err.0.contains("nosuch"), "{}", err.0);
    }

    #[test]
    fn build_rejects_a_row_with_the_wrong_field_count() {
        let err = RowTable::build(
            vec!["a".into(), "b".into()],
            vec![vec!["1".into()]],
            "a",
            MatchKind::Exact,
        )
        .unwrap_err();
        assert!(err.0.contains("row 1"), "{}", err.0);
    }

    #[test]
    fn build_rejects_a_malformed_cidr_row() {
        let err = RowTable::build(
            vec!["net".into(), "v".into()],
            vec![vec!["not-a-cidr".into(), "x".into()]],
            "net",
            MatchKind::Cidr,
        )
        .unwrap_err();
        assert!(err.0.contains("not-a-cidr"), "{}", err.0);
    }

    #[test]
    fn build_rejects_a_malformed_regex_row() {
        let err = RowTable::build(
            vec!["pat".into(), "v".into()],
            vec![vec!["(unclosed".into(), "x".into()]],
            "pat",
            MatchKind::Regex,
        )
        .unwrap_err();
        assert!(err.0.contains("unclosed"), "{}", err.0);
    }

    // -- match_rows ---------------------------------------------------

    fn matched(t: &RowTable, key: &str, m: Multiplicity) -> Vec<usize> {
        let mut out = Vec::new();
        t.view().match_rows(key, m, &mut Vec::new(), &mut out);
        out
    }

    /// A key held by several rows answers all of them in table order, and its first row otherwise.
    #[test]
    fn exact_and_regex_matches_come_in_table_order() {
        let rows = |keys: &[&str]| keys.iter().map(|k| vec![k.to_string(), String::new()]).collect();
        let t = RowTable::build(
            vec!["k".into(), "v".into()],
            rows(&["a", "b", "a", "c", "a"]),
            "k",
            MatchKind::Exact,
        )
        .unwrap();
        assert_eq!(matched(&t, "a", All), [0, 2, 4]);
        assert_eq!(matched(&t, "a", Best), [0]);
        assert_eq!(matched(&t, "z", All), [] as [usize; 0]);

        let t = RowTable::build(
            vec!["k".into(), "v".into()],
            rows(&["x", "^a", "b", "a"]),
            "k",
            MatchKind::Regex,
        )
        .unwrap();
        assert_eq!(matched(&t, "ab", All), [1, 2, 3]);
        assert_eq!(matched(&t, "ab", Multiplicity::First), [1]);
        assert_eq!(matched(&t, "ab", Best), [1], "the longest pattern, earliest on a tie");
    }

    // -- run: exact -----------------------------------------------------

    #[test]
    fn exact_match_writes_the_matched_column_and_falls_back_on_a_miss() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 3, 0);
        seed_str_slot(&mut pool, &mut batch, SLOT_RAW, &[Some("a"), Some("z"), None]);

        let t = RowTable::build(
            vec!["k".into(), "v".into()],
            vec![vec!["a".into(), "found".into()]],
            "k",
            MatchKind::Exact,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();
        run_on(
            &table,
            Best,
            &[("v", out_slot, Some("fallback"))],
            &mut batch,
            &mut pool,
        );

        let col = batch.peek(out_slot);
        let v = col.var().unwrap();
        assert_eq!(v.get(0), b"found");
        assert_eq!(v.get(1), b"fallback", "a miss uses the default");
        assert_eq!(v.get(2), b"fallback", "a null key also falls back");
        batch.reclaim(&mut pool);
    }

    #[test]
    fn a_bool_int_float_or_bytes_key_matches_the_cell_that_holds_its_text() {
        let mut pool = BufferPool::new();
        let t = RowTable::build(
            vec!["k".into(), "v".into()],
            vec![
                vec!["true".into(), "bool".into()],
                vec!["404".into(), "int".into()],
                vec!["2.5".into(), "float".into()],
                vec!["Y2Fm6Q==".into(), "bytes".into()],
            ],
            "k",
            MatchKind::Exact,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();

        let mut b = Column::alloc(&mut pool, &DataType::Bool, 1);
        bits::set_row(b.bool_mut().unwrap().values, 0, true);
        let mut i = Column::alloc(&mut pool, &DataType::I64, 1);
        i.i64_mut().unwrap().values[0] = 404;
        let mut f = Column::alloc(&mut pool, &DataType::F64, 1);
        f.f64_mut().unwrap().values[0] = 2.5;
        // Base64, as bytes are spelled wherever they become text.
        let mut y = SB::new(&mut pool, VT::Bytes, 1);
        y.push_grow(&mut pool, b"caf\xe9");
        let y = y.finish();

        for (key, want) in [(b, "bool"), (i, "int"), (f, "float"), (y, "bytes")] {
            let mut batch = Batch::new(&layout_with_extra(1), 1, 0);
            batch.put(SLOT_RAW, key, &mut pool);
            run_on(&table, Best, &[("v", out_slot, None)], &mut batch, &mut pool);
            assert_eq!(
                batch.peek(out_slot).var().unwrap().get(0),
                want.as_bytes(),
                "the {want} key"
            );
            batch.reclaim(&mut pool);
        }
    }

    // -- run: CIDR --------------------------------------------------------

    #[test]
    fn cidr_all_writes_every_matching_row_as_an_array() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 2, 0);
        seed_str_slot(
            &mut pool,
            &mut batch,
            SLOT_RAW,
            &[Some("10.1.2.3"), Some("192.168.0.1")],
        );

        let t = RowTable::build(
            vec!["net".into(), "zone".into()],
            vec![
                vec!["10.0.0.0/8".into(), "broad".into()],
                vec!["10.1.0.0/16".into(), "narrow".into()],
            ],
            "net",
            MatchKind::Cidr,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();
        run_on(&table, All, &[("zone", out_slot, None)], &mut batch, &mut pool);
        let col = batch.peek(out_slot);
        assert_eq!(col.data_type(), DataType::Array(Box::new(DataType::Str)));
        assert_eq!(
            list_row(col, 0),
            ["broad", "narrow"],
            "every containing prefix, in table order"
        );
        assert!(
            col.list().unwrap().validity.is_valid(1),
            "no match is an empty array, not null"
        );
        assert_eq!(list_row(col, 1), Vec::<String>::new());
        batch.reclaim(&mut pool);
    }

    /// Row `row` of an `Array(Str)` column as text.
    fn list_row(col: &Column, row: usize) -> Vec<String> {
        let list = col.list().unwrap();
        let child = list.child.var().unwrap();
        (list.offsets[row] as usize..list.offsets[row + 1] as usize)
            .map(|i| String::from_utf8_lossy(child.get(i)).into_owned())
            .collect()
    }

    /// The three orderings on one table: a prefix that is not first, two rows at one prefix length, a duplicate
    /// prefix, and a `/0` catch-all. `Best` breaks the tie at the longest prefix by table order, `First` is the
    /// earliest row whatever its length, and `All` is every containing row in table order.
    #[test]
    fn cidr_orderings_hold_with_ties_duplicates_and_a_catch_all() {
        let rows = vec![
            vec!["10.0.0.0/8".into(), "b".into()],
            vec!["10.1.0.0/16".into(), "a".into()],
            vec!["10.1.0.0/16".into(), "c".into()],
            vec!["0.0.0.0/0".into(), "d".into()],
        ];
        let expect = |multiplicity: Multiplicity, want: [&str; 2]| {
            let mut pool = BufferPool::new();
            let mut batch = Batch::new(&layout_with_extra(2), 2, 0);
            seed_str_slot(&mut pool, &mut batch, SLOT_RAW, &[Some("10.1.2.3"), Some("172.16.0.1")]);
            let t = RowTable::build(vec!["net".into(), "zone".into()], rows.clone(), "net", MatchKind::Cidr).unwrap();
            let table = std::sync::Arc::new(Table::Row(t));
            let out_slot: SlotId = raw_layout().len();
            run_on(&table, multiplicity, &[("zone", out_slot, None)], &mut batch, &mut pool);
            let col = batch.peek(out_slot);
            let got: Vec<String> = if multiplicity == Multiplicity::All {
                (0..2).map(|row| list_row(col, row).join(",")).collect()
            } else {
                (0..2)
                    .map(|row| String::from_utf8_lossy(col.var().unwrap().get(row)).into_owned())
                    .collect()
            };
            assert_eq!(got, want, "{multiplicity:?}");
            batch.reclaim(&mut pool);
        };
        expect(Multiplicity::Best, ["a", "d"]);
        expect(Multiplicity::First, ["b", "d"]);
        expect(Multiplicity::All, ["b,a,c,d", "d"]);
    }

    /// An IPv6 table answers IPv6 addresses the same way, and its `::/0` does not contain an IPv4 address: the two
    /// families are kept apart, as `contains` kept them.
    #[test]
    fn cidr_answers_ipv6_and_keeps_the_families_apart() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 3, 0);
        seed_str_slot(
            &mut pool,
            &mut batch,
            SLOT_RAW,
            &[Some("2001:db8:1::5"), Some("10.0.0.1"), Some("2001:db8:1::5%eth0")],
        );
        let t = RowTable::build(
            vec!["net".into(), "zone".into()],
            vec![
                vec!["2001:db8::/32".into(), "wide".into()],
                vec!["2001:db8:1::/48".into(), "site".into()],
                vec!["::/0".into(), "any6".into()],
            ],
            "net",
            MatchKind::Cidr,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();
        run_on(&table, All, &[("zone", out_slot, None)], &mut batch, &mut pool);
        let col = batch.peek(out_slot);
        assert_eq!(list_row(col, 0), ["wide", "site", "any6"]);
        assert_eq!(
            list_row(col, 1),
            Vec::<String>::new(),
            "an IPv4 address is outside every IPv6 prefix"
        );
        assert_eq!(
            list_row(col, 2),
            ["wide", "site", "any6"],
            "a zone names a link, not part of the address"
        );
        batch.reclaim(&mut pool);
    }

    #[test]
    fn a_non_ip_key_against_a_cidr_table_is_a_miss_not_a_panic() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 1, 0);
        seed_str_slot(&mut pool, &mut batch, SLOT_RAW, &[Some("not-an-ip")]);

        let t = RowTable::build(
            vec!["net".into(), "zone".into()],
            vec![vec!["10.0.0.0/8".into(), "broad".into()]],
            "net",
            MatchKind::Cidr,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();
        run_on(
            &table,
            Best,
            &[("zone", out_slot, Some("default"))],
            &mut batch,
            &mut pool,
        );
        assert_eq!(batch.peek(out_slot).var().unwrap().get(0), b"default");
        batch.reclaim(&mut pool);
    }

    // -- run: regex ---------------------------------------------------

    #[test]
    fn regex_best_picks_the_longest_matching_pattern() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 1, 0);
        seed_str_slot(&mut pool, &mut batch, SLOT_RAW, &[Some("chrome-mobile-99")]);

        let t = RowTable::build(
            vec!["pat".into(), "agent".into()],
            vec![
                vec!["chrome".into(), "Chrome".into()],
                vec!["chrome-mobile".into(), "Chrome Mobile".into()],
            ],
            "pat",
            MatchKind::Regex,
        )
        .unwrap();
        let table = std::sync::Arc::new(Table::Row(t));
        let out_slot: SlotId = raw_layout().len();
        run_on(&table, Best, &[("agent", out_slot, None)], &mut batch, &mut pool);
        assert_eq!(batch.peek(out_slot).var().unwrap().get(0), b"Chrome Mobile");
        batch.reclaim(&mut pool);
    }

    // -- run: MMDB -------------------------------------------------------

    #[test]
    fn mmdb_lookup_extracts_dotted_paths() {
        let mut pool = BufferPool::new();
        let mut batch = Batch::new(&layout_with_extra(2), 2, 0);
        seed_str_slot(&mut pool, &mut batch, SLOT_RAW, &[Some("10.1.5.5"), Some("8.8.8.8")]);

        let m = mmdb::MmdbTable::open(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/lookup/test-data/test-city.mmdb"
        )))
        .unwrap();
        let table = std::sync::Arc::new(Table::Mmdb(m));
        let country_slot: SlotId = raw_layout().len();
        let city_slot = country_slot + 1;
        run_on(
            &table,
            Multiplicity::Best,
            &[
                ("country.iso_code", country_slot, Some("unknown")),
                ("city.names.en", city_slot, Some("unknown")),
            ],
            &mut batch,
            &mut pool,
        );
        assert_eq!(batch.peek(country_slot).var().unwrap().get(0), b"US");
        assert_eq!(batch.peek(city_slot).var().unwrap().get(0), b"Springfield");
        assert_eq!(
            batch.peek(country_slot).var().unwrap().get(1),
            b"unknown",
            "no containing network is a miss"
        );
        batch.reclaim(&mut pool);
    }

    // -- allowed roots -------------------------------------------------------

    fn temp_dir_for(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("smeltr-roots-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A table referencing what the upload endpoint just handed back has to resolve. The upload commits the file
    /// into the bundle's `lookups/` and hands back `lookups/<name>`, relative to the bundle that will declare it.
    #[test]
    fn a_file_where_the_upload_endpoint_writes_resolves() {
        let bundle = temp_dir_for("upload").join("groups").join("standalone");
        let dir = bundle.join("lookups");
        std::fs::create_dir_all(&dir).expect("the upload endpoint creates this directory the same way");
        let file = dir.join("allowed-roots-probe.csv");
        std::fs::write(&file, "a\n1\n").unwrap();
        let resolved = resolve_lookup_path("lookups/allowed-roots-probe.csv", &bundle);
        std::fs::remove_file(&file).ok();
        resolved.expect("a lookup file where uploads land must resolve");
    }

    /// A missing file reports that, not a confusing "outside its bundle" -- the author's mistake is a typo, not a
    /// policy violation.
    #[test]
    fn a_missing_file_reports_that_rather_than_a_policy_error() {
        let bundle = temp_dir_for("missing");
        std::fs::create_dir_all(&bundle).unwrap();
        let err = resolve_lookup_path("lookups/nope.csv", &bundle).expect_err("a missing file is an error");
        assert!(err.0.contains("couldn't read lookup file"), "{}", err.0);
    }

    /// A table's file is committed beside the config that names it, so there is no lookup file a deployment holds
    /// and the commit does not.
    #[test]
    fn an_absolute_path_is_refused() {
        let bundle = temp_dir_for("absolute");
        std::fs::create_dir_all(&bundle).unwrap();
        let file = bundle.join("geo.csv");
        std::fs::write(&file, "a\n1\n").unwrap();

        let err = resolve_lookup_path(file.to_str().unwrap(), &bundle)
            .expect_err("absolute is refused however readable the file is");
        assert!(err.0.contains("absolute path"), "{}", err.0);
        assert!(err.0.contains("lookups/<name>"), "{}", err.0);
    }

    /// A stored file's index is built once and kept beside it; a later table maps it, and one that does not read
    /// back is built again rather than trusted.
    #[test]
    fn a_stored_files_index_is_kept_and_rebuilt_when_damaged() {
        use std::io::Write;
        let repo = temp_dir_for("kept-index");
        let mut incoming = store::Incoming::new(&repo).unwrap();
        incoming.write_all(b"k,v\na,1\nb,2\na,3\n").unwrap();
        let stored = store::path_of(&repo, &incoming.finish(None).unwrap().oid);
        let table = || {
            let file = store::map(&stored).unwrap();
            let t = RowTable::from_csv("t.csv", file, Some(&stored), b',', "k", MatchKind::Exact).unwrap();
            let rows = [matched(&t, "a", All), matched(&t, "b", All), matched(&t, "c", All)];
            assert_eq!(rows, [vec![0, 2], vec![1], vec![]]);
        };
        let index = index_path(&stored, "k", MatchKind::Exact, b',');

        table();
        let kept = std::fs::read(&index).expect("the index is kept");
        table();
        assert_eq!(std::fs::read(&index).unwrap(), kept, "a sound index is used as it is");

        let mut damaged = kept.clone();
        let end = damaged.len();
        damaged[end - 8..].fill(0xee);
        // In bounds, and a column's recorded types no longer what its cells read as.
        let mut quiet = kept.clone();
        quiet[end - 8..].fill(0);
        assert_ne!(quiet, kept);
        for broken in [b"not an index".to_vec(), damaged, quiet] {
            std::fs::write(&index, broken).unwrap();
            table();
            assert_eq!(std::fs::read(&index).unwrap(), kept, "a damaged index is built again");
        }
        std::fs::remove_dir_all(&repo).ok();
    }

    #[test]
    fn a_column_every_cell_of_which_reads_as_a_type_is_known_to() {
        let t = csv("t.csv", b"k,n,f\na,1,1.5\nb,2,x\n").unwrap();
        assert!(t.all_fit(1, &DataType::I64) && t.all_fit(1, &DataType::F64));
        assert!(!t.all_fit(1, &DataType::Bool) && !t.all_fit(2, &DataType::F64) && !t.all_fit(0, &DataType::I64));
    }

    #[test]
    fn a_csv_that_is_not_utf8_says_so_rather_than_being_read_as_rows() {
        let err = csv("geo.csv", &[b'a', b',', b'b', b'\n', 0xff, 0xfe]).expect_err("invalid UTF-8 is not a table");
        assert!(err.0.contains("not valid UTF-8"), "{}", err.0);
    }

    /// Errors name the file whatever the caller called it, since there may be no path to display.
    #[test]
    fn an_error_names_what_the_caller_called_it() {
        let err = csv("lookups/geo.csv", b"").expect_err("empty is an error");
        assert!(err.0.contains("`lookups/geo.csv`"), "{}", err.0);
    }

    /// The point of a relative path: the same bundle text works wherever the commit was checked out. Two checkouts
    /// of one bundle at different absolute paths each resolve to their own copy.
    #[test]
    fn the_same_relative_path_resolves_in_every_checkout_of_a_bundle() {
        let leader = temp_dir_for("checkout-leader");
        let worker = temp_dir_for("checkout-worker");
        for root in [&leader, &worker] {
            std::fs::create_dir_all(root.join("lookups")).unwrap();
            std::fs::write(root.join("lookups").join("geo.csv"), "a\n1\n").unwrap();
        }

        let on_leader = resolve_lookup_path("lookups/geo.csv", &leader).unwrap();
        let on_worker = resolve_lookup_path("lookups/geo.csv", &worker).unwrap();
        assert_ne!(on_leader, on_worker, "two checkouts, two files");
        assert!(on_leader.starts_with(std::fs::canonicalize(&leader).unwrap()));
        assert!(on_worker.starts_with(std::fs::canonicalize(&worker).unwrap()));
    }

    /// The confinement a relative path gets: the `lookups/` of its own bundle, the `at` directory `resolve_lookup_path`
    /// was given. A shared table naming `../groups/west/secret.csv` is refused even though both are inside the same
    /// repository.
    #[test]
    fn a_relative_path_that_climbs_out_of_its_bundle_is_refused() {
        let repo = temp_dir_for("climb");
        let shared = repo.join("shared");
        let group = repo.join("groups").join("west");
        std::fs::create_dir_all(&shared).unwrap();
        std::fs::create_dir_all(&group).unwrap();
        std::fs::write(group.join("secret.csv"), "a\n1\n").unwrap();

        let err = resolve_lookup_path("../groups/west/secret.csv", &shared)
            .expect_err("a relative path is confined by the bundle that declared it");
        assert!(err.0.contains("outside the `lookups/` folder"), "{}", err.0);
    }

    /// A file in `lookups/` that is a link to one elsewhere in the bundle -- among the samples, say -- is refused:
    /// what would be read is where the link leads.
    #[cfg(unix)]
    #[test]
    fn a_link_out_of_the_lookups_folder_is_refused() {
        let bundle = temp_dir_for("link-out");
        std::fs::create_dir_all(bundle.join("lookups")).unwrap();
        std::fs::create_dir_all(bundle.join("samples")).unwrap();
        std::fs::write(bundle.join("samples").join("users.csv"), "a\n1\n").unwrap();
        let link = bundle.join("lookups").join("users.csv");
        std::fs::remove_file(&link).ok();
        std::os::unix::fs::symlink("../samples/users.csv", &link).unwrap();

        let err = resolve_lookup_path("lookups/users.csv", &bundle).expect_err("the link leads out of lookups/");
        assert!(err.0.contains("outside the `lookups/` folder"), "{}", err.0);
    }
}
