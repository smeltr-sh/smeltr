// A record, as the preview draws it. The table reads it as one leaf per
// column (`diffRecords`), a list in one cell. An opened row reads it as a tree
// (`diffTree`) of structs and arrays alike, drawn a page at a time
// (`recordLines`): a value's width and depth are both unbounded, so no action
// may draw more than about a page of lines, wherever they come from. Nesting
// is a path, not a wall of braces, and a group holding one thing is a longer
// name rather than a level of its own -- this data is mostly one-child chains,
// and each one costs an indent for nothing.
//
// `diff.ts` stays as it is: its shallow, top-level diff is what the change
// pages compare rows with, and this is a second reading of the same idea
// rather than a replacement for it.

import { formatDiffValue } from './diff';
import { joinName, keySpans, pushKey } from './names';

export type FieldState = 'unchanged' | 'added' | 'removed' | 'changed';

/** A leaf field, at its full path. */
export interface FieldLeaf {
  path: string;
  state: FieldState;
  before?: unknown;
  after?: unknown;
}

/** How many of each mark a part of a record holds. */
export interface Marks {
  added: number;
  changed: number;
  removed: number;
}

/** One drawn line: a struct's or an array's name, a field, or the line
 * standing for the children of `path` not yet drawn. `label` is what the line
 * shows at its depth, which is the path minus whatever an ancestor already
 * said, plus whatever a collapsed one-child group gave it. `type` is the
 * value's type as the language spells it. */
export type RecordLine =
  | {
      kind: 'group';
      path: string;
      depth: number;
      label: string;
      type: string | null;
      branch: 'struct' | 'array';
      open: boolean;
      size: number;
      marks: Marks;
    }
  | ({ kind: 'field'; depth: number; label: string; type: string | null } & FieldLeaf)
  | { kind: 'more'; path: string; depth: number; shown: number; total: number };

/** A value that is not a plain object is a leaf, arrays included: the table
 * draws a list in one cell, and only an opened record unfolds it
 * (`unfoldLists`). */
function isBranch(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function leaves(value: unknown, prefix: string, into: Map<string, unknown>): void {
  if (!isBranch(value)) {
    if (prefix !== '') into.set(prefix, value);
    return;
  }
  const keys = Object.keys(value);
  if (keys.length === 0) {
    // An empty object is a value, not an absence: it has no leaves to draw,
    // so it stands as its own leaf rather than vanishing from the record.
    if (prefix !== '') into.set(prefix, value);
    return;
  }
  for (const key of keys) {
    leaves(value[key], pushKey(prefix, key), into);
  }
}

/** A record's leaves by path: what the grid reads a cell from without diffing
 * the record. */
export function leafMap(record: unknown): Map<string, unknown> {
  const into = new Map<string, unknown>();
  leaves(record, '', into);
  return into;
}

/** Every path any of `maps` has, alphabetically -- which lands siblings
 * together for free, and keeps a column where it was last time, since the
 * order a pipeline declares its fields in moves as its steps are edited. */
export function pathsOf(maps: Iterable<Map<string, unknown>>): string[] {
  const paths = new Set<string>();
  for (const m of maps) for (const p of m.keys()) paths.add(p);
  return [...paths].sort();
}

/** Whether two values draw the same. Most are the same scalar, which `===`
 * settles without serializing either. */
export function sameValue(a: unknown, b: unknown): boolean {
  return a === b || JSON.stringify(a) === JSON.stringify(b);
}

/** One field, before and after a step, from each side's `leafMap`; undefined
 * where neither side has it. */
export function fieldAt(
  before: Map<string, unknown>,
  after: Map<string, unknown>,
  path: string,
): FieldLeaf | undefined {
  const inB = before.has(path);
  const inA = after.has(path);
  if (!inA && !inB) return undefined;
  if (!inA) return { path, state: 'removed', before: before.get(path) };
  if (!inB) return { path, state: 'added', after: after.get(path) };
  const bv = before.get(path);
  const av = after.get(path);
  return { path, state: sameValue(bv, av) ? 'unchanged' : 'changed', before: bv, after: av };
}

/** Every leaf path either side has, in path order, each said to be added,
 * removed, changed or untouched. */
export function diffRecords(before: unknown, after: unknown): FieldLeaf[] {
  const b = leafMap(before);
  const a = leafMap(after);
  return pathsOf([b, a]).map((path) => fieldAt(b, a, path)!);
}

/** A type a pipeline declares, as the data dictionary spells it
 * (`DataType::spell`): `str`, `variant`, `array<i64>`, `{ip: str, ...}`. */
export type Declared =
  | { kind: 'scalar'; name: string }
  | { kind: 'array'; of: Declared }
  | { kind: 'struct'; fields: Map<string, Declared>; open: boolean };

/** `text`, one spelled type, read back. */
export function parseDeclared(text: string): Declared {
  let at = 0;
  const skip = () => {
    while (text[at] === ' ') at++;
  };
  const read = (): Declared => {
    skip();
    if (text.startsWith('array<', at)) {
      at += 'array<'.length;
      const of = read();
      skip();
      at++; // `>`
      return { kind: 'array', of };
    }
    if (text[at] === '{') {
      at++;
      const fields = new Map<string, Declared>();
      let open = false;
      for (;;) {
        skip();
        if (text[at] === '}') break;
        if (text.startsWith('...', at)) {
          open = true;
          at += 3;
        } else {
          let key: string;
          if (text[at] === '"') {
            // A key that is not an identifier is a JSON string.
            let end = at + 1;
            while (end < text.length && text[end] !== '"') end += text[end] === '\\' ? 2 : 1;
            key = JSON.parse(text.slice(at, end + 1)) as string;
            at = end + 1;
          } else {
            const end = text.indexOf(':', at);
            key = text.slice(at, end);
            at = end;
          }
          at++; // `:`
          fields.set(key, read());
        }
        skip();
        if (text[at] === ',') at++;
      }
      at++; // `}`
      return { kind: 'struct', fields, open };
    }
    const m = /^[a-z0-9_]+/.exec(text.slice(at));
    const name = m ? m[0] : text.slice(at);
    at += name.length;
    return { kind: 'scalar', name };
  };
  return read();
}

/** The record a data dictionary lists, field by field, as one struct. A
 * field `*` says its struct is open. */
export function declaredRecord(fields: [string, string][]): Declared {
  const root: Declared = { kind: 'struct', fields: new Map(), open: false };
  for (const [path, spelled] of fields) {
    const keys = keySpans(path).map((k) => k.key);
    let at: Declared = root;
    keys.forEach((key, i) => {
      if (at.kind !== 'struct') return;
      if (i === keys.length - 1) {
        if (key === '*') at.open = true;
        else at.fields.set(key, parseDeclared(spelled));
        return;
      }
      let next = at.fields.get(key);
      if (!next) {
        next = { kind: 'struct', fields: new Map(), open: false };
        at.fields.set(key, next);
      }
      at = next;
    });
  }
  return root;
}

/** What one side of a step declares at a place in the record: a declared
 * type, `undeclared` where no declaration reaches, or `unknown` when the side
 * did not compile and so declares nothing anyone can read. */
type DeclaredAt = Declared | 'undeclared' | 'unknown';

function declaredChild(d: DeclaredAt, key: string | null): DeclaredAt {
  if (d === 'unknown' || d === 'undeclared') return d;
  if (key === null) return d.kind === 'array' ? d.of : 'undeclared';
  return d.kind === 'struct' ? (d.fields.get(key) ?? 'undeclared') : 'undeclared';
}

/** A type as a line shows it: a struct by its kind alone, since its fields
 * are drawn beneath it. */
function typeLabel(d: DeclaredAt): string | null {
  if (d === 'unknown') return null;
  if (d === 'undeclared') return 'undeclared';
  if (d.kind === 'scalar') return d.name;
  if (d.kind === 'struct') return 'struct';
  return `array<${typeLabel(d.of)}>`;
}

/** The type `declared` gives the field at `path`, as a line shows it; null when `declared` is. */
export function typeAt(declared: Declared | null, path: string): string | null {
  let d: DeclaredAt = declared ?? 'unknown';
  // An unquoted `[3]` is an index, which `keySpans` leaves its bracket on.
  for (const { key } of keySpans(path)) d = declaredChild(d, key.startsWith('[') ? null : key);
  return typeLabel(d);
}

/** One part of a record, both sides of a step at once. A leaf is a scalar, an
 * empty struct or array, or a value whose shape the step changed, which reads
 * best whole. */
export interface DiffNode {
  /** The key as the path spells it: `name`, `["a.b"]`, `[3]`. */
  key: string;
  path: string;
  kind: 'struct' | 'array' | 'leaf';
  /** As the side that has it declares it, the after side where both do; null
   * where that side's types are unknown. */
  type: string | null;
  leaf?: FieldLeaf;
  children: DiffNode[];
  marks: Marks;
  /** The lines it draws with everything beneath it open. */
  lines: number;
}

type Shape = 'struct' | 'array' | 'leaf';

function shape(v: unknown): Shape {
  if (Array.isArray(v)) return v.length > 0 ? 'array' : 'leaf';
  if (isBranch(v)) return Object.keys(v).length > 0 ? 'struct' : 'leaf';
  return 'leaf';
}

/** The record before and after a step, as one tree. `keep` says whether a
 * field the table draws as one cell is drawn at all; a struct none of whose
 * fields are kept is not either. `declared` is each side's record type, or
 * null where that side did not compile. */
export function diffTree(
  before: unknown,
  after: unknown,
  keep: (path: string) => boolean = () => true,
  declared: { before: Declared | null; after: Declared | null } = { before: null, after: null },
): DiffNode {
  // The record is a struct whatever it holds; a side that is none, or empty,
  // is a record with nothing in it.
  const has = (v: unknown) => isBranch(v) && Object.keys(v).length > 0;
  return (
    node(
      '',
      '',
      { has: has(before), value: before, declared: declared.before ?? 'unknown' },
      { has: has(after), value: after, declared: declared.after ?? 'unknown' },
      keep,
    ) ?? {
      key: '',
      path: '',
      kind: 'struct',
      type: 'struct',
      children: [],
      marks: { added: 0, changed: 0, removed: 0 },
      lines: 0,
    }
  );
}

/** One side of a step at one place in the record. */
interface Side {
  has: boolean;
  value: unknown;
  declared: DeclaredAt;
}

/** The side's child at `key`, or at an array's index `i`. */
function sideAt(side: Side, key: string | number): Side {
  const v = side.value as Record<string, unknown> & unknown[];
  const has =
    side.has &&
    (typeof key === 'number'
      ? key < (side.value as unknown[]).length
      : key in (side.value as object));
  return {
    has,
    value: has ? v[key as never] : undefined,
    declared: declaredChild(side.declared, typeof key === 'number' ? null : key),
  };
}

function node(
  key: string,
  path: string,
  b: Side,
  a: Side,
  keep: (path: string) => boolean,
): DiffNode | null {
  const sb = b.has ? shape(b.value) : null;
  const sa = a.has ? shape(a.value) : null;
  if (sb === null && sa === null) return null;
  const kind = sb === null ? sa! : sa === null ? sb : sa === sb ? sa : 'leaf';
  // A struct's fields are each the table's own column; past one, the table
  // draws the value in one cell, and keeps or drops it whole.
  if (kind !== 'struct' && !keep(path)) return null;
  const type = typeLabel(a.has ? a.declared : b.declared);
  const marks = { added: 0, changed: 0, removed: 0 };
  if (kind === 'leaf') {
    const state: FieldState = !a.has
      ? 'removed'
      : !b.has
        ? 'added'
        : sameValue(b.value, a.value)
          ? 'unchanged'
          : 'changed';
    if (state !== 'unchanged') marks[state]++;
    const leaf: FieldLeaf = { path, state };
    if (b.has) leaf.before = b.value;
    if (a.has) leaf.after = a.value;
    return { key, path, kind, type, leaf, children: [], marks, lines: 1 };
  }
  const children: DiffNode[] = [];
  if (kind === 'array') {
    const length = (s: Side) => (s.has ? (s.value as unknown[]).length : 0);
    for (let i = 0; i < Math.max(length(b), length(a)); i++) {
      const at = `[${i}]`;
      const child = node(at, path + at, sideAt(b, i), sideAt(a, i), () => true);
      if (child) children.push(child);
    }
  } else {
    const keys = (s: Side) => (s.has ? Object.keys(s.value as object) : []);
    for (const k of [...new Set([...keys(b), ...keys(a)])].sort()) {
      const p = pushKey(path, k);
      const at = p.slice(path.length).replace(/^\./, '');
      const child = node(at, p, sideAt(b, k), sideAt(a, k), keep);
      if (child) children.push(child);
    }
    if (children.length === 0) return null;
  }
  let lines = 1;
  for (const c of children) {
    marks.added += c.marks.added;
    marks.changed += c.marks.changed;
    marks.removed += c.marks.removed;
    lines += c.lines;
  }
  return { key, path, kind, type, children, marks, lines };
}

/** Whether nothing of `n` is left after the step. */
function gone(n: DiffNode): boolean {
  return n.kind === 'leaf' ? n.leaf!.state === 'removed' : n.children.every(gone);
}

/** The tree with what the step removed lifted out of its structs, and what was
 * lifted, a removed struct whole. A rename reads as its new name alone, and
 * the old names wait on one line. An array keeps what it lost, since its
 * elements say where in it they were. */
export function withoutRemoved(root: DiffNode): { tree: DiffNode; removed: DiffNode[] } {
  const removed: DiffNode[] = [];
  const prune = (n: DiffNode): DiffNode => {
    if (n.kind !== 'struct') return n;
    const children: DiffNode[] = [];
    for (const c of n.children) {
      if (gone(c)) removed.push(c);
      else children.push(prune(c));
    }
    const marks = { added: 0, changed: 0, removed: 0 };
    let lines = 1;
    for (const c of children) {
      marks.added += c.marks.added;
      marks.changed += c.marks.changed;
      marks.removed += c.marks.removed;
      lines += c.lines;
    }
    return { ...n, children, marks, lines };
  };
  return { tree: prune(root), removed };
}

/** Each leaf of `nodes`, by its whole path. */
export function leafLines(nodes: DiffNode[]): RecordLine[] {
  const out: RecordLine[] = [];
  const walk = (n: DiffNode) => {
    if (n.kind === 'leaf')
      out.push({ ...n.leaf!, kind: 'field', depth: 0, label: n.path, type: n.type });
    else n.children.forEach(walk);
  };
  nodes.forEach(walk);
  return out;
}

/** About how many lines a page draws: a record opened, a struct or an array
 * opened, or one more page of either. */
export const PAGE_LINES = 40;

/** What the reader chose, by a struct's or an array's path: whether it is
 * open, and how many pages past its first it draws. */
export interface Choices {
  open?: (path: string) => boolean | undefined;
  pages?: (path: string) => number | undefined;
}

/** A struct holding exactly one thing, absorbed into that thing's name as far
 * down as the chain goes: `destination.nat` holding only `ip` draws as
 * `nat.ip`; `source.nat` holding `ip` and `port` stays a level. An array
 * stays a level however short, so its type and size are still said. */
function chain(n: DiffNode): { label: string; node: DiffNode } {
  let label = n.key;
  while (n.kind === 'struct' && n.children.length === 1) {
    n = n.children[0];
    label = joinName(label, n.key);
  }
  return { label, node: n };
}

/** Whether a node holds anything a step added, changed or removed. */
const changed = (n: DiffNode) =>
  n.kind === 'leaf'
    ? n.leaf!.state !== 'unchanged'
    : n.marks.added + n.marks.changed + n.marks.removed > 0;

/** The lines to draw. The record's top level and each opened struct or array
 * draw their children a page at a time, then a `more` line; a child starts
 * open only if all of it fits in what is left of the page, so a small value
 * reads whole and a large one folds to one line until asked for. With
 * `changedOnly`, a child that holds no change is left out. */
export function recordLines(
  root: DiffNode,
  choices: Choices = {},
  changedOnly = false,
): RecordLine[] {
  const out: RecordLine[] = [];
  const pagesOf = (path: string) => PAGE_LINES * (1 + (choices.pages?.(path) ?? 0));

  const children = (n: DiffNode, depth: number): number => {
    const budget = pagesOf(n.path);
    const kids = changedOnly ? n.children.filter(changed) : n.children;
    let used = 0;
    let i = 0;
    for (; i < kids.length; i++) {
      // At least one, however long: a page is never empty.
      if (i > 0 && used >= budget) break;
      used += draw(kids[i], depth, budget - used);
    }
    if (i < kids.length) {
      out.push({ kind: 'more', path: n.path, depth, shown: i, total: kids.length });
      used++;
    }
    return used;
  };

  // Says how many lines the child takes from its parent's page. One the reader
  // opened takes one: what it draws is its own pages, so opening it does not
  // push its siblings off the parent's.
  const draw = (child: DiffNode, depth: number, left: number): number => {
    const { label, node: n } = chain(child);
    if (n.kind === 'leaf') {
      out.push({ ...n.leaf!, kind: 'field', depth, label, type: n.type });
      return 1;
    }
    const chosen = choices.open?.(n.path);
    const open = chosen ?? n.lines <= left;
    out.push({
      kind: 'group',
      path: n.path,
      depth,
      label,
      type: n.type,
      branch: n.kind,
      open,
      size: n.children.length,
      marks: n.marks,
    });
    if (!open) return 1;
    const drawn = children(n, depth + 1);
    return chosen ? 1 : 1 + drawn;
  };

  children(root, 0);
  return out;
}

/** Every field by its whole path, a page at a time; with `changedOnly`, only
 * the fields a step changed. */
export function pathLines(
  root: DiffNode,
  choices: Choices = {},
  changedOnly = false,
): RecordLine[] {
  const fields: DiffNode[] = [];
  const walk = (n: DiffNode) => {
    if (changedOnly && !changed(n)) return;
    if (n.kind === 'leaf') fields.push(n);
    else n.children.forEach(walk);
  };
  walk(root);
  const shown = Math.min(fields.length, PAGE_LINES * (1 + (choices.pages?.(root.path) ?? 0)));
  const out: RecordLine[] = fields
    .slice(0, shown)
    .map((n) => ({ ...n.leaf!, kind: 'field', depth: 0, label: n.path, type: n.type }));
  if (shown < fields.length)
    out.push({ kind: 'more', path: root.path, depth: 0, shown, total: fields.length });
  return out;
}

/** Both sides of a change on one line, or only the new value with the old one
 * a click away. A fixed budget rather than the true edge of the panel: a value
 * that only just fits would otherwise change shape as the divider moves. */
const INLINE_BUDGET_CHARS = 48;

export function changeFitsInline(
  before: unknown,
  after: unknown,
  budget: number = INLINE_BUDGET_CHARS,
): boolean {
  // The arrow and its spaces are the third term.
  return formatDiffValue(before).length + formatDiffValue(after).length + 3 <= budget;
}

/** A path split where a reader's eye should land: everything before the
 * last key carries the nesting and recedes, the last key is the field, as
 * the path spells it (`labels` and `["app.kubernetes.io/name"]`). */
export function splitPath(path: string): { prefix: string; leaf: string } {
  const spans = keySpans(path);
  const cut = spans.length === 0 ? 0 : spans[spans.length - 1].start;
  return { prefix: path.slice(0, cut), leaf: path.slice(cut) };
}

/** What a record's marks add up to, for the header of its block. */
export function countStates(fields: FieldLeaf[]): {
  added: number;
  changed: number;
  removed: number;
} {
  let added = 0;
  let changed = 0;
  let removed = 0;
  for (const f of fields) {
    if (f.state === 'added') added++;
    else if (f.state === 'changed') changed++;
    else if (f.state === 'removed') removed++;
  }
  return { added, changed, removed };
}
