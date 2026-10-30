// What a change did to each object it touched, for the review page.
//
// A change is read the way its author thinks of it: one card per source,
// pipeline or table that differs, not one hunk per file. Objects are matched
// by id within their section; a pipeline's steps by their own id, so a step
// that was edited reads as changed rather than as one removed and one added.

import {
  SHARED_SCOPE,
  recordLine,
  sampleSource,
  scopedKey,
  type ConfigSection,
  type FileConfigDto,
  type SampleDto,
  type TestDto,
  type TestResult,
  type TestSummary,
  type UndoTarget,
} from './api';
import { diffObjects, type DiffEntry } from './diff';
import { stepSummary, stepsFromDto } from './pipeline';
import { bundleRoutes } from './routeFlow';

export type Noun =
  | 'source'
  | 'destination'
  | 'route'
  | 'lookup table'
  | 'grok pattern'
  | 'schema'
  | 'pipeline'
  | 'worker group'
  | 'sample'
  | 'test'
  | 'lookup file'
  | 'file';

/** A group's own manifest, `system.json`, which holds no objects: the group
 * itself is what it describes. */
export const MANIFEST = 'manifest';

/** A bundle's samples and tests: each one its own file under these. */
export const SAMPLES = 'samples';
export const TESTS = 'tests';
/** A bundle's lookup data files, each one a table can name by its path. */
export const LOOKUPS = 'lookups';
/** A file of a bundle that does not read, at either end of a range. */
export const UNREADABLE = 'unreadable';

/** The manifest's fields a card compares. */
const MANIFEST_FIELDS = ['name', 'exceptions', 'version'] as const;

export interface SectionSpec {
  key: ConfigSection;
  noun: Noun;
  /** Fields handled elsewhere, or too structural to name as one field. */
  skip: string[];
  /** Its order is meaning, so an object moved within it is a change. */
  ordered?: true;
}

/** Every section a change can touch, in the order a change is told:
 * sources, destinations, routes, lookup tables, grok patterns, schemas, pipelines. */
export const SECTIONS: SectionSpec[] = [
  { key: 'sources', noun: 'source', skip: ['id'] },
  { key: 'destinations', noun: 'destination', skip: ['id'] },
  // Routes are tried in order.
  { key: 'routes', noun: 'route', skip: ['id'], ordered: true },
  { key: 'lookupTables', noun: 'lookup table', skip: ['id'] },
  { key: 'grokPatterns', noun: 'grok pattern', skip: ['id'] },
  { key: 'schemas', noun: 'schema', skip: ['id'] },
  { key: 'pipelines', noun: 'pipeline', skip: ['id', 'steps'] },
];

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function byId<T extends { id: string }>(items: T[]): Map<string, T> {
  return new Map(items.map((item) => [item.id, item]));
}

/** Which bundle a path in the repository belongs to. */
export function scopeOf(path: string): string {
  const group = /^groups\/([^/]+)\//.exec(path);
  return group ? group[1] : SHARED_SCOPE;
}

/** A bundle with nothing in it: the side of a comparison where the scope
 * did not exist yet, or no longer does. */
export function emptyConfig(): FileConfigDto {
  return {
    version: 1,
    sources: [],
    destinations: [],
    pipelines: [],
    routes: [],
    lookupTables: [],
    grokPatterns: [],
    schemas: [],
  };
}

/** JSON with every object's keys sorted, so two reads of one value compare
 * equal whatever order each serializer wrote its keys in. */
export function canonicalJson(value: unknown): string {
  const sort = (v: unknown): unknown =>
    Array.isArray(v)
      ? v.map(sort)
      : isRecord(v)
        ? Object.fromEntries(
            Object.keys(v)
              .sort()
              .map((k) => [k, sort(v[k])]),
          )
        : v;
  return JSON.stringify(sort(value));
}

export type Status = 'added' | 'removed' | 'changed';

/** One step of a changed pipeline, in the order the pipeline runs it, with
 * a removed step kept where it stood. */
export interface StepChange {
  status: Status | 'same';
  /** The step's own id, where it has one: what a comment on it names. */
  id?: string;
  type: string;
  /** One line: the step's key fields, or its expression. */
  summary: string;
  /** Its place in the pipeline after the change, from 1; absent for a removed step. */
  position?: number;
  /** It runs in another order against the steps around it. */
  moved?: true;
  /** An `if` step's branches, compared the same way. */
  then?: StepChange[];
  else?: StepChange[];
}

/** One row of an inline lookup table, and whether the change added or removed it. */
export interface RowChange {
  status: 'added' | 'removed' | 'same';
  cells: string[];
}

export interface ObjectChange {
  scope: string;
  section:
    | ConfigSection
    | typeof MANIFEST
    | typeof SAMPLES
    | typeof TESTS
    | typeof LOOKUPS
    | typeof UNREADABLE;
  noun: Noun;
  id: string;
  /** What a reader calls it: its name where it has one, its id otherwise. */
  label: string;
  status: Status;
  before?: unknown;
  after?: unknown;
  /** Every field that differs, the skipped ones aside. */
  fields: DiffEntry[];
  /** It stands in another order against the objects around it, in a
   * section whose order is meaning. */
  moved?: true;
  /** A Kafka or Event Hubs source whose `start` this changes: deploying it
   * moves the source there. */
  movesSource?: true;
  /** A pipeline's steps, every one of them. */
  steps?: StepChange[];
  /** An inline lookup table's columns and rows. */
  headers?: string[];
  rows?: RowChange[];
  /** A lookup file's delimiter at each end, from the table naming it there. */
  delimiters?: { before?: string; after?: string };
}

/** A change to one object of a section: what can be undone and commented on. */
export type SectionChange = ObjectChange & { section: ConfigSection };

export function isSectionChange(c: ObjectChange): c is SectionChange {
  return SECTIONS.some((spec) => spec.key === c.section);
}

/** What Undo changes puts back for a card: its object, or its file by its
 * path in the bundle. Null for a group's own card. */
export function undoTarget(c: ObjectChange): UndoTarget | null {
  if (isSectionChange(c)) return { section: c.section, id: c.id };
  if (c.section === SAMPLES || c.section === TESTS) return { path: `${c.section}/${c.id}.json` };
  if (c.section === LOOKUPS || c.section === UNREADABLE) return { path: c.id };
  return null;
}

/** One scope's two sides. */
export interface ScopePair {
  scope: string;
  before: FileConfigDto;
  after: FileConfigDto;
  /** How the range changed the scope's `system.json`, as git says it: `A` for
   * a group the change creates, `D` for one it deletes. Absent where the file
   * did not change. A read cannot tell an absent group from an empty one, so
   * this is what does. */
  manifest?: string;
  /** The scope's samples and tests the range changed, each as git and the
   * leader read it at both ends. */
  files?: FileSides[];
  /** The scope's lookup data files the range changed: each path, within the
   * bundle, to git's status for it. */
  lookups?: Map<string, string>;
  /** The scope's files that do not read at each end: each path, within the
   * bundle, to why. */
  unreadable?: { before: Map<string, string>; after: Map<string, string> };
  /** `shared/`'s bundle at each end, for a group's: a group runs shared routes before its own, so they count in a
   * route's name (`routeName`). */
  shared?: { before: FileConfigDto; after: FileConfigDto };
}

/** Each end's pipelines by id, to their names: a scope's own over `shared/`'s, which a group runs too. */
export interface SideNames {
  before: Map<string, string>;
  after: Map<string, string>;
}

export function sidePipelineNames(pair: Pick<ScopePair, 'before' | 'after' | 'shared'>): SideNames {
  const names = (config: FileConfigDto, shared?: FileConfigDto) =>
    new Map([...(shared?.pipelines ?? []), ...config.pipelines].map((p) => [p.id, p.name]));
  return {
    before: names(pair.before, pair.shared?.before),
    after: names(pair.after, pair.shared?.after),
  };
}

/** Pipeline `id` as a reader calls it at `side`: its name there, else at the other end, else the id itself. */
export function pipelineNameAt(names: SideNames, id: string, side: 'before' | 'after'): string {
  const other = side === 'before' ? 'after' : 'before';
  return names[side].get(id) || names[other].get(id) || id;
}

/** A route's `pipeline` field, naming its pipeline at each end. */
function pipelineByName(f: DiffEntry, names: SideNames): DiffEntry {
  if (f.key !== 'pipeline') return f;
  const at = (v: unknown, side: 'before' | 'after') =>
    typeof v === 'string' ? pipelineNameAt(names, v, side) : v;
  return { ...f, before: at(f.before, 'before'), after: at(f.after, 'after') };
}

/** Route `id` of a scope by its `routeName`: as the change leaves it, or as it was where the change removes it. */
export function changedRouteName(
  pair: Pick<ScopePair, 'before' | 'after' | 'shared'>,
  id: string,
): string | undefined {
  const named = (config: FileConfigDto, shared?: FileConfigDto) =>
    bundleRoutes(config, shared).find((r) => r.id === id)?.name;
  return named(pair.after, pair.shared?.after) ?? named(pair.before, pair.shared?.before);
}

/** The file each section is read from. A section whose file does not read
 * is read as empty, so every object in it would read as removed or added. A
 * pipeline and a schema each have a file of their own (`fileOfObject`). */
const SECTION_FILE: Record<ConfigSection, string> = {
  sources: 'sources.json',
  destinations: 'destinations.json',
  routes: 'routes.json',
  lookupTables: 'lookup-tables.json',
  grokPatterns: 'grok-patterns.json',
  schemas: '',
  pipelines: '',
};

/** The file `spec`'s object `id` is read from. */
function fileOfObject(spec: SectionSpec, id: string): string {
  if (spec.key === 'pipelines' || spec.key === 'schemas')
    return `${spec.key}/${pipelineSlug(id)}.json`;
  return SECTION_FILE[spec.key];
}

/** A pipeline's or schema's filename, as the leader names it (`config_file::slugify`). */
export function pipelineSlug(id: string): string {
  // Only ASCII letters and digits survive, lowered after: lowering first would turn `K` (Kelvin) into `k`.
  const slug = id
    .replace(/[^A-Za-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .toLowerCase();
  return slug === '' ? 'pipeline' : slug;
}

/** A file that stopped or started reading, or reads wrong for another
 * reason. Samples and tests have their own cards. */
function unreadableChanges(scope: string, pair: ScopePair): ObjectChange[] {
  const { before, after } = pair.unreadable ?? { before: new Map(), after: new Map() };
  const paths = [...new Set([...before.keys(), ...after.keys()])]
    .filter((path) => fileOf(path) === null && before.get(path) !== after.get(path))
    .sort();
  return paths.map((path) => {
    const [was, now] = [before.get(path), after.get(path)];
    const kind = was === undefined ? 'added' : now === undefined ? 'removed' : 'changed';
    const why: DiffEntry = { key: 'does not read', kind };
    if (was !== undefined) why.before = was;
    if (now !== undefined) why.after = now;
    return {
      scope,
      section: UNREADABLE,
      noun: 'file',
      id: path,
      label: path,
      status: 'changed',
      fields: [why],
    };
  });
}

/** One sample or test file the range changed. `status` is git's, as for
 * `ScopePair.manifest`; a side is absent where the file is, or where it does
 * not read. */
export type FileSides =
  | { kind: typeof SAMPLES; id: string; status: string; before?: SampleDto; after?: SampleDto }
  | { kind: typeof TESTS; id: string; status: string; before?: TestDto; after?: TestDto };

/** The sample or test file at `path` within a scope's bundle, or null for any
 * other file. */
export function fileOf(path: string): { kind: typeof SAMPLES | typeof TESTS; id: string } | null {
  const m = /^(samples|tests)\/([^/]+)\.json$/.exec(path);
  if (!m) return null;
  return { kind: m[1] === SAMPLES ? SAMPLES : TESTS, id: m[2] };
}

function labelOf(item: unknown, id: string): string {
  return isRecord(item) && typeof item.name === 'string' && item.name !== '' ? item.name : id;
}

function oneLine(text: string): string {
  return text.replace(/\s*\n\s*/g, ' ').trim();
}

/** A step said in one line: what it reads and writes, or its expression. */
export function stepLine(raw: Record<string, unknown>): string {
  const str = (key: string) => (typeof raw[key] === 'string' ? (raw[key] as string) : '');
  switch (raw.type) {
    case 'code':
      return oneLine(str('body'));
    case 'lookup': {
      const outputs = Array.isArray(raw.outputs)
        ? raw.outputs.map((o) => (isRecord(o) && typeof o.field === 'string' ? o.field : '…'))
        : [];
      return `${str('table') || '…'}  ${str('input') || '…'} → ${outputs.join(', ') || '…'}`;
    }
    case 'drop':
      return oneLine(str('condition'));
    case 'if':
      return `if ${oneLine(str('condition'))}`;
    case 'comment':
      return oneLine(str('text'));
  }
  try {
    return stepSummary(stepsFromDto([raw])[0]);
  } catch {
    // A step this GUI cannot model still has a type to name.
    return '';
  }
}

function stepType(raw: Record<string, unknown>): string {
  return typeof raw.type === 'string' ? raw.type : 'step';
}

function stepId(raw: unknown): string | null {
  return isRecord(raw) && typeof raw.id === 'string' ? raw.id : null;
}

function asList(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function stepEntry(
  raw: Record<string, unknown>,
  status: StepChange['status'],
  position: number | undefined,
  then: StepChange[] | undefined,
  els: StepChange[] | undefined,
): StepChange {
  const entry: StepChange = { status, type: stepType(raw), summary: stepLine(raw) };
  const id = stepId(raw);
  if (id !== null) entry.id = id;
  if (position !== undefined) entry.position = position;
  if (then) entry.then = then;
  if (els) entry.else = els;
  return entry;
}

/** A step with nothing to compare against: every branch step takes its status. */
function whole(raw: Record<string, unknown>, status: Status, position?: number): StepChange {
  const branch = (key: string) =>
    raw.type === 'if'
      ? asList(raw[key])
          .filter(isRecord)
          .map((s) => whole(s, status))
      : undefined;
  return stepEntry(raw, status, position, branch('then'), branch('else'));
}

/** One list of steps against another, in the order `after` runs them, each
 * removed step placed after the step it followed. Matched by id within the
 * list, so a step moved into a branch reads as removed here and added there. */
export function compareSteps(before: unknown[], after: unknown[]): StepChange[] {
  const was = before.filter(isRecord);
  const now = after.filter(isRecord);
  const nowIds = new Set(now.map(stepId).filter((id) => id !== null));
  const wasIndex = new Map<string, number>();
  was.forEach((s, i) => {
    const id = stepId(s);
    if (id !== null) wasIndex.set(id, i);
  });

  const out: StepChange[] = [];
  let next = 0;
  const removedUpTo = (end: number) => {
    for (; next < end; next++) {
      const id = stepId(was[next]);
      if (id === null || !nowIds.has(id)) out.push(whole(was[next], 'removed'));
    }
  };

  now.forEach((step, i) => {
    const id = stepId(step);
    const at = id === null ? undefined : wasIndex.get(id);
    if (at === undefined) {
      out.push(whole(step, 'added', i + 1));
      return;
    }
    // A step behind one already passed is out of the order it had.
    const moved = at < next;
    if (!moved) {
      removedUpTo(at);
      next = at + 1;
    }
    const old = was[at];
    const own = (s: Record<string, unknown>) => canonicalJson({ ...s, then: null, else: null });
    const status = own(old) === own(step) && !moved ? 'same' : 'changed';
    const branches = step.type === 'if' || old.type === 'if';
    const entry = stepEntry(
      step,
      status,
      i + 1,
      branches ? compareSteps(asList(old.then), asList(step.then)) : undefined,
      branches ? compareSteps(asList(old.else), asList(step.else)) : undefined,
    );
    if (moved) entry.moved = true;
    out.push(entry);
  });
  removedUpTo(was.length);
  return out;
}

function cellsOf(row: unknown): string[] {
  return asList(row).map((cell) => String(cell));
}

/** An inline table's rows, after's in order, then each row only before had.
 * Rows are compared whole, as many times as each occurs. */
export function compareRows(before: unknown[], after: unknown[]): RowChange[] {
  const left = new Map<string, number>();
  for (const row of before) {
    const key = JSON.stringify(row);
    left.set(key, (left.get(key) ?? 0) + 1);
  }
  const out: RowChange[] = [];
  for (const row of after) {
    const key = JSON.stringify(row);
    const n = left.get(key) ?? 0;
    if (n > 0) left.set(key, n - 1);
    out.push({ status: n > 0 ? 'same' : 'added', cells: cellsOf(row) });
  }
  for (const row of before) {
    const key = JSON.stringify(row);
    const n = left.get(key) ?? 0;
    if (n === 0) continue;
    left.set(key, n - 1);
    out.push({ status: 'removed', cells: cellsOf(row) });
  }
  return out;
}

function isInline(item: unknown): item is { headers: unknown; rows: unknown } {
  return isRecord(item) && item.type === 'inline';
}

function objectChange(
  scope: string,
  spec: SectionSpec,
  id: string,
  before: unknown,
  after: unknown,
  moved: boolean,
  label = labelOf(after ?? before, id),
): ObjectChange | null {
  const status: Status =
    before === undefined ? 'added' : after === undefined ? 'removed' : 'changed';
  const inline = isInline(before) || isInline(after);
  const skip = inline ? [...spec.skip, 'rows'] : spec.skip;
  const fields = diffObjects(before, after).filter(
    (entry) => entry.kind !== 'unchanged' && !skip.includes(entry.key),
  );

  const change: ObjectChange = {
    scope,
    section: spec.key,
    noun: spec.noun,
    id,
    label,
    status,
    fields,
  };
  if (before !== undefined) change.before = before;
  if (after !== undefined) change.after = after;
  if (moved && status === 'changed') change.moved = true;

  if (spec.key === 'sources' && startChanges(before, after)) change.movesSource = true;

  let differs = moved || fields.length > 0;
  if (spec.key === 'pipelines') {
    const steps = compareSteps(
      asList(isRecord(before) ? before.steps : undefined),
      asList(isRecord(after) ? after.steps : undefined),
    );
    change.steps = steps;
    differs ||= hasChange(steps);
  }
  if (inline) {
    const side = (isInline(after) ? after : before) as { headers: unknown };
    change.headers = asList(side.headers).map(String);
    change.rows = compareRows(
      isInline(before) ? asList(before.rows) : [],
      isInline(after) ? asList(after.rows) : [],
    );
    differs ||= change.rows.some((r) => r.status !== 'same');
  }
  return status === 'changed' && !differs ? null : change;
}

function hasChange(steps: StepChange[]): boolean {
  return steps.some(
    (s) => s.status !== 'same' || hasChange(s.then ?? []) || hasChange(s.else ?? []),
  );
}

/** The ids in `now` that stand behind one they followed in `was`. */
function outOfOrder(was: { id: string }[], now: { id: string }[]): Set<string> {
  const at = new Map(was.map((o, i) => [o.id, i]));
  const out = new Set<string>();
  let furthest = -1;
  for (const { id } of now) {
    const i = at.get(id);
    if (i === undefined) continue;
    if (i < furthest) out.add(id);
    else furthest = i;
  }
  return out;
}

/** What the range did to a group itself: created it, deleted it, or changed
 * what its manifest says. Null for `shared`, which has no manifest, and where
 * the manifest did not change. A changed manifest with no field changed still
 * gets a card: the config read reports every version as current, so a repaired
 * version shows only as a changed file. */
export function groupChange(pair: ScopePair): ObjectChange | null {
  const { scope, manifest } = pair;
  if (scope === SHARED_SCOPE || manifest === undefined) return null;
  const status: Status = manifest === 'A' ? 'added' : manifest === 'D' ? 'removed' : 'changed';
  const pick = (config: FileConfigDto): Record<string, unknown> =>
    Object.fromEntries(
      MANIFEST_FIELDS.filter((k) => config[k] !== undefined).map((k) => [k, config[k]]),
    );
  const before = status === 'added' ? undefined : pick(pair.before);
  const after = status === 'removed' ? undefined : pick(pair.after);
  const fields = diffObjects(before, after).filter((entry) => entry.kind !== 'unchanged');
  const change: ObjectChange = {
    scope,
    section: MANIFEST,
    noun: 'worker group',
    id: scope,
    label: labelOf(after ?? before, scope),
    status,
    fields,
  };
  if (before !== undefined) change.before = before;
  if (after !== undefined) change.after = after;
  return change;
}

/** Both sides are the same Kafka or Event Hubs source, at another `start`.
 * An absent start is `earliest`, so naming it moves nothing. */
function startChanges(before: unknown, after: unknown): boolean {
  const start = (s: unknown) =>
    isRecord(s) && (s.type === 'kafka' || s.type === 'eventHubs')
      ? JSON.stringify(s.start ?? { type: 'earliest' })
      : undefined;
  const from = start(before);
  return from !== undefined && start(after) !== undefined && from !== start(after);
}

/** `fields` without the ones it leaves unset, so an unset one is not shown as added or removed. */
function present(fields: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== undefined));
}

function recordCells(record: SampleDto['records'][number], sourced: boolean): string[] {
  return sourced
    ? [recordLine(record), sampleSource(record) ? JSON.stringify(sampleSource(record)) : '']
    : [recordLine(record)];
}

function expectedCells(doc: unknown): string[] {
  return [doc === null ? 'dropped' : JSON.stringify(doc)];
}

/** A changed sample or test file as a card: its fields, and its records or
 * expected output as rows. Git, not the two reads, says how the file
 * changed, so one that stopped or started reading still has its card. */
export function fileChange(scope: string, file: FileSides): ObjectChange {
  const status: Status =
    file.status === 'A' ? 'added' : file.status === 'D' ? 'removed' : 'changed';
  const change: ObjectChange = {
    scope,
    section: file.kind,
    noun: file.kind === SAMPLES ? 'sample' : 'test',
    id: file.id,
    label: labelOf(file.after ?? file.before, file.id),
    status,
    fields: [],
  };
  if (file.kind === SAMPLES) {
    const fieldsOf = (s: SampleDto | undefined) => s && present({ name: s.name, source: s.source });
    change.fields = diffObjects(fieldsOf(file.before), fieldsOf(file.after));
    const was = file.before?.records ?? [];
    const now = file.after?.records ?? [];
    const sourced = [...was, ...now].some((r) => sampleSource(r) !== undefined);
    change.headers = sourced ? ['record', 'source fields'] : ['record'];
    change.rows = compareRows(
      was.map((r) => recordCells(r, sourced)),
      now.map((r) => recordCells(r, sourced)),
    );
  } else {
    const fieldsOf = (t: TestDto | undefined) =>
      t &&
      present({
        name: t.name,
        pipeline: t.pipeline,
        sample: t.sample,
        rowsIn: t.rowsIn,
        lockedAt: t.lockedAt,
      });
    change.fields = diffObjects(fieldsOf(file.before), fieldsOf(file.after));
    change.headers = ['expected output'];
    change.rows = compareRows(
      (file.before?.expected ?? []).map(expectedCells),
      (file.after?.expected ?? []).map(expectedCells),
    );
  }
  change.fields = change.fields.filter((f) => f.kind !== 'unchanged');
  if (file.before !== undefined) change.before = file.before;
  if (file.after !== undefined) change.after = file.after;
  return change;
}

/** The paths a lookup table names, on either side of its card. */
function namedPaths(c: ObjectChange): string[] {
  return [c.before, c.after].flatMap((t) =>
    isRecord(t) && typeof t.path === 'string' ? [t.path] : [],
  );
}

/** A lookup file's name as its uploader gave it: an upload is stored under
 * its content's hash, then that name. */
export function uploadedName(path: string): string {
  const base = path.slice(path.lastIndexOf('/') + 1);
  return base.replace(/^[0-9a-f]{16}-/, '');
}

/** A changed lookup file whose table's card does not already name it: an
 * upload no table points at yet, a file a table stopped naming without one
 * naming it in its place, or bytes changed under a path a table keeps. */
/** The delimiter of the CSV table in `config` that names `path`, if one does. */
function delimiterOf(config: FileConfigDto, path: string): string | undefined {
  const table = (config.lookupTables ?? []).find((t) => t.type === 'csv' && t.path === path);
  return table?.type === 'csv' ? table.delimiter : undefined;
}

function lookupFileChange(scope: string, path: string, gitStatus: string): ObjectChange {
  const status: Status = gitStatus === 'A' ? 'added' : gitStatus === 'D' ? 'removed' : 'changed';
  // Two uploads of one name differ only in their path.
  const where: DiffEntry =
    status === 'removed'
      ? { key: 'path', kind: 'removed', before: path }
      : { key: 'path', kind: 'added', after: path };
  return {
    scope,
    section: LOOKUPS,
    noun: 'lookup file',
    id: path,
    label: uploadedName(path),
    status,
    fields: [where],
  };
}

/** The rows two versions of a CSV lookup file differ in, under the newer
 * one's header row. */
export function csvRowChanges(
  before: string[][],
  after: string[][],
): { headers: string[]; rows: RowChange[] } {
  const headers = (after[0] ?? before[0] ?? []).map(String);
  return { headers, rows: compareRows(before.slice(1), after.slice(1)) };
}

/** Every object that differs between each scope's two sides: scopes in the
 * order given, each opening with the group itself where it changed, then
 * sections in `SECTIONS` order, objects by id, then samples and tests, then
 * lookup files no table's card names. */
export function objectChanges(pairs: ScopePair[]): ObjectChange[] {
  const out: ObjectChange[] = [];
  for (const pair of pairs) {
    const { scope, before, after } = pair;
    const unread = new Set([
      ...(pair.unreadable?.before.keys() ?? []),
      ...(pair.unreadable?.after.keys() ?? []),
    ]);
    // A manifest that does not read is read with its defaults, so its file's card says what changed.
    const group = unread.has('system.json') ? null : groupChange(pair);
    if (group) out.push(group);
    out.push(...unreadableChanges(scope, pair));
    const names = sidePipelineNames(pair);
    for (const spec of SECTIONS) {
      const wasList = (before[spec.key] ?? []) as { id: string }[];
      const nowList = (after[spec.key] ?? []) as { id: string }[];
      const was = byId(wasList);
      const now = byId(nowList);
      const moved = spec.ordered ? outOfOrder(wasList, nowList) : new Set<string>();
      const ids = [...new Set([...was.keys(), ...now.keys()])].sort();
      for (const id of ids) {
        // Its file says what it is, not that it was removed or added.
        if (unread.has(fileOfObject(spec, id))) continue;
        const route = spec.key === 'routes' ? changedRouteName(pair, id) : undefined;
        const change = objectChange(
          scope,
          spec,
          id,
          was.get(id),
          now.get(id),
          moved.has(id),
          route,
        );
        if (change && spec.key === 'routes')
          change.fields = change.fields.map((f) => pipelineByName(f, names));
        if (change) out.push(change);
      }
    }
    const scoped = out.filter((c) => c.scope === scope && c.section === 'lookupTables');
    const named = new Set(scoped.flatMap(namedPaths));
    const lookupCards = [...(pair.lookups ?? [])]
      // A table's card compares a file it replaced, never bytes changed under a path it keeps.
      .filter(([path, status]) => !named.has(path) || status === 'M')
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([path, status]) => ({
        ...lookupFileChange(scope, path, status),
        delimiters: { before: delimiterOf(before, path), after: delimiterOf(after, path) },
      }));
    for (const kind of [SAMPLES, TESTS]) {
      const files = (pair.files ?? []).filter((f) => f.kind === kind);
      files.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
      for (const file of files) out.push(fileChange(scope, file));
    }
    out.push(...lookupCards);
  }
  return out;
}

/** The tests that cover pipeline `id` in `scope`: by the test's summary,
 * which names every pipeline an end-to-end test runs through, or by the
 * result's own pipeline where the summary is missing. */
export function testsCovering(
  results: TestResult[],
  summaries: Map<string, TestSummary>,
  scope: string,
  id: string,
): TestResult[] {
  return results.filter((t) => {
    if (t.area !== scope) return false;
    const summary = summaries.get(scopedKey(t.area, t.id));
    const pipelines = summary ? summary.pipelines : t.pipeline ? [t.pipeline] : [];
    return pipelines.includes(id);
  });
}

/** One line of a line diff. */
export interface DiffLine {
  kind: 'same' | 'added' | 'removed';
  text: string;
}

/** Above this many cells the longest-common-subsequence table is not built:
 * the two sides are shown whole, one removed and one added. */
const LINE_DIFF_LIMIT = 4_000_000;

/** `before` and `after` as indented JSON, compared line by line. An absent
 * side has no lines. */
export function jsonDiff(before: unknown, after: unknown): DiffLine[] {
  const lines = (v: unknown) => (v === undefined ? [] : JSON.stringify(v, null, 2).split('\n'));
  const a = lines(before);
  const b = lines(after);
  if (a.length * b.length > LINE_DIFF_LIMIT) {
    return [
      ...a.map((text) => ({ kind: 'removed' as const, text })),
      ...b.map((text) => ({ kind: 'added' as const, text })),
    ];
  }
  // lcs[i][j]: the longest common run of a[i..] and b[j..].
  const lcs: number[][] = Array.from({ length: a.length + 1 }, () =>
    new Array<number>(b.length + 1).fill(0),
  );
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }
  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      out.push({ kind: 'same', text: a[i] });
      i++;
      j++;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
      out.push({ kind: 'removed', text: a[i++] });
    } else {
      out.push({ kind: 'added', text: b[j++] });
    }
  }
  while (i < a.length) out.push({ kind: 'removed', text: a[i++] });
  while (j < b.length) out.push({ kind: 'added', text: b[j++] });
  return out;
}
