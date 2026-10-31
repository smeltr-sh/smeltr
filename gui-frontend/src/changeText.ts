// The words a change is opened with.
//
// A change's title becomes `main`'s commit subject and its description the
// body (`git.rs`, `deploy_change`), so both are read off what actually
// differs rather than asked for. The comparison is structural: every word
// names a field, an id, a pipeline's name or a value that is really there,
// and nothing is inferred. A pipeline is said by its name, wherever it
// appears: its id is a key no reader chose. One object changed one way is a
// sentence; a change touching many is a count over a body that lists them,
// which is the shape a commit message already has.

import type { FileConfigDto } from './api';
import { diffObjects } from './diff';
import {
  SECTIONS,
  byId,
  changedRouteName,
  isRecord,
  pipelineNameAt,
  sidePipelineNames,
  type Noun,
  type ScopePair,
  type SideNames,
} from './objectChanges';
import { count } from './time';

export interface ChangeText {
  /** The commit subject: one line, imperative. */
  subject: string;
  /** The commit body: one line per object changed, or empty when the
   * subject already says the whole of it. */
  body: string;
}

/** One scope's two sides. A branch can touch more than one group, and each
 * group is its own bundle. */
export interface ScopeChange {
  scope: string;
  before: FileConfigDto;
  after: FileConfigDto;
  shared?: ScopePair['shared'];
}

/** `name` is the object as a reader calls it: its id, a pipeline's name, or a route's `routeName`. */
type Edit =
  | { op: 'add'; noun: Noun; name: string; scope: string }
  | { op: 'remove'; noun: Noun; name: string; scope: string }
  | { op: 'update'; noun: Noun; name: string; scope: string; detail: string };

/** How long a described value may run before its middle is dropped. Both
 * ends survive: cutting the tail hides the very thing a reader looks for --
 * which destination a policy now copies to, which broker was added. */
const VALUE_MAX = 48;

function elide(text: string): string {
  if (text.length <= VALUE_MAX) return text;
  const keep = Math.floor((VALUE_MAX - 1) / 2);
  return `${text.slice(0, keep)}…${text.slice(text.length - keep)}`;
}

/** A line break in a value -- a PEM certificate, a connector's authorities -- read as one line's worth: the
 * subject this feeds is one line by `parse_marker`'s own rule, and everything after the first would silently
 * become the body instead of the field it was describing. */
function oneLine(text: string): string {
  return text.replace(/\s*\n\s*/g, ' ');
}

/** A tagged union member said as what it does. Every variant the config
 * holds is named here; one that is not falls back to its own tag, which is
 * coarse but never wrong -- and never raw JSON, which reads as a bug and
 * truncates away the fact that mattered. */
function describeTagged(value: Record<string, unknown>): string {
  const type = String(value.type ?? '');
  switch (type) {
    // LookupTableDto and ParserKind share `csv`; both mean the same word.
    case 'csv':
      return typeof value.path === 'string' ? `csv ${value.path}` : 'csv';
    case 'inline':
      return `inline, ${count(Array.isArray(value.rows) ? value.rows.length : 0, 'row')}`;
    case 'mmdb':
      return `mmdb ${String(value.path ?? '')}`.trim();
    case 'json':
      return 'json';
    case 'keyValue':
      return 'key/value';
    case 'grok':
      return 'regex/grok';
    // SourceDto and DestinationDto
    case 'kafka':
      return 'kafka';
    case 'metrics':
      return 'smeltr metrics';
    case 'logs':
      return 'smeltr logs';
    case 'elasticsearch':
      return 'elasticsearch';
    case 'splunkHec':
      return 'splunk hec';
    // A Kafka source's start or stop: RFC 3339 in UTC, the same moment to every reviewer.
    case 'timestamp':
      return typeof value.millis === 'number' ? rfc3339(value.millis) : 'timestamp';
    default:
      return type === '' ? 'a value' : type;
  }
}

/** `millis` as RFC 3339 in UTC, its milliseconds only when it has some. */
function rfc3339(millis: number): string {
  const date = new Date(millis);
  if (Number.isNaN(date.getTime())) return String(millis);
  return date.toISOString().replace('.000Z', 'Z');
}

/** Does every item carry these keys, with string values where asked? */
function allHave(items: unknown[], keys: string[]): boolean {
  return items.every(
    (item) => isRecord(item) && keys.every((key) => typeof item[key] === 'string'),
  );
}

/** A list said as its contents. The shapes named here are the ones whose
 * contents are the change -- an assignment's expression, a rename's pair --
 * and saying "1 entry" of them names nothing at all. */
function describeArray(items: unknown[]): string {
  if (items.length === 0) return 'none';
  if (items.every((item) => typeof item === 'string')) return elide(items.join(', '));
  // Delivery rules: where rows go is the fact, not how many rules say so.
  if (items.every((item) => isRecord(item) && Array.isArray(item.to))) {
    const to = [...new Set(items.flatMap((item) => (item as { to: string[] }).to))];
    return elide(`to ${to.join(', ')}`);
  }
  const rows = items as Record<string, string>[];
  if (allHave(items, ['target', 'expr'])) {
    return elide(rows.map((row) => `${row.target}=${row.expr}`).join(', '));
  }
  if (allHave(items, ['from', 'to'])) {
    return elide(rows.map((row) => `${row.from}→${row.to}`).join(', '));
  }
  if (allHave(items, ['name'])) return elide(rows.map((row) => row.name).join(', '));
  return count(items.length, 'entry', 'entries');
}

/** A field's value as a reader would say it. */
export function describe(value: unknown): string {
  // A field that is absent and one explicitly set to nothing are different
  // facts. An omitted number is usually its default -- `stallAfterSecs`
  // absent is 30 seconds, not no timeout -- so it must not read as zero.
  if (value === undefined) return 'unset';
  if (value === null) return 'none';
  if (typeof value === 'string') return value === '' ? 'empty' : elide(oneLine(value));
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (Array.isArray(value)) return describeArray(value);
  if (isRecord(value)) {
    if (typeof value.type === 'string') return elide(describeTagged(value));
    const keys = Object.keys(value);
    // A choice that names its targets, `{ "dlq": ["kafka-dlq"] }`: the choice and the targets are the whole change.
    const named = keys.length === 1 ? value[keys[0]] : undefined;
    if (typeof named === 'string') return elide(`${keys[0]} ${named}`);
    if (Array.isArray(named) && named.length > 0 && named.every((x) => typeof x === 'string'))
      return elide(`${keys[0]} ${named.join(', ')}`);
    return keys.length === 0 ? 'none' : count(keys.length, 'entry', 'entries');
  }
  return 'a value';
}

/** Every changed field of one object, as `name from before to after`. Keys are
 * compared whole (`diff.ts`), so a nested value reads as one field rather
 * than a diff inside a diff. With `names`, a `pipeline` field -- a route's,
 * a call step's -- holds a pipeline id and is said as that pipeline's name. */
function fieldPhrases(
  before: unknown,
  after: unknown,
  skip: string[],
  names?: SideNames,
): string[] {
  const say = (key: string, value: unknown, side: 'before' | 'after') => {
    if (!names || key !== 'pipeline' || typeof value !== 'string') return describe(value);
    return describe(pipelineNameAt(names, value, side));
  };
  return diffObjects(before, after)
    .filter((entry) => entry.kind !== 'unchanged' && !skip.includes(entry.key))
    .map(
      (entry) =>
        `${entry.key} from ${say(entry.key, entry.before, 'before')} to ${say(entry.key, entry.after, 'after')}`,
    );
}

// -- pipeline steps --------------------------------------------------------

/** Every step of a pipeline by its id, which survives an edit. Nested
 * branches are flattened, so a step buried in an `if` is named like any
 * other. The steps are raw JSON: one with no string id has no entry, and
 * its branches are walked all the same. */
function stepsById(steps: unknown[]): Map<string, Record<string, unknown>> {
  const found = new Map<string, Record<string, unknown>>();
  const walk = (list: unknown[]) => {
    for (const raw of list) {
      if (!isRecord(raw)) continue;
      if (typeof raw.id === 'string') found.set(raw.id, raw);
      if (Array.isArray(raw.then)) walk(raw.then);
      if (Array.isArray(raw.else)) walk(raw.else);
    }
  };
  walk(steps);
  return found;
}

function stepType(step: Record<string, unknown>): string {
  return typeof step.type === 'string' ? step.type : 'step';
}

function stepPhrases(before: unknown[], after: unknown[], names: SideNames): string[] {
  const was = stepsById(before);
  const now = stepsById(after);
  const phrases: string[] = [];

  for (const [id, step] of now) {
    if (!was.has(id)) phrases.push(`add ${stepType(step)} step`);
  }
  for (const [id, step] of was) {
    if (!now.has(id)) phrases.push(`remove ${stepType(step)} step`);
  }
  for (const [id, step] of was) {
    const next = now.get(id);
    if (!next) continue;
    // A branch's own steps report themselves, so an `if` speaks only for
    // its condition.
    const fields = fieldPhrases(step, next, ['id', 'type', 'then', 'else'], names);
    if (fields.length > 0) phrases.push(`${stepType(step)} step ${fields.join(', ')}`);
  }
  return phrases;
}

// -- one scope -------------------------------------------------------------

interface Section {
  noun: Noun;
  before: { id: string }[];
  after: { id: string }[];
  /** Fields handled elsewhere, or too structural to name. */
  skip: string[];
}

function sections(before: FileConfigDto, after: FileConfigDto): Section[] {
  return SECTIONS.map((spec) => ({
    noun: spec.noun,
    before: before[spec.key] ?? [],
    after: after[spec.key] ?? [],
    skip: spec.skip,
  }));
}

function editsForScope(change: ScopeChange): Edit[] {
  const { scope } = change;
  const edits: Edit[] = [];
  const names = sidePipelineNames(change);
  // A pipeline is called by the name it had, which is the one a reader
  // knows it by; a pipeline the change adds has only its new one. A route is
  // called as the branch has it, as its card on the change page is, falling
  // back to main's for a route the change removes.
  const nameFor = (noun: Noun, id: string) =>
    noun === 'pipeline'
      ? pipelineNameAt(names, id, 'before')
      : noun === 'route'
        ? (changedRouteName(change, id) ?? id)
        : id;

  for (const section of sections(change.before, change.after)) {
    const was = byId(section.before);
    const now = byId(section.after);
    const ids = [...new Set([...was.keys(), ...now.keys()])].sort();

    for (const id of ids) {
      const before = was.get(id);
      const after = now.get(id);
      if (!before && after) {
        edits.push({ op: 'add', noun: section.noun, name: nameFor(section.noun, id), scope });
        continue;
      }
      if (before && !after) {
        edits.push({ op: 'remove', noun: section.noun, name: nameFor(section.noun, id), scope });
        continue;
      }
      if (!before || !after) continue;

      const phrases = fieldPhrases(
        before,
        after,
        section.skip,
        section.noun === 'route' ? names : undefined,
      );
      if (section.noun === 'pipeline') {
        const bSteps = (before as { steps?: unknown[] }).steps ?? [];
        const aSteps = (after as { steps?: unknown[] }).steps ?? [];
        phrases.push(...stepPhrases(bSteps, aSteps, names));
      }
      if (phrases.length > 0) {
        edits.push({
          op: 'update',
          noun: section.noun,
          name: nameFor(section.noun, id),
          scope,
          detail: phrases.join(', '),
        });
      }
    }
  }
  return edits;
}

// -- rendering -------------------------------------------------------------

function plural(noun: Noun): string {
  return `${noun}s`;
}

/** `a`, `a and b`, `a, b and c`. */
function series(parts: string[]): string {
  if (parts.length <= 1) return parts.join('');
  return `${parts.slice(0, -1).join(', ')} and ${parts[parts.length - 1]}`;
}

function line(edit: Edit, sayScope: boolean): string {
  const where = sayScope ? ` in ${edit.scope}` : '';
  switch (edit.op) {
    case 'add':
      return `Add ${edit.noun} ${edit.name}${where}`;
    case 'remove':
      return `Remove ${edit.noun} ${edit.name}${where}`;
    case 'update':
      return `Update ${edit.noun} ${edit.name}${where}: ${edit.detail}`;
  }
}

const VERB: Record<Edit['op'], string> = { add: 'Add', remove: 'Remove', update: 'Update' };

function subjectFor(edits: Edit[], sayScope: boolean): string {
  if (edits.length === 1) return line(edits[0], sayScope);

  const nouns = [...new Set(edits.map((edit) => edit.noun))];
  const ops = new Set(edits.map((edit) => edit.op));
  const groups = new Set(edits.map((edit) => edit.scope));

  // One verb only where every edit shares it: "Add 3 sources" is a promise,
  // and an update hiding among adds would break it.
  const verb = ops.size === 1 ? VERB[edits[0].op] : 'Change';
  if (nouns.length === 1 && groups.size === 1) {
    const noun = edits.length === 1 ? nouns[0] : plural(nouns[0]);
    return `${verb} ${edits.length} ${noun}`;
  }
  const across = groups.size > 1 ? `${count(groups.size, 'group')}` : series(nouns.map(plural));
  return `${verb} ${count(edits.length, 'object')} across ${across}`;
}

/** What to call a change, from the two configurations it sits between.
 *
 * Deterministic: the same pair always yields the same words, in the same
 * order -- sources, destinations, routes, lookup tables, pipelines,
 * each in its id's order -- so the text can be pinned by a test. */
export function changeText(changes: ScopeChange[]): ChangeText {
  const edits = changes.flatMap(editsForScope);
  if (edits.length === 0) {
    // Nothing this function models differs. A branch is refused a change
    // when it is level with `main` (`branch_api.rs`), so something did
    // move: say so plainly rather than naming the wrong thing.
    return { subject: 'Update the configuration', body: '' };
  }

  const sayScope = new Set(edits.map((edit) => edit.scope)).size > 1;
  const subject = subjectFor(edits, sayScope);
  const body = edits.length === 1 ? '' : edits.map((edit) => line(edit, sayScope)).join('\n');
  return { subject, body };
}
