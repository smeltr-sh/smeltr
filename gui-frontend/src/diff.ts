// A shallow, top-level-key diff between two JSON row states. Deliberately
// not recursive: a nested value is compared whole, by its JSON text, which
// shows "this field changed" without rendering a diff inside a diff.

export type DiffKind = 'unchanged' | 'added' | 'removed' | 'changed';

export interface DiffEntry {
  key: string;
  kind: DiffKind;
  before?: unknown;
  after?: unknown;
}

function sameValue(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/** Every key either side has: `before`'s keys in its order, then keys only
 * `after` has in its order. A non-object on either side counts as empty. */
export function diffObjects(before: unknown, after: unknown): DiffEntry[] {
  const b = (
    before && typeof before === 'object' && !Array.isArray(before) ? before : {}
  ) as Record<string, unknown>;
  const a = (after && typeof after === 'object' && !Array.isArray(after) ? after : {}) as Record<
    string,
    unknown
  >;

  const entries: DiffEntry[] = [];
  const seen = new Set<string>();

  for (const key of Object.keys(b)) {
    seen.add(key);
    if (!(key in a)) {
      entries.push({ key, kind: 'removed', before: b[key] });
    } else if (!sameValue(b[key], a[key])) {
      entries.push({ key, kind: 'changed', before: b[key], after: a[key] });
    } else {
      entries.push({ key, kind: 'unchanged', before: b[key], after: a[key] });
    }
  }
  for (const key of Object.keys(a)) {
    if (!seen.has(key)) {
      entries.push({ key, kind: 'added', after: a[key] });
    }
  }
  return entries;
}

// A string that reads exactly like a bare number, `true`, `false` or `null`
// would be a JSON scalar were the string type not shown -- the one case a
// bare string is ambiguous rather than merely quote-free.
const READS_AS_JSON_SCALAR = /^(-?\d+(\.\d+)?([eE][+-]?\d+)?|true|false|null)$/;

/** One line for the diff view: a string bare, unless what it says could
 * also be read as a number, `true`, `false` or `null` -- then quoted, the
 * one case the field's actual type (str vs a numeric type) is not
 * otherwise visible. Everything else renders as JSON. */
export function formatDiffValue(v: unknown): string {
  if (v === undefined) return '';
  if (typeof v === 'string') return READS_AS_JSON_SCALAR.test(v) ? JSON.stringify(v) : v;
  return JSON.stringify(v);
}

/** A config page's rows, keyed the way a reader names them. An array of rows
 * carrying an `id` keys by that id, so a row that moved in the list is not
 * read as one removed and another added. Anything else is handed back whole
 * for `diffObjects` to make what it can of. */
function keyedRows(json: string): unknown | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch {
    // Nothing truthful to say about text that is not a value at all. Both
    // sides come from `JSON.stringify` in every caller, so this is a floor,
    // not a case a reader reaches.
    return undefined;
  }
  if (!Array.isArray(parsed)) return parsed;
  const rows: Record<string, unknown> = {};
  parsed.forEach((row, i) => {
    const id =
      row && typeof row === 'object' && 'id' in row
        ? String((row as { id: unknown }).id)
        : String(i);
    rows[id] = row;
  });
  return rows;
}

/** What `after` would change about `before`, for two of the serialized values a
 * config page keeps. Unchanged rows are left out: this answers "what differs",
 * which is the only part of it a reader is being asked about. A value that is
 * not a list of rows produces nothing, and the caller says the rest in words. */
export function changesBetween(beforeJson: string, afterJson: string): DiffEntry[] {
  const [before, after] = [keyedRows(beforeJson), keyedRows(afterJson)];
  if (before === undefined || after === undefined) return [];
  return diffObjects(before, after).filter((entry) => entry.kind !== 'unchanged');
}

/** A change stated in one line, the shape `HeldEdit` draws and `diffSteps`
 * already produces for pipeline steps. The three states are `recordDiff.ts`'s
 * own, reused wherever a reader is shown what would change. */
export interface ChangeRow {
  kind: 'added' | 'removed' | 'changed';
  summary: string;
}

/** `changesBetween`, said in lines. A changed row names the value it would
 * take; an added or removed one needs only its name. */
export function summarizeChanges(beforeJson: string, afterJson: string): ChangeRow[] {
  const rows: ChangeRow[] = [];
  for (const entry of changesBetween(beforeJson, afterJson)) {
    if (entry.kind === 'unchanged') continue;
    rows.push({
      kind: entry.kind,
      summary: entry.kind === 'changed' ? changedFields(entry) : entry.key,
    });
  }
  return rows;
}

/** `id \u2014 field, field`: the fields of a row that differ, not the row. A
 * source or a route rendered whole is a wall of JSON, and what the reader is
 * being asked about is which parts of it move. Falls back to the value where
 * the row is not an object with fields to name. */
function changedFields(entry: DiffEntry): string {
  const fields = diffObjects(entry.before, entry.after)
    .filter((field) => field.kind !== 'unchanged')
    .map((field) => field.key);
  const said = fields.length > 0 ? fields.join(', ') : formatDiffValue(entry.after);
  return `${entry.key} \u2014 ${said}`;
}
