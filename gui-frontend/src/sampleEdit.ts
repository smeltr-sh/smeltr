// Editing a saved sample's table (`SampleEditor.svelte`), one cell each. A column that holds an object's unclaimed
// fields shows as a column per field in it, and an internal field there that holds an object, as `__source` does, as
// a column per field inside that, named by its path (`__source.offset`).

import { keySpans } from './names';
import {
  isInternal,
  pastedElements,
  recordsFromText,
  type CaptureColumnDto,
  type CapturedRowDto,
  type SampleDto,
  type SampleRecordDto,
} from './api';
import { heldTable } from './capture';

/** One column of the editor: column `at` of the table, or `field` of the object that column's cells hold. */
export interface EditColumn {
  name: string;
  type?: string;
  at: number;
  field?: string;
}

/** The columns a record shows: its fields, and an internal object's fields as their paths. */
function recordColumns(record: SampleRecordDto): string[] {
  return Object.keys(record).flatMap((k) =>
    inner(record, k) ? Object.keys(inner(record, k)!).map((f) => `${k}.${f}`) : [k],
  );
}

/** The object an internal field `key` holds, whose fields are columns of their own. */
function inner(record: SampleRecordDto, key: string): Record<string, unknown> | null {
  const v = record[key];
  return isInternal(key) && v !== null && typeof v === 'object' && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : null;
}

/** `column`'s field, and the field inside it a path names. */
function split(column: string): [string, string | null] {
  const dot = column.indexOf('.');
  return isInternal(column) && dot > 0
    ? [column.slice(0, dot), column.slice(dot + 1)]
    : [column, null];
}

/** The object `row` holds in column `at`, where it holds one. */
function held(row: CapturedRowDto, at: number): SampleRecordDto | null {
  const v = row.values[at];
  return v !== null && typeof v === 'object' && !Array.isArray(v) ? (v as SampleRecordDto) : null;
}

function valueAt(row: CapturedRowDto, column: EditColumn): unknown {
  if (column.field === undefined) return row.values[column.at] ?? undefined;
  const record = held(row, column.at);
  const [field, path] = split(column.field);
  return record && (path === null ? record[field] : inner(record, field)?.[path]);
}

/** Sets `column` in `values`, a copy the caller owns, or clears it for `undefined`. */
function setAt(values: unknown[], column: EditColumn, value: unknown) {
  if (column.field === undefined) {
    values[column.at] = value ?? null;
    return;
  }
  const record = { ...(values[column.at] as SampleRecordDto | null) };
  const [field, path] = split(column.field);
  const object = path === null ? record : { ...(inner(record, field) ?? {}) };
  if (value === undefined) delete object[path ?? field];
  else object[path ?? field] = value;
  if (path !== null) record[field] = object;
  values[column.at] = record;
}

/** A sample's table as the editor holds it, every row with every value, and the columns it shows: each of the
 * table's, and for one of unclaimed fields every field any row has there. */
export function editTable(sample: SampleDto): {
  columns: CaptureColumnDto[];
  rows: CapturedRowDto[];
  shown: EditColumn[];
} {
  const { columns, rows } = heldTable(sample);
  const shown = columns.flatMap((c, at): EditColumn[] => {
    if (!c.name.endsWith('...')) return [{ ...c, at }];
    const fields: string[] = [];
    for (const row of rows)
      for (const k of recordColumns(held(row, at) ?? {})) if (!fields.includes(k)) fields.push(k);
    return fields.map((field) => ({ name: c.name.slice(0, -3) + field, at, field }));
  });
  return { columns, rows, shown };
}

/** A cell's text: a string as it is, a missing value empty, anything else as JSON. */
export function cellText(row: CapturedRowDto, column: EditColumn): string {
  const v = valueAt(row, column);
  if (v === undefined) return '';
  return typeof v === 'string' ? v : JSON.stringify(v);
}

/** Whether `column`'s value in `row` failed its type when it was read. */
export function cellMistyped(row: CapturedRowDto, column: EditColumn): boolean {
  return column.field === undefined && (row.failed?.includes(column.at) ?? false);
}

/** The value `text` stands for in a column of `type`, and whether it fails that type. A value that fails is kept as
 * typed, as a capture keeps one the source spelled wrong, and fails again when the sample is read. With no type, the
 * value keeps the kind it had: a string stays a string, anything else is read as JSON where it parses. */
export function parseCell(
  text: string,
  type: string | undefined,
  previous: unknown,
): { value: unknown; mistyped: boolean } {
  if (type === 'i64' || type === 'f64') {
    const n = text.trim() === '' ? NaN : Number(text);
    const ok = Number.isFinite(n) && (type === 'f64' || Number.isInteger(n));
    return ok ? { value: n, mistyped: false } : { value: text, mistyped: true };
  }
  if (type === 'bool') {
    if (text === 'true' || text === 'false') return { value: text === 'true', mistyped: false };
    return { value: text, mistyped: true };
  }
  if (typeof previous === 'string' || previous === undefined || type === 'str')
    return { value: text, mistyped: false };
  try {
    return { value: JSON.parse(text), mistyped: false };
  } catch {
    return { value: text, mistyped: false };
  }
}

/** `row` with `column` set to what `text` stands for, its failed cells kept in step. An empty cell clears a value that
 * was not a string. `shown` is every column of the editor. */
export function withCell(
  row: CapturedRowDto,
  shown: EditColumn[],
  column: EditColumn,
  text: string,
): CapturedRowDto {
  const values = [...row.values];
  const previous = valueAt(row, column);
  const { value, mistyped } = parseCell(text, column.type, previous);
  const cleared = text === '' && typeof previous !== 'string' && column.type !== 'str';
  setAt(values, column, cleared ? undefined : value);
  // A byte record is read from the bytes its source stored, ahead of `_raw` (`read_table`), so an edit to its text
  // is an edit to them.
  const bytes = shown.find((c) => c.name === '__source.bytes');
  if (column.name === '_raw' && bytes && typeof valueAt(row, bytes) === 'string')
    setAt(values, bytes, base64(text));
  const failed = (row.failed ?? []).filter((f) => f !== column.at);
  if (mistyped && !cleared && column.field === undefined) failed.push(column.at);
  return { ...row, values, failed };
}

/** `text`'s UTF-8 bytes as base64, as a stored record holds bytes. */
function base64(text: string): string {
  let binary = '';
  for (const byte of new TextEncoder().encode(text)) binary += String.fromCharCode(byte);
  return btoa(binary);
}

/** `record` as a row under `columns`: each named column's value from its path, and each column of unclaimed fields
 * what is left of its object once the columns under it have theirs. */
function rowOf(columns: CaptureColumnDto[], record: SampleRecordDto): CapturedRowDto {
  const left = structuredClone(record);
  const take = (path: string): unknown => {
    const keys = keySpans(path).map((span) => span.key);
    const last = keys.pop();
    let at: unknown = left;
    for (const key of keys) at = (at as SampleRecordDto | null)?.[key];
    if (at === null || typeof at !== 'object') return null;
    if (last === undefined) return at;
    const value = (at as SampleRecordDto)[last];
    delete (at as SampleRecordDto)[last];
    return value ?? null;
  };
  const values: unknown[] = columns.map((c) => (c.name.endsWith('...') ? null : take(c.name)));
  // The deepest object first, so the one above it holds what is left.
  const unclaimed = columns.flatMap((c, at) => (c.name.endsWith('...') ? [{ c, at }] : []));
  for (const { c, at } of unclaimed.sort((a, b) => b.c.name.length - a.c.name.length)) {
    const rest = take(c.name.slice(0, -3).replace(/\.$/, ''));
    values[at] = rest !== null && Object.keys(rest as object).length > 0 ? rest : null;
  }
  return { values };
}

/** Records pasted into a sample of `columns`, read as Add sample reads a paste (`recordsFromText`): one JSON value,
 * an array's elements a record, or else a line a record. A sample that holds `_raw` takes a whole record, an object
 * with a `_raw` field, as it is, and anything else as `_raw` text; one that does not takes each as a JSON object of
 * its fields. `error` names the first that is not one. */
export function pastedRows(
  text: string,
  columns: CaptureColumnDto[],
  shown: EditColumn[],
): { rows: CapturedRowDto[] } | { error: string } {
  const rows = (records: SampleRecordDto[]) => ({ rows: records.map((r) => rowOf(columns, r)) });
  if (shown.some((c) => c.name === '_raw')) return rows(recordsFromText(text));
  const isObject = (v: unknown) => v !== null && typeof v === 'object' && !Array.isArray(v);
  const elements = pastedElements(text);
  if (elements) {
    const bad = elements.findIndex((e) => !isObject(e));
    if (bad >= 0) return { error: `record ${bad + 1} is not a JSON object` };
    return rows(elements as SampleRecordDto[]);
  }
  const records: SampleRecordDto[] = [];
  const lines = text.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (line === '') continue;
    let parsed: unknown = null;
    try {
      parsed = JSON.parse(line);
    } catch {
      // Not JSON: refused below.
    }
    if (!isObject(parsed)) return { error: `line ${i + 1} is not a JSON object` };
    records.push(parsed as SampleRecordDto);
  }
  return rows(records);
}

/** Whether `row` holds `query` in any of its values. Case does not count. */
export function matches(row: CapturedRowDto, query: string): boolean {
  const q = query.trim().toLowerCase();
  return q === '' || holds(row.values, q);
}

/** Whether a value, or any value inside it, holds `q` as its text reads: not as JSON, where `\` and `"` are escaped. */
function holds(value: unknown, q: string): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'object') return Object.values(value).some((v) => holds(v, q));
  return String(value).toLowerCase().includes(q);
}
