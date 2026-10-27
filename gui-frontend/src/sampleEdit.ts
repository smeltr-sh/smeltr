// Editing a saved sample's records (`SampleEditor.svelte`). A record is edited as its fields, one cell each. An internal
// field that holds an object, as `__source` and `__capture` do, is a cell per field inside it, named by its path
// (`__source.offset`), as the capture's columns name them.

import {
  isInternal,
  pastedElements,
  recordsFromText,
  sampleSource,
  type SampleDto,
  type SampleRecordDto,
} from './api';

/** Where a record lists the fields whose value failed its type (`io::take_mistyped`): `smeltr.mistyped`. */
const TAG = 'smeltr';
const MISTYPED = 'mistyped';

/** One column of the editor: a field of the records, or a path into an internal object, and its type where the capture
 * recorded one. */
export interface EditColumn {
  name: string;
  type?: string;
}

/** The columns a record shows: its fields, an internal object's fields as their paths, and not a tag field that holds
 * only its list of mistyped fields, which shows as the cells it marks. A tag that also holds a destination's messages
 * is a field like any other. */
function recordColumns(record: SampleRecordDto): string[] {
  const onlyMistyped =
    mistypedOf(record) !== null && Object.keys(record[TAG] as Record<string, unknown>).length === 1;
  return Object.keys(record)
    .filter((k) => !(k === TAG && onlyMistyped))
    .flatMap((k) =>
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

function valueAt(record: SampleRecordDto, column: string): unknown {
  const [field, path] = split(column);
  return path === null ? record[field] : inner(record, field)?.[path];
}

/** Sets `column` in `record`, a copy the caller owns, or removes it for `undefined`. */
function setAt(record: SampleRecordDto, column: string, value: unknown) {
  const [field, path] = split(column);
  if (path === null) {
    if (value === undefined) delete record[field];
    else record[field] = value;
    return;
  }
  const object = { ...(inner(record, field) ?? {}) };
  if (value === undefined) delete object[path];
  else object[path] = value;
  record[field] = object;
}

/** The record's list of mistyped fields, or `null` when it has none to keep. */
function mistypedOf(record: SampleRecordDto): string[] | null {
  const tag = record[TAG];
  if (tag === null || typeof tag !== 'object' || Array.isArray(tag)) return null;
  const names = (tag as Record<string, unknown>)[MISTYPED];
  return Array.isArray(names) && names.every((n) => typeof n === 'string') ? names : null;
}

/** `found`, in the order the capture's columns name them, then the rest in the order first seen. A stored record's keys
 * come back sorted, so the columns are the only record of the order its source gave them. */
function inCaptureOrder(found: string[], captured: string[]): string[] {
  return [
    ...captured.filter((c) => found.includes(c)),
    ...found.filter((k) => !captured.includes(k)),
  ];
}

/** The columns the editor shows for `sample`: every field any record has, typed by the capture's column of the same
 * name. */
export function editColumns(sample: SampleDto): EditColumn[] {
  const types = new Map((sample.columns ?? []).map((c) => [c.name, c.type]));
  const names: string[] = [];
  for (const r of sample.records)
    for (const k of recordColumns(r)) if (!names.includes(k)) names.push(k);
  return inCaptureOrder(names, [...types.keys()]).map((name) => ({ name, type: types.get(name) }));
}

/** A cell's text: a string as it is, a missing field empty, anything else as JSON. */
export function cellText(record: SampleRecordDto, column: string): string {
  const v = valueAt(record, column);
  if (v === undefined) return '';
  return typeof v === 'string' ? v : JSON.stringify(v);
}

/** Whether `column`'s value in `record` failed its type when it was read. */
export function cellMistyped(record: SampleRecordDto, column: string): boolean {
  return mistypedOf(record)?.includes(column) ?? false;
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

/** `record` with `column` set to what `text` stands for, its mistyped list kept in step. An empty cell removes a field
 * that was not a string. */
export function withCell(
  record: SampleRecordDto,
  column: EditColumn,
  text: string,
): SampleRecordDto {
  const next: SampleRecordDto = { ...record };
  const previous = valueAt(record, column.name);
  const { value, mistyped } = parseCell(text, column.type, previous);
  const cleared = text === '' && typeof previous !== 'string' && column.type !== 'str';
  setAt(next, column.name, cleared ? undefined : value);
  // A byte record is read from the bytes its source stored, ahead of `_raw` (`read_stored`), so an edit to its text
  // is an edit to them.
  const source = sampleSource(record);
  if (column.name === '_raw' && typeof source?.bytes === 'string')
    next.__source = { ...source, bytes: base64(text) };
  const names = (mistypedOf(record) ?? []).filter((n) => n !== column.name);
  if (mistyped && !cleared) names.push(column.name);
  const held = record[TAG] ?? {};
  // A record's own field by that name, not an object, has no place for the list; the value fails when read regardless.
  if (typeof held !== 'object' || held === null || Array.isArray(held)) return next;
  const tag = { ...(held as Record<string, unknown>) };
  if (names.length > 0) next[TAG] = { ...tag, [MISTYPED]: names };
  else if (mistypedOf(record) !== null) {
    delete tag[MISTYPED];
    if (Object.keys(tag).length > 0) next[TAG] = tag;
    else delete next[TAG];
  }
  return next;
}

/** `text`'s UTF-8 bytes as base64, as a stored record holds bytes. */
function base64(text: string): string {
  let binary = '';
  for (const byte of new TextEncoder().encode(text)) binary += String.fromCharCode(byte);
  return btoa(binary);
}

/** Records pasted into a sample whose records are `columns`, read as Add sample reads a paste (`recordsFromText`): one
 * JSON value, an array's elements a record, or else a line a record. A sample with `_raw` takes a whole record, an
 * object with a `_raw` field, as it is, and anything else as `_raw` text; one without takes each as a JSON object of
 * its fields. `error` names the first that is not one. */
export function pastedRecords(
  text: string,
  columns: EditColumn[],
): { records: SampleRecordDto[] } | { error: string } {
  if (columns.length === 0 || columns.some((c) => c.name === '_raw'))
    return { records: recordsFromText(text) };
  const isObject = (v: unknown) => v !== null && typeof v === 'object' && !Array.isArray(v);
  const elements = pastedElements(text);
  if (elements) {
    const bad = elements.findIndex((e) => !isObject(e));
    if (bad >= 0) return { error: `record ${bad + 1} is not a JSON object` };
    return { records: elements as SampleRecordDto[] };
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
  return { records };
}

/** Whether `record` holds `query` in any of its fields. Case does not count. */
export function matches(record: SampleRecordDto, query: string): boolean {
  const q = query.trim().toLowerCase();
  return q === '' || holds(record, q);
}

/** Whether a value, or any value inside it, holds `q` as its text reads: not as JSON, where `\` and `"` are escaped. */
function holds(value: unknown, q: string): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'object') return Object.values(value).some((v) => holds(v, q));
  return String(value).toLowerCase().includes(q);
}
