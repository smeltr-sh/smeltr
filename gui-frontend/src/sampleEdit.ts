// Editing a saved sample's records (`SampleEditor.svelte`). A record is edited as its own fields, one cell each;
// what its source set, under `__source`, is never shown as a cell and rides along untouched, so a pipeline that
// reads where a record was read from runs as before. A record added by paste was read from nowhere and has none.

import {
  pastedElements,
  recordsFromText,
  sampleSource,
  type SampleDto,
  type SampleRecordDto,
} from './api';

/** Where a record lists the fields whose value failed its type (`io::take_mistyped`): `smeltr.mistyped`. */
const TAG = 'smeltr';
const MISTYPED = 'mistyped';

/** One column of the editor: a top-level field of the records, and its type where the capture recorded one. */
export interface EditColumn {
  name: string;
  type?: string;
}

/** The fields a record shows as cells: everything but what its source set and a tag field that holds only its list of
 * mistyped fields. A tag that also holds a destination's messages is a field like any other. */
function ownKeys(record: SampleRecordDto): string[] {
  const onlyMistyped =
    mistypedOf(record) !== null && Object.keys(record[TAG] as Record<string, unknown>).length === 1;
  return Object.keys(record).filter((k) => k !== '__source' && !(k === TAG && onlyMistyped));
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
  for (const r of sample.records) for (const k of ownKeys(r)) if (!names.includes(k)) names.push(k);
  return inCaptureOrder(names, [...types.keys()]).map((name) => ({ name, type: types.get(name) }));
}

/** The `__source` fields that tell `sample`'s records apart: those whose value is not the same on every record. The
 * source's name and type never do, nor its bytes, which are the record itself. A SQL source's fields are one
 * table's, the same on every row, so it has none. */
export function readFromFields(sample: SampleDto): string[] {
  const sources = sample.records.map((r) => sampleSource(r) ?? {});
  const keys: string[] = [];
  for (const s of sources) for (const k of Object.keys(s)) if (!keys.includes(k)) keys.push(k);
  const captured = (sample.columns ?? [])
    .map((c) => c.name)
    .filter((n) => n.startsWith('__source.'))
    .map((n) => n.slice('__source.'.length));
  return inCaptureOrder(keys, captured).filter((k) => {
    if (k === 'name' || k === 'type' || k === 'bytes') return false;
    const first = JSON.stringify(sources[0][k]);
    return sources.some((s) => JSON.stringify(s[k]) !== first);
  });
}

/** Where `record` was read from, as `fields` name it: `p0 · 18233`-style, its values joined. Empty for a pasted one. */
export function readFrom(record: SampleRecordDto, fields: string[]): string {
  const source = sampleSource(record);
  if (!source) return '';
  return fields
    .map((f) => source[f])
    .filter((v) => v !== undefined && v !== null)
    .map((v) => (typeof v === 'string' ? v : JSON.stringify(v)))
    .join(' · ');
}

/** A cell's text: a string as it is, a missing field empty, anything else as JSON. */
export function cellText(record: SampleRecordDto, column: string): string {
  const v = record[column];
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
  const previous = record[column.name];
  const { value, mistyped } = parseCell(text, column.type, previous);
  const cleared = text === '' && typeof previous !== 'string' && column.type !== 'str';
  if (cleared) delete next[column.name];
  else next[column.name] = value;
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
 * JSON value, an array's elements a record, or else a line a record. A sample of `_raw` text takes them as text; one
 * of other fields takes each as a JSON object of its fields. `error` names the first that is not one. */
export function pastedRecords(
  text: string,
  columns: EditColumn[],
): { records: SampleRecordDto[] } | { error: string } {
  const raw = columns.length === 0 || (columns.length === 1 && columns[0].name === '_raw');
  if (raw) return { records: recordsFromText(text) };
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

/** Whether `record` holds `query`, in any of its own fields or where it was read from. Case does not count. */
export function matches(record: SampleRecordDto, query: string): boolean {
  const q = query.trim().toLowerCase();
  return q === '' || JSON.stringify(record).toLowerCase().includes(q);
}
