// What the capture dialog derives from a capture's status: one chip per
// worker, and the rows shown under them. Pure, so it is tested without a
// dialog.

import { count } from './time';
import { keySpans } from './names';
import type {
  CaptureBatchDto,
  CaptureStatus,
  CaptureWorkerDto,
  HeldRecord,
  SampleRecordDto,
  SourceDto,
  StartPosition,
  startCapture,
} from './api';

/** The name of the column that holds an object's fields no other column names (`plan::tree::UNCLAIMED`): alone for
 * the root's, after the object's path and a dot for any other's. */
const UNCLAIMED = '...';

/** `batch` with every row holding every column's value: a constant column's `value` is copied onto each row. */
export function heldTable(batch: CaptureBatchDto): CaptureBatchDto {
  const carried = batch.columns.flatMap((c, i) => (c.value == null ? [i] : []));
  if (carried.length === batch.columns.length) return batch;
  const at = batch.columns.map((_, i) => carried.indexOf(i));
  return {
    columns: batch.columns.map(({ name, type }) => ({ name, type })),
    rows: batch.rows.map((row) => ({
      ...row,
      values: batch.columns.map((c, i) => c.value ?? row.values[at[i]]),
      failed: row.failed?.map((f) => carried[f]),
    })),
  };
}

/** The table of `records` a person pasted, as the leader makes it (`CaptureBatch::pasted`): one column, the root's
 * unclaimed fields, which holds each whole. */
export function pastedTable(records: SampleRecordDto[]): CaptureBatchDto {
  return {
    columns: [{ name: UNCLAIMED, type: 'variant' }],
    rows: records.map((record) => ({ values: [record] })),
  };
}

/** The column's place in a record object: the path of its field, or of the object whose unclaimed fields it holds. */
function columnPath(name: string): { path: string; unclaimed: boolean } {
  if (name === UNCLAIMED) return { path: '', unclaimed: true };
  return name.endsWith(`.${UNCLAIMED}`)
    ? { path: name.slice(0, -UNCLAIMED.length - 1), unclaimed: true }
    : { path: name, unclaimed: false };
}

/** Sets `object`'s own `key`. A field may be named as something every object inherits, like `__proto__`, and
 * assigning to that name would set the object's prototype instead of a key. */
function put(object: SampleRecordDto, key: string, value: unknown) {
  Object.defineProperty(object, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

/** Each row of `batch` as a record to show, and the names of the columns whose value failed its type on it. */
export function tableRecords(batch: CaptureBatchDto): HeldRecord[] {
  const { columns, rows } = heldTable(batch);
  const places = columns.map((c) => columnPath(c.name));
  return rows.map((row) => {
    const record: SampleRecordDto = {};
    row.values.forEach((value, i) => {
      if (value == null) return;
      const keys = keySpans(places[i].path).map((span) => span.key);
      const last = places[i].unclaimed ? undefined : keys.pop();
      let at = record;
      for (const key of keys) {
        if (!Object.hasOwn(at, key)) put(at, key, {});
        at = at[key] as SampleRecordDto;
      }
      if (last !== undefined) put(at, last, value);
      else if (typeof value === 'object') for (const [k, v] of Object.entries(value)) put(at, k, v);
    });
    const failed = (row.failed ?? []).map((f) => columns[f].name);
    return { record, ...(row.worker && { worker: row.worker }), ...(failed.length && { failed }) };
  });
}

/** One worker in the capture, and how many of the held records it sent. */
export interface WorkerChip extends CaptureWorkerDto {
  count: number;
}

/** How many records the capture holds. */
export function heldCount(status: CaptureStatus): number {
  return status.records.length;
}

/** Every worker the capture knows of, in the leader's order. The leader never
 * drops one, so a chosen chip stays on screen. */
export function workerChips(status: CaptureStatus): WorkerChip[] {
  const counts = new Map<string, number>();
  for (const { worker } of status.records)
    if (worker !== undefined) counts.set(worker, (counts.get(worker) ?? 0) + 1);
  return status.workers.map(({ worker, state }) => ({
    worker,
    state,
    count: counts.get(worker) ?? 0,
  }));
}

/** The held records, or only one worker's when a chip is chosen. */
export function shownRecords(status: CaptureStatus, only: string | null): HeldRecord[] {
  return only === null ? status.records : status.records.filter((r) => r.worker === only);
}

/** Where a record a block holds says why it blocked (`worker::capture::EXCEPTION_MESSAGE`). It is not a field of
 * the record. */
export const EXCEPTION_MESSAGE = ['__exception', 'message'];

/** A held record as the grid shows it: its fields, and what failed on it apart from them. */
export interface CapturedRecord {
  record: Record<string, unknown>;
  exceptions: string[];
  worker?: string;
}

/** The value at `path` in `record`, taken out of it. An object the take leaves empty goes too. */
function take(record: Record<string, unknown>, path: string[]): unknown {
  const [key, ...rest] = path;
  const at = record[key];
  if (rest.length === 0) {
    delete record[key];
    return at;
  }
  if (at === null || typeof at !== 'object' || Array.isArray(at)) return undefined;
  const inner = { ...(at as Record<string, unknown>) };
  const value = take(inner, rest);
  if (Object.keys(inner).length === 0) delete record[key];
  else record[key] = inner;
  return value;
}

/** The value at `name`, a field's name, in a held record, or undefined where it has none. */
export function valueAt(record: Record<string, unknown>, name: string): unknown {
  let at: unknown = record;
  for (const { key } of keySpans(name)) {
    if (at === null || typeof at !== 'object' || Array.isArray(at)) return undefined;
    at = (at as Record<string, unknown>)[key];
  }
  return at;
}

/** Each of `held` as the grid shows it: why a block holds it, and each value that did not read as its column's type
 * (`columns`), as exceptions rather than fields. */
export function heldRows(held: HeldRecord[], columns: CaptureStatus['columns']): CapturedRecord[] {
  const types = new Map(columns.map((c) => [c.name, c.type]));
  return held.map(({ worker, record: sent, failed }) => {
    const record = { ...sent };
    const exceptions: string[] = [];
    const message = take(record, EXCEPTION_MESSAGE);
    if (message !== undefined && message !== null) exceptions.push(String(message));
    for (const name of failed ?? [])
      exceptions.push(`${name} could not be read as ${types.get(name) ?? 'its type'}`);
    return worker === undefined ? { record, exceptions } : { record, exceptions, worker };
  });
}

/** A captured value as one cell shows it: a string as it is, anything else as JSON. */
export function cellText(value: unknown): string {
  return typeof value === 'string' ? value : JSON.stringify(value);
}

/** Whether nothing about the capture can change any more: it is over, and every
 * worker it knows of has said why it stopped. A finished capture still takes a
 * late worker's reason. */
export function settled(status: CaptureStatus): boolean {
  return status.done && status.workers.every((w) => w.state === 'answered');
}

/** One line on where the capture stands, for the dialog's body. */
export function progressText(status: CaptureStatus): string {
  const held = count(heldCount(status), 'record');
  const senders = new Set(status.records.flatMap((r) => (r.worker === undefined ? [] : [r.worker])))
    .size;
  const from = `from ${senders} of ${count(status.workers.length, 'worker')}`;
  if (status.stopped) return `Stopped with ${status.secondsLeft}s left. ${held} ${from}.`;
  if (status.done) return `Finished. ${held} ${from}.`;
  return `${held} so far ${from}.`;
}

/** What a capture reads. A source travels as the author has it, since a worker follows main and
 * may never have seen it. A destination is named: it is one the group's workers already run. */
export type CaptureTarget = { source: SourceDto } | { destination: string };

/** Where a capture taps: at a source, or at a destination. */
export type CaptureAt = 'source' | 'destination';

/** What the capture form asks for, whatever the target. */
export interface CaptureAsk {
  group: string;
  filter: string;
  maxMatches: number;
  deadlineSecs: number;
  /** Where a stream source's capture starts. Absent, it reads from the latest. */
  start?: StartPosition;
  /** A receiver captured as sent, which takes no filter. */
  raw?: boolean;
}

/** The request that starts a capture. An empty filter is left out: it keeps everything. */
export function captureRequest(
  target: CaptureTarget,
  ask: CaptureAsk,
): Parameters<typeof startCapture>[0] {
  const filter = (!ask.raw && ask.filter.trim()) || undefined;
  const bounds = { maxMatches: ask.maxMatches, deadlineSecs: ask.deadlineSecs };
  if ('destination' in target)
    return { group: ask.group, destination: target.destination, filter, ...bounds };
  return {
    group: ask.group,
    source: target.source,
    filter,
    start: ask.start,
    ...(ask.raw ? { raw: true } : {}),
    ...bounds,
  };
}

/** A piece of help text; `code` marks a field's name. */
export interface HelpPart {
  text: string;
  code?: boolean;
}

/** What the filter reads, which is where the capture taps: a source's record as it arrived,
 * or a destination's row as its pipeline finished it. */
export function filterHelp(at: CaptureAt): HelpPart[] {
  const form = 'An expression in the same form as a route condition. ';
  if (at === 'destination')
    return [
      {
        text: `${form}It reads the row as its pipeline finished it: the fields the pipeline writes.`,
      },
    ];
  return [
    { text: `${form}It reads the record as it arrived, before any pipeline: ` },
    { text: '_raw', code: true },
    { text: ' and the fields the source declares, not a parsed field.' },
  ];
}

/** An example filter for the empty field. */
export function filterPlaceholder(at: CaptureAt): string {
  return at === 'destination' ? 'level == "error"' : '_raw.includes("error")';
}
