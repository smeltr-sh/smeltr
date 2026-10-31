// What the capture dialog derives from a capture's status: one chip per
// worker, and the rows shown under them. Pure, so it is tested without a
// dialog.

import { count } from './time';
import type {
  CaptureBatch,
  CaptureStatus,
  CaptureWorkerDto,
  SourceDto,
  StartPosition,
  startCapture,
} from './api';

/** One worker in the capture, and how many of the held records it sent. */
export interface WorkerChip extends CaptureWorkerDto {
  count: number;
}

/** How many records the capture holds: its rows across every batch. */
export function heldCount(status: CaptureStatus): number {
  return status.batches.reduce((n, b) => n + b.rows.length, 0);
}

/** Every worker the capture knows of, in the leader's order. The leader never
 * drops one, so a chosen chip stays on screen. */
export function workerChips(status: CaptureStatus): WorkerChip[] {
  const counts = new Map<string, number>();
  for (const b of status.batches)
    for (const r of b.rows)
      if (r.worker !== undefined) counts.set(r.worker, (counts.get(r.worker) ?? 0) + 1);
  return status.workers.map(({ worker, state }) => ({
    worker,
    state,
    count: counts.get(worker) ?? 0,
  }));
}

/** The held batches, or only one worker's rows when a chip is chosen. A batch
 * left with no row is left out. */
export function shownRecords(status: CaptureStatus, only: string | null): CaptureBatch[] {
  const batches =
    only === null
      ? status.batches
      : status.batches.map((b) => ({ ...b, rows: b.rows.filter((r) => r.worker === only) }));
  return batches.filter((b) => b.rows.length > 0);
}

/** The first `max` rows across the batches, in order. */
export function firstRows(batches: CaptureBatch[], max: number): CaptureBatch[] {
  const out: CaptureBatch[] = [];
  let left = max;
  for (const b of batches) {
    if (left <= 0) break;
    out.push({ ...b, rows: b.rows.slice(0, left) });
    left -= b.rows.length;
  }
  return out;
}

/** The indexes of a batch's columns that hold a value on some row. */
export function filledColumns(batch: CaptureBatch): number[] {
  return batch.columns
    .map((_, i) => i)
    .filter((i) => batch.rows.some((r) => r.values[i] !== null && r.values[i] !== undefined));
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
  const senders = new Set(
    status.batches.flatMap((b) =>
      b.rows.flatMap((r) => (r.worker === undefined ? [] : [r.worker])),
    ),
  ).size;
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
}

/** The request that starts a capture. An empty filter is left out: it keeps everything. */
export function captureRequest(
  target: CaptureTarget,
  ask: CaptureAsk,
): Parameters<typeof startCapture>[0] {
  const filter = ask.filter.trim() || undefined;
  const bounds = { maxMatches: ask.maxMatches, deadlineSecs: ask.deadlineSecs };
  if ('destination' in target)
    return { group: ask.group, destination: target.destination, filter, ...bounds };
  return {
    group: ask.group,
    source: target.source,
    filter,
    start: ask.start,
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
