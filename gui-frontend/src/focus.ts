// The panel's arithmetic (design doc "The map", The panel): figures and their formatting, a
// window's buckets laid onto a fixed grid, and one page of a destination's worker queues.
// `FocusView.svelte` draws what these return.

import type { ExpectedPointDto, QueueFillDto, SpreadDto } from './api';

/** What a pipeline spent across all its steps -- the denominator every bar is drawn against, summed
 * once rather than once per row. */
export function totalMicros(steps: { micros: number }[]): number {
  return steps.reduce((sum, s) => sum + s.micros, 0);
}

/** A step's share of `spent`, for the bar beside it. `null` when nothing was spent at all, where
 * every step would otherwise read as the whole of nothing. */
export function shareOfTime(micros: number, spent: number): number | null {
  return spent > 0 ? micros / spent : null;
}

/** Microseconds at the precision that says something: under a millisecond the digits matter, past
 * it they do not. */
export function micros(value: number): string {
  if (value < 1000) return `${value.toFixed(value < 10 ? 1 : 0)} µs`;
  if (value < 1_000_000) return `${(value / 1000).toFixed(1)} ms`;
  return `${(value / 1_000_000).toFixed(1)} s`;
}

/** Thousands separated, and never a decimal: these are counts of rows. */
export function rows(n: number): string {
  return Math.round(n).toLocaleString();
}

/** A rate per second: whole from ten up, one decimal below, so a quiet source does not read as
 * stopped. */
export function perSecond(n: number): string {
  return Math.abs(n) >= 10 ? rows(n) : String(Math.round(n * 10) / 10);
}

/** A share as a percentage, at the precision that still says something: `16.4%`, and `<0.1%` for
 * anything smaller than that reads as, which is not the same as none. */
export function percent(value: number | null): string {
  if (value === null) return '—';
  if (value === 0) return '0%';
  if (value < 0.001) return '<0.1%';
  return `${(value * 100).toFixed(1)}%`;
}

// -- the queues ---------------------------------------------------------------
//
// A destination has one queue per worker running it, each bounded in bytes. The fill is the only
// figure on the panel that is not a total over the window: it is a level, read off the freshest
// heartbeat (`src/leader/stats.rs`, `Live`). The panel says so, because two clocks in one panel
// with nothing to tell them apart is a lie.

/** How full a queue has to be before it is drawn as a problem rather than as traffic. A queue at
 * this much is one batch from refusing an offer and parking the lanes that feed it. */
export const QUEUE_ALARM = 0.9;

/** How full one queue is, 0 to 1, or more where it holds more than its budget. Zero where the
 * budget is missing or nonsense: a division that yields an infinity is not something to draw a bar
 * from. */
export function queueShare(queue: QueueFillDto): number {
  if (!(queue.budget > 0)) return 0;
  return queue.bytes / queue.budget;
}

/** A round trip as a reader wants it: whole milliseconds, one decimal under ten. */
export function millis(ms: number): string {
  return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`;
}

// -- the spreads --------------------------------------------------------------
//
// A spread is one figure bucket by bucket across the window (`src/leader/stats.rs`, `Spread`).
// The panel draws one as a line across the window.

/** The spread's buckets laid on the window's grid, oldest first, `null` where the leader recorded
 * nothing -- an absent bucket has to stay a gap, or a source that went quiet for a minute draws as
 * if it never did. The grid ends at the last finished bucket, since the one still filling is not
 * in the spread; a point outside the grid is not drawn. `nowMillis` is the leader's clock
 * (`StatsResponse.nowMillis`), the one the buckets are on: the browser's may sit a bucket off,
 * which would drop the newest bucket or draw a gap the leader had not finished yet. */
export function spreadSlots(
  spread: SpreadDto,
  bucketMillis: number,
  windowMillis: number,
  nowMillis: number,
): (number | null)[] {
  const count = Math.max(1, Math.round(windowMillis / bucketMillis));
  const nowBucket = Math.floor(nowMillis / bucketMillis) * bucketMillis;
  const first = nowBucket - count * bucketMillis;
  const slots: (number | null)[] = Array.from({ length: count }, () => null);
  for (const point of spread.buckets) {
    const i = Math.round((point.bucketMillis - first) / bucketMillis);
    if (i >= 0 && i < count) slots[i] = point.value;
  }
  return slots;
}

/** When each of `spreadSlots`' slots began, in epoch milliseconds, on the same grid. */
export function slotTimes(bucketMillis: number, windowMillis: number, nowMillis: number): number[] {
  const count = Math.max(1, Math.round(windowMillis / bucketMillis));
  const first = Math.floor(nowMillis / bucketMillis) * bucketMillis - count * bucketMillis;
  return Array.from({ length: count }, (_, i) => first + i * bucketMillis);
}

/** The expected band at each of `times`: the span that holds it, else `null`. */
export function expectedSlots(
  points: ExpectedPointDto[],
  times: number[],
): ({ low: number; high: number } | null)[] {
  return times.map((at) => {
    const p = points.find((p) => p.fromMillis <= at && at < p.toMillis);
    return p ? { low: p.low, high: p.high } : null;
  });
}

/** One page of a destination's worker queues, in the order the leader sends them -- busiest
 * current worker first, stale ones last -- with `page` clamped into range, so a page left over
 * from a longer list still shows the last one. `start` and `end` count from 1, for "9–16 of 40". */
export function workerPage<T>(
  queues: T[],
  page: number,
  size: number,
): { rows: T[]; page: number; pages: number; start: number; end: number; total: number } {
  const total = queues.length;
  const pages = Math.max(1, Math.ceil(total / size));
  const at = Math.min(Math.max(0, page), pages - 1);
  const rows = queues.slice(at * size, at * size + size);
  return {
    rows,
    page: at,
    pages,
    start: total === 0 ? 0 : at * size + 1,
    end: at * size + rows.length,
    total,
  };
}
