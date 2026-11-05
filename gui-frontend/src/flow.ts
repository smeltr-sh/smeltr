// Turning a `FlowResponseDto` (E2, `GET /api/flow`) into what the Status page draws: a
// dense, gap-aware grid for the chart, and a ranking by how far each source is from its normal
// (the Main storyboard's "Furthest from normal").
//
// A bucket the leader never wrote is absent from `points` -- a gap, not a zero
// (`src/leader/series.rs`) -- and every function here keeps that distinction rather than
// filling it in.

import type { FlowNormalDto, FlowPointDto, FlowSeriesDto } from './api';

/** The window picker (E2: sixty minutes by default, out to thirty days). */
export const FLOW_WINDOWS: { label: string; minutes: number }[] = [
  { label: '15m', minutes: 15 },
  { label: '60m', minutes: 60 },
  { label: '24h', minutes: 24 * 60 },
  { label: '7d', minutes: 7 * 24 * 60 },
  { label: '30d', minutes: 30 * 24 * 60 },
];

export const DEFAULT_WINDOW_MINUTES = 60;

/** The finest tier's bucket width (`series::Tier::TenSeconds`). A window wider than that
 * tier holds is served from a rollup, and the last few minutes have not aged into it yet --
 * so an empty answer there means "not rolled up", not "nothing ran". */
export const FINEST_BUCKET_MILLIS = 10_000;

/** How many rows the ranked table shows (Main storyboard). */
export const FURTHEST_FROM_NORMAL_LIMIT = 8;

/** The per-second rate implied by one bucket's total, whatever the bucket counts -- rows for
 * the chart, payload bytes for the reading beside it. */
export function bucketRate(point: FlowPointDto, bucketMillis: number): number {
  return point.value / (bucketMillis / 1000);
}

/** The first bucket boundary at or after `fromMillis`: where a dense series starts, and so
 * the instant its slot 0 stands for. Aligned inward, because a bucket that began before the
 * window is not one the window can show part of -- a bucket is whole or it is absent. */
export function firstBucket(bucketMillis: number, fromMillis: number): number {
  return Math.ceil(fromMillis / bucketMillis) * bucketMillis;
}

/** `points` reindexed onto every expected bucket boundary from `fromMillis` to `toMillis`,
 * step `bucketMillis` -- `null` where the leader recorded nothing. What a chart draws
 * through rather than across (E2, "a gap is not a zero"). */
export function denseSeries(
  points: FlowPointDto[],
  bucketMillis: number,
  fromMillis: number,
  toMillis: number,
): (number | null)[] {
  const byBucket = new Map(points.map((p) => [p.bucketMillis, p.value]));
  const start = firstBucket(bucketMillis, fromMillis);
  const end = Math.floor(toMillis / bucketMillis) * bucketMillis;
  const out: (number | null)[] = [];
  for (let t = start; t <= end; t += bucketMillis) {
    const v = byBucket.get(t);
    out.push(v === undefined ? null : v);
  }
  return out;
}

/** `denseSeries` per second rather than per bucket -- the unit the panel's own headline
 * reads (`currentRate`), so a chart drawn from this and the number above it cannot
 * disagree. It is also the only unit that holds across the window picker: a bucket is 10s
 * at sixty minutes and an hour at thirty days, so raw totals from two windows are not the
 * same measurement. A gap stays a gap; there is no rate where there was no bucket. */
export function denseRates(
  points: FlowPointDto[],
  bucketMillis: number,
  fromMillis: number,
  toMillis: number,
): (number | null)[] {
  const start = firstBucket(bucketMillis, fromMillis);
  return denseSeries(points, bucketMillis, fromMillis, toMillis).map((value, i) =>
    value === null
      ? null
      : bucketRate({ bucketMillis: start + i * bucketMillis, value }, bucketMillis),
  );
}

/** Every subject of a metric summed bucket by bucket, for a whole-fleet total. A bucket is
 * present the moment any one subject reported into it -- the rest contribute nothing, which
 * is correct addition, not a guess about what they were doing -- and absent only when
 * nothing anywhere did. */
export function sumSeries(series: FlowSeriesDto[]): FlowPointDto[] {
  const totals = new Map<number, number>();
  for (const s of series) {
    for (const p of s.points) {
      totals.set(p.bucketMillis, (totals.get(p.bucketMillis) ?? 0) + p.value);
    }
  }
  return [...totals.entries()]
    .sort(([a], [b]) => a - b)
    .map(([bucketMillis, value]) => ({ bucketMillis, value }));
}

/** The bucket before the one `nowMillis` falls in: the most recent one that has closed.
 *
 * Exported because a chart has to end here too, not at `nowMillis`. The bucket covering
 * now is still filling, so its total is whatever has landed so far and reads as a dip
 * that fills itself in on the next poll. A chart ending there and a headline reading the
 * closed bucket would be two answers to the same question. */
export function lastClosedBucket(bucketMillis: number, nowMillis: number): number {
  return Math.floor(nowMillis / bucketMillis) * bucketMillis - bucketMillis;
}

/** The rate over the bucket that has just closed, or `null` where the series does not hold
 * it.
 *
 * Not the bucket covering `nowMillis`. That one is still filling, so its running total over
 * the bucket's full width climbs from nothing to the true rate and starts again -- and
 * before its first row lands there is no row there at all, which is not a rate of zero. A
 * closed bucket is a whole count over a known width. `null` rather than 0 where the series
 * does not hold it, the same distinction the chart draws as a gap. */
export function currentRate(
  points: FlowPointDto[],
  bucketMillis: number,
  nowMillis: number,
): number | null {
  const closed = lastClosedBucket(bucketMillis, nowMillis);
  const point = points.find((p) => p.bucketMillis === closed);
  return point ? bucketRate(point, bucketMillis) : null;
}

/** A source's last half hour against its normal, as the Status page ranks it. `awayPercent` is how
 * far past its normal's nearest edge it is, in percent of that edge: 0 within its normal, and
 * `null` above a normal of none, which any rate is infinitely past. */
export type RankedSource = FlowNormalDto & { awayPercent: number | null };

/** How far `n` is past its normal's nearest edge, in percent of that edge (`RankedSource`). */
export function awayPercent(n: FlowNormalDto): number | null {
  if (n.now < n.low) return ((n.now - n.low) / n.low) * 100;
  if (n.now <= n.high) return 0;
  return n.high > 0 ? ((n.now - n.high) / n.high) * 100 : null;
}

/** How far from the middle of its normal `n` is: past 1 outside it, by how far past its edge;
 * within it, the share of the way from the middle to the edge. What the ranking sorts by, so
 * that within their normals the sources nearest an edge come first. */
function distance(n: FlowNormalDto): number {
  const away = awayPercent(n);
  if (away === null) return Infinity;
  if (away !== 0) return 1 + Math.abs(away) / 100;
  const half = (n.high - n.low) / 2;
  return half > 0 ? Math.abs(n.now - (n.low + half)) / half : 0;
}

/** Sources ranked by how far each is from its normal -- not by size, so one dying source is not
 * hidden by a hundred healthy ones (Main storyboard, "Furthest from normal"). The warning reads the
 * same normal, so a source ranked outside it is one the Issues page names. */
export function furthestFromNormal(
  normals: FlowNormalDto[],
  limit: number = FURTHEST_FROM_NORMAL_LIMIT,
): RankedSource[] {
  return normals
    .toSorted((a, b) => distance(b) - distance(a))
    .slice(0, limit)
    .map((n) => ({ ...n, awayPercent: awayPercent(n) }));
}
