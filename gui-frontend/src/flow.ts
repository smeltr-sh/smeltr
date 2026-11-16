// What the Status page draws of its flow (E2): a dense, gap-aware grid for the chart, read from
// `GET /api/series`, and a ranking by how far each source is from its normal
// (the Main storyboard's "Furthest from normal").
//
// A bucket the leader never wrote is absent from `points` -- a gap, not a zero
// (`src/leader/series.rs`) -- and every function here keeps that distinction rather than
// filling it in.

import type { FlowKind, FlowNormalDto, SeriesAnswerDto } from './api';

/** The window picker (E2: sixty minutes by default, out to thirty days). */
export const FLOW_WINDOWS: { label: string; minutes: number }[] = [
  { label: '15m', minutes: 15 },
  { label: '60m', minutes: 60 },
  { label: '24h', minutes: 24 * 60 },
  { label: '7d', minutes: 7 * 24 * 60 },
  { label: '30d', minutes: 30 * 24 * 60 },
];

export const DEFAULT_WINDOW_MINUTES = 60;

/** How many rows the ranked table shows (Main storyboard). */
export const FURTHEST_FROM_NORMAL_LIMIT = 8;

/** Each kind's rows in, rows out, bytes in and bytes out (`leader::FlowKind::metrics`). Out is what was handed to
 * destinations; a destination's own out, and the fleet's, is what destinations acknowledged. */
export const FLOW_METRICS: Record<FlowKind | 'fleet', [string, string, string, string]> = {
  fleet: [
    'smeltr_rows_polled_total',
    'smeltr_rows_delivered_total',
    'smeltr_bytes_polled_total',
    'smeltr_bytes_delivered_total',
  ],
  source: [
    'smeltr_rows_polled_total',
    'smeltr_rows_sent_total',
    'smeltr_bytes_polled_total',
    'smeltr_bytes_sent_total',
  ],
  pipeline: [
    'smeltr_route_matched_total',
    'smeltr_rows_sent_total',
    'smeltr_route_matched_bytes_total',
    'smeltr_bytes_sent_total',
  ],
  route: [
    'smeltr_route_matched_total',
    'smeltr_rows_sent_total',
    'smeltr_route_matched_bytes_total',
    'smeltr_bytes_sent_total',
  ],
  destination: [
    'smeltr_rows_sent_total',
    'smeltr_rows_delivered_total',
    'smeltr_bytes_sent_total',
    'smeltr_bytes_delivered_total',
  ],
};

/** Every series of `answer` added together, on each of its points from the first through `toMillis`: `null` where
 * nothing landed (E2, "a gap is not a zero"). */
export function denseAnswer(answer: SeriesAnswerDto, toMillis: number): (number | null)[] {
  const sums = new Map<number, number>();
  for (const s of answer.series) {
    for (const [at, value] of s.points) sums.set(at, (sums.get(at) ?? 0) + value);
  }
  const out: (number | null)[] = [];
  for (let at = answer.fromMillis; at <= toMillis; at += answer.pointMillis) {
    out.push(sums.get(at) ?? null);
  }
  return out;
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

/** A source's reading against its normal, as the Status page ranks it. `awayPercent` is how
 * far past its normal's nearest edge it is, in percent of that edge: 0 within its normal. */
export type RankedSource = FlowNormalDto & { awayPercent: number };

/** How far `n` is past its normal's nearest edge, in percent of that edge (`RankedSource`). The
 * high edge is above zero: the leader judges a source only where it usually reads rows. */
export function awayPercent(n: FlowNormalDto): number {
  if (n.now < n.low) return ((n.now - n.low) / n.low) * 100;
  if (n.now <= n.high) return 0;
  return ((n.now - n.high) / n.high) * 100;
}

/** How far from the middle of its normal `n` is: past 1 outside it, by how far past its edge;
 * within it, the share of the way from the middle to the edge. What the ranking sorts by, so
 * that within their normals the sources nearest an edge come first. */
function distance(n: FlowNormalDto): number {
  const away = awayPercent(n);
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
