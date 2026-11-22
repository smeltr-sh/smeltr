// `GET /api/series` and `GET /api/series/metrics` (`leader::query`), answered from every point the staged fleet's
// leader stored, as `tools/screenshots/demo.mjs` records them: the demo's leader asks these of what the fleet kept, so
// every metric, filter, split, window and clock the Metrics page can ask for has its answer. `series.test.ts` holds
// the port to the leader's own answers, which the recording keeps beside the points.

/** One subject's points, as `series_points` keeps them: per tier, its code, each bucket's start as the gap from the
 * one before it in milliseconds (the first's from zero), and each bucket's value. */
export interface StoredSeries {
  metric: string;
  /** `series.labels_json`, the subject's key. */
  key: string;
  counter: boolean;
  tiers: [code: number, gaps: number[], values: number[]][];
}

/** What the recording keeps of the leader's series: every subject's points, and some answers the leader gave, each
 * at its key and when it was asked, which the port must give too. */
export interface SeriesPart {
  series: StoredSeries[];
  checks: { key: string; askedAt: number; body: SeriesAnswer }[];
}

export type Draw = 'perSecond' | 'totals';

export interface SeriesAnswer {
  metric: string;
  counter: boolean;
  draw: Draw;
  pointMillis: number;
  fromMillis: number;
  series: { name: string; values: string[]; points: [number, number][] }[];
}

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;
/** `leader::MAX_WINDOW_MINUTES`. */
const MAX_WINDOW_MINUTES = 30 * 24 * 60;
/** `series::Tier`, finest first: its code, bucket and how long it keeps one. */
const TIERS = [
  { code: 0, bucket: 10_000, keeps: HOUR_MS },
  { code: 1, bucket: MINUTE_MS, keeps: DAY_MS },
  { code: 2, bucket: 5 * MINUTE_MS, keeps: (29 * 24 + 1) * HOUR_MS },
  { code: 3, bucket: HOUR_MS, keeps: 30 * DAY_MS },
];
/** `series::CHART_POINTS` and `series::WIDEST_POINT_MS`. */
const CHART_POINTS = 360;
const WIDEST_POINT_MS = 6 * HOUR_MS;
const GROUP_LABEL = 'group';

const floorTo = (at: number, width: number, offset = 0) =>
  Math.floor((at - offset) / width) * width + offset;

/** `series::chart_point_ms`. */
function chartPointMs(windowMs: number): number {
  return (
    [...TIERS.map((t) => t.bucket), WIDEST_POINT_MS].find((p) => windowMs <= CHART_POINTS * p) ??
    WIDEST_POINT_MS
  );
}

/** `query::grid`. */
function grid(draw: Draw, windowMinutes: number, offsetMs: number, endMs: number, nowMs: number) {
  const windowMs = windowMinutes * MINUTE_MS;
  const width =
    draw === 'perSecond'
      ? chartPointMs(windowMs)
      : windowMinutes <= 60
        ? MINUTE_MS
        : windowMinutes <= 24 * 60
          ? HOUR_MS
          : DAY_MS;
  const age = nowMs - (endMs - windowMs);
  const holding = TIERS.find((t) => t.keeps >= age) ?? TIERS[3];
  const pointMs = Math.max(width, holding.bucket);
  const fromMs =
    draw === 'perSecond'
      ? floorTo(endMs - windowMs, pointMs, offsetMs)
      : floorTo(endMs, pointMs, offsetMs) - (Math.trunc(windowMs / pointMs) - 1) * pointMs;
  return { pointMs, fromMs };
}

const labelsOf = (key: string): [string, string][] | undefined => {
  try {
    return JSON.parse(key);
  } catch {
    return undefined;
  }
};
const valueOf = (labels: [string, string][], name: string) => labels.find(([k]) => k === name)?.[1];

/** Answers the leader's series reads from `series`. */
export class SeriesStore {
  private readonly byMetric = new Map<string, StoredSeries[]>();

  /** The newest ten-second bucket stored, which was still filling when the points were read. */
  private readonly newest: number = 0;

  constructor(series: StoredSeries[]) {
    for (const s of series) {
      for (const [code, gaps] of s.tiers) {
        if (code === TIERS[0].code)
          this.newest = Math.max(
            this.newest,
            gaps.reduce((a, b) => a + b, 0),
          );
      }
      const of = this.byMetric.get(s.metric) ?? [];
      of.push(s);
      this.byMetric.set(s.metric, of);
    }
    for (const of of this.byMetric.values())
      of.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  /** `query::metrics`, every group seen. */
  metrics(): { name: string; counter: boolean; labels: Record<string, string[]> }[] {
    return [...this.byMetric.keys()].sort().map((name) => {
      const of = this.byMetric.get(name)!;
      const labels: Record<string, Set<string>> = {};
      for (const s of of) {
        const subject = labelsOf(s.key);
        if (!subject || valueOf(subject, GROUP_LABEL) === undefined) continue;
        for (const [k, v] of subject) (labels[k] ??= new Set()).add(v);
      }
      return {
        name,
        counter: of[0].counter,
        labels: Object.fromEntries(
          Object.keys(labels)
            .sort()
            .map((k) => [k, [...labels[k]].sort()]),
        ),
      };
    });
  }

  /** How far to move every point so the newest bucket stored, which was still filling, is the one filling at
   * `nowMs`: the last bucket a chart draws, the one before it, is then whole. */
  shiftTo(nowMs: number): number {
    return floorTo(nowMs, TIERS[0].bucket) - this.newest;
  }

  /** `query::answer` for the query `q` holds, at `nowMs`, with every point moved `shiftMs` later: the answer, or why
   * the query is not one. Moved, the points' buckets fall on the reader's own hours and days, as a live fleet's do. */
  answer(q: URLSearchParams, nowMs: number, shiftMs = 0): SeriesAnswer | string {
    const filter: [string, string][] = [];
    for (const part of (q.get('filter') ?? '').split(',').filter(Boolean)) {
      const at = part.indexOf(':');
      if (at <= 0) return `filter \`${part}\` is not label:value`;
      filter.push([part.slice(0, at), part.slice(at + 1)]);
    }
    const metric = q.get('metric') ?? '';
    const by = (q.get('by') ?? '').split(',').filter(Boolean);
    const number = (name: string, fallback: number) => {
      const n = Number(q.get(name) ?? fallback);
      return Number.isFinite(n) ? Math.trunc(n) : fallback;
    };
    const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n));
    const windowMinutes = clamp(number('windowMinutes', 60), 1, MAX_WINDOW_MINUTES);
    const offsetMs = -clamp(number('utcOffsetMinutes', 0), -14 * 60, 14 * 60) * MINUTE_MS;
    const subjects = this.byMetric.get(metric) ?? [];
    // A metric nothing has reported yet is a quiet one, not a wrong question.
    const counter = subjects[0]?.counter ?? metric.endsWith('_total');
    const draw: Draw = counter && q.get('draw') === 'totals' ? 'totals' : 'perSecond';
    const endMs = nowMs - clamp(number('agoMinutes', 0), 0, MAX_WINDOW_MINUTES) * MINUTE_MS;
    const { pointMs, fromMs } = grid(draw, windowMinutes, offsetMs, endMs, nowMs);
    // `Tier::within_from`: the coarsest tier whose buckets fit in a point and start on the reader's clock, and every
    // finer one, which still holds the window's newest buckets.
    const within =
      [...TIERS]
        .reverse()
        .find((t) => t.bucket <= pointMs && ((offsetMs % t.bucket) + t.bucket) % t.bucket === 0) ??
      TIERS[0];
    const tiers = new Set(TIERS.filter((t) => t.code <= within.code).map((t) => t.code));
    const merge = counter ? (a: number, b: number) => a + b : Math.max;

    const splits = new Map<string, { values: string[]; points: Map<number, number> }>();
    for (const s of subjects) {
      const labels = labelsOf(s.key);
      if (!labels || valueOf(labels, GROUP_LABEL) === undefined) continue;
      if (!filter.every(([k, v]) => valueOf(labels, k) === v)) continue;
      const values = by.map((label) => valueOf(labels, label));
      if (values.some((v) => v === undefined)) continue;
      // `RegistryStore::points_from`: each bucket on the point it starts in, a point's buckets merged.
      const points: [number, number][] = [];
      for (const [code, gaps, values] of s.tiers) {
        if (!tiers.has(code)) continue;
        let at = 0;
        gaps.forEach((gap, i) => {
          at += gap;
          const bucket = at + shiftMs;
          if (bucket < fromMs || bucket >= endMs) return;
          const point = floorTo(bucket, pointMs, offsetMs);
          if (point >= fromMs) points.push([point, values[i]]);
        });
      }
      if (!points.length) continue;
      points.sort((a, b) => a[0] - b[0]);
      const merged: [number, number][] = [];
      for (const [at, value] of points) {
        const last = merged[merged.length - 1];
        if (last && last[0] === at) last[1] = merge(last[1], value);
        else merged.push([at, value]);
      }
      const key = JSON.stringify(values);
      const into = splits.get(key) ?? {
        values: values as string[],
        points: new Map<number, number>(),
      };
      for (const [at, value] of merged) into.points.set(at, (into.points.get(at) ?? 0) + value);
      splits.set(key, into);
    }
    const per = (value: number) =>
      counter && draw === 'perSecond' ? value / (pointMs / 1000) : value;
    return {
      metric,
      counter,
      draw,
      pointMillis: pointMs,
      fromMillis: fromMs,
      // In `BTreeMap<Vec<String>>` order, as the leader answers.
      series: [...splits.values()]
        .sort((a, b) => compareValues(a.values, b.values))
        .map(({ values, points }) => ({
          name: values.join(', '),
          values,
          points: [...points.entries()]
            .sort((a, b) => a[0] - b[0])
            .map(([at, value]) => [at, per(value)] as [number, number]),
        })),
    };
  }
}

/** `a` against `b` element by element, a shorter list first where one starts the other. */
function compareValues(a: string[], b: string[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
  }
  return a.length - b.length;
}
