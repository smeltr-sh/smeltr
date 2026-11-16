// What the Metrics page asks (`leader::query`), as it rides in the hash: a view worth showing someone is a link, and
// the agent links one.
import catalog from '../../website/src/lib/metrics.json';
import type { SeriesAnswerDto, SeriesDraw } from './api';

export type MetricsView = {
  metric?: string;
  filter: [string, string][];
  by: string;
  windowMinutes: number;
  draw: SeriesDraw;
  /** How many of the busiest series are drawn on their own; the rest add up to Other. */
  top: number;
};

export const DEFAULT_VIEW: MetricsView = {
  metric: 'smeltr_rows_polled_total',
  filter: [],
  by: '',
  windowMinutes: 60,
  draw: 'perSecond',
  top: 5,
};

export function viewFromParams(params: URLSearchParams): MetricsView {
  const minutes = Number(params.get('windowMinutes'));
  const top = Number(params.get('top'));
  return {
    metric: params.get('metric') || DEFAULT_VIEW.metric,
    filter: (params.get('filter') ?? '')
      .split(',')
      .map((pair) => pair.split(':'))
      .filter((pair): pair is [string, string] => pair.length === 2 && pair[0] !== ''),
    by: params.get('by') ?? '',
    windowMinutes: Number.isFinite(minutes) && minutes > 0 ? minutes : DEFAULT_VIEW.windowMinutes,
    draw: params.get('draw') === 'totals' ? 'totals' : 'perSecond',
    top: Number.isInteger(top) && top > 0 ? top : DEFAULT_VIEW.top,
  };
}

function params(v: MetricsView): URLSearchParams {
  const q = new URLSearchParams();
  if (v.metric) q.set('metric', v.metric);
  if (v.filter.length) q.set('filter', v.filter.map(([k, value]) => `${k}:${value}`).join(','));
  if (v.by) q.set('by', v.by);
  q.set('windowMinutes', String(v.windowMinutes));
  if (v.draw === 'totals') q.set('draw', 'totals');
  if (v.top !== DEFAULT_VIEW.top) q.set('top', String(v.top));
  return q;
}

export function toHash(v: MetricsView): string {
  return `#/status/metrics?${params(v)}`;
}

/** The query `GET /api/series` takes for `v`, with the reader's clock, so hours and days start where theirs do. */
export function seriesQuery(v: MetricsView, utcOffsetMinutes: number): string {
  const q = new URLSearchParams({
    metric: v.metric ?? '',
    filter: v.filter.map(([k, value]) => `${k}:${value}`).join(','),
    by: v.by,
    windowMinutes: String(v.windowMinutes),
    draw: v.draw,
    utcOffsetMinutes: String(utcOffsetMinutes),
  });
  return q.toString();
}

/** What the website's metrics reference says of `name`: its group and what it counts. A name listed twice, with
 * different labels, takes the entry whose labels `labels` holds. */
export function described(name: string, labels: string[] = []): { group: string; what: string } {
  const entries = catalog.filter((m) => m.name === name);
  const fits = entries.find((m) => m.labels.every((l) => labels.includes(l))) ?? entries[0];
  return fits ? { group: fits.group, what: fits.what } : { group: 'Other', what: '' };
}

/** What `name` counts in, by what its name says. */
export function unitOf(name: string): 'bytes' | 'seconds' | 'rows' | '' {
  if (name.includes('bytes')) return 'bytes';
  if (name.includes('seconds')) return 'seconds';
  if (name.includes('rows')) return 'rows';
  return '';
}

/** The categorical palette's colors, `--series-0` on (`App.svelte`). */
const PALETTE = 7;

/** The color class of the series at `slot`: past the palette the colors come round again, and the legend and the
 * table, which name each series, tell two of one color apart. */
export function toneOf(slot: number | null): string {
  return slot === null ? 'series-other' : `series-${slot % PALETTE}`;
}

export type Drawn = {
  name: string;
  /** Its color, by rank; `null` for the series folded into Other. */
  slot: number | null;
  /** One value per point of the window, `null` where nothing landed. */
  points: (number | null)[];
  total: number;
};

/** `answer`'s series laid on one grid of `count` points, every one, busiest first. */
export function onGrid(answer: SeriesAnswerDto, count: number): Drawn[] {
  return answer.series
    .map((s) => {
      const points: (number | null)[] = Array(count).fill(null);
      for (const [at, value] of s.points) {
        const i = Math.round((at - answer.fromMillis) / answer.pointMillis);
        if (i >= 0 && i < count) points[i] = value;
      }
      return {
        name: s.name,
        slot: null,
        points,
        total: points.reduce<number>((a, v) => a + (v ?? 0), 0),
      };
    })
    .sort((a, b) => b.total - a.total || a.name.localeCompare(b.name))
    .map((s, i) => ({ ...s, slot: i }));
}

/** The `top` busiest of `all` and, when there are more, one Other that adds the rest up. */
export function fold(all: Drawn[], top: number): Drawn[] {
  if (all.length <= top) return all;
  const rest = all.slice(top);
  const count = rest[0].points.length;
  const points = Array.from({ length: count }, (_, i) =>
    rest.some((s) => s.points[i] !== null)
      ? rest.reduce((a, s) => a + (s.points[i] ?? 0), 0)
      : null,
  );
  return [
    ...all.slice(0, top),
    { name: 'Other', slot: null, points, total: rest.reduce((a, s) => a + s.total, 0) },
  ];
}
