import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import { SERIES_PART, partFile } from './parts';
import { SeriesStore, type SeriesAnswer, type SeriesPart } from './series';

const part: SeriesPart = JSON.parse(
  fs.readFileSync(new URL(`./recorded/${await partFile(SERIES_PART)}`, import.meta.url), 'utf8'),
);
const store = new SeriesStore(part.series);

/** The newest point of each series, which was still filling when the leader answered and filled on before the
 * recorder read the points, is left out. */
const settled = (answer: SeriesAnswer) =>
  answer.series.map(({ name, points }) => ({
    name,
    points: points.slice(0, -1).map(([at, value]) => [at, Number(value.toPrecision(9))]),
  }));

describe("the demo's series answers", () => {
  it('holds an answer the leader gave for each shape the GUI asks', () => {
    expect(part.checks.filter((c) => c.key.startsWith('/api/series?')).length).toBeGreaterThan(3);
  });

  for (const { key, askedAt, body } of part.checks) {
    it(`answers ${key} as the leader did`, () => {
      const url = new URL(key, 'http://demo');
      if (url.pathname === '/api/series/metrics') {
        expect(store.metrics()).toEqual((body as unknown as { metrics: unknown }).metrics);
        return;
      }
      const ours = store.answer(url.searchParams, askedAt);
      if (typeof ours === 'string') throw new Error(ours);
      const { metric, counter, draw, pointMillis, fromMillis } = body;
      expect({ ...ours, series: undefined }).toEqual({
        metric,
        counter,
        draw,
        pointMillis,
        fromMillis,
        series: undefined,
      });
      expect(settled(ours)).toEqual(settled(body));
    });
  }

  it('says why a filter is not one', () => {
    expect(store.answer(new URLSearchParams({ metric: 'x', filter: 'source' }), 0)).toBe(
      'filter `source` is not label:value',
    );
  });
});
