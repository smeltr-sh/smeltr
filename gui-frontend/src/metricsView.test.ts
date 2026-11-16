import { describe, expect, it } from 'vitest';
import {
  DEFAULT_VIEW,
  described,
  fold,
  onGrid,
  toHash,
  unitOf,
  viewFromParams,
} from './metricsView';

describe('metricsView', () => {
  it('rides the hash and back', () => {
    const view = {
      ...DEFAULT_VIEW,
      metric: 'smeltr_bytes_polled_total',
      filter: [['group', 'east']] as [string, string][],
      by: 'source',
      windowMinutes: 1440,
      draw: 'totals' as const,
      top: 3 as const,
    };
    const hash = toHash(view);
    expect(viewFromParams(new URLSearchParams(hash.split('?')[1]))).toEqual(view);
  });

  it('reads a bare hash as the default view, and drops what it cannot read', () => {
    expect(viewFromParams(new URLSearchParams(''))).toEqual(DEFAULT_VIEW);
    expect(viewFromParams(new URLSearchParams('top=4&window=-1&filter=bad')).top).toBe(
      DEFAULT_VIEW.top,
    );
  });

  it('lays sparse points on the grid, busiest first, and folds the rest into Other', () => {
    const answer = {
      metric: 'm',
      counter: true,
      draw: 'perSecond' as const,
      pointMillis: 10,
      fromMillis: 100,
      series: ['a', 'b', 'c', 'd'].map((name, i) => ({
        name,
        points: [[100 + 10 * i, i + 1] as [number, number]],
      })),
    };
    const all = onGrid(answer, 4);
    expect(all.map((s) => s.name)).toEqual(['d', 'c', 'b', 'a']);
    expect(all[0].points).toEqual([null, null, null, 4]);
    const shown = fold(all, 3);
    expect(shown.map((s) => [s.name, s.slot])).toEqual([
      ['d', 0],
      ['c', 1],
      ['b', 2],
      ['Other', null],
    ]);
    expect(shown[3].points).toEqual([1, null, null, null]);
  });

  it('names what a metric counts from the reference, and its unit from its name', () => {
    expect(described('smeltr_rows_polled_total').group).toBe('Sources');
    expect(described('smeltr_rows_dropped_total', ['pipeline', 'function']).group).toBe(
      'Pipeline steps',
    );
    expect(unitOf('smeltr_bytes_polled_total')).toBe('bytes');
    expect(unitOf('smeltr_tick_poll_seconds_total')).toBe('seconds');
    expect(unitOf('smeltr_commits_total')).toBe('');
  });
});
