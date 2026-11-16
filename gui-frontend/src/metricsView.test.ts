import { describe, expect, it } from 'vitest';
import {
  DEFAULT_VIEW,
  alongside,
  described,
  filtersFor,
  fitsBeside,
  fold,
  laidOn,
  onGrid,
  toHash,
  toneOf,
  unitOf,
  viewFromParams,
} from './metricsView';

describe('metricsView', () => {
  it('rides the hash and back', () => {
    const view = {
      ...DEFAULT_VIEW,
      metric: 'smeltr_bytes_polled_total',
      more: ['smeltr_rows_polled_total'],
      filter: [['group', 'east']] as [string, string][],
      by: 'source',
      windowMinutes: 1440,
      draw: 'totals' as const,
      top: 30,
    };
    const hash = toHash(view);
    expect(viewFromParams(new URLSearchParams(hash.split('?')[1]))).toEqual(view);
  });

  it('narrows each metric only by the filters whose label it carries, and by its group', () => {
    const filter: [string, string][] = [
      ['group', 'east'],
      ['source', 'orders'],
    ];
    expect(filtersFor(filter, { destination: ['splunk'] })).toEqual([['group', 'east']]);
    expect(filtersFor(filter, { source: ['orders'] })).toEqual(filter);
    expect(filtersFor(filter, undefined)).toEqual(filter);
  });

  it('draws beside others only a metric not among them, of their kind, in at most two units', () => {
    const rows = { name: 'smeltr_rows_polled_total', counter: true };
    const bytes = { name: 'smeltr_bytes_polled_total', counter: true };
    const seconds = { name: 'smeltr_tick_poll_seconds_total', counter: true };
    const gauge = { name: 'smeltr_process_memory_bytes', counter: false };
    expect(fitsBeside(bytes, [rows])).toBe(true);
    expect(fitsBeside(rows, [rows])).toBe(false);
    expect(fitsBeside(gauge, [rows])).toBe(false);
    expect(fitsBeside(seconds, [rows, bytes])).toBe(false);
    expect(fitsBeside(seconds, [rows])).toBe(true);
  });

  it('reads a repeated metric in a link once', () => {
    const v = viewFromParams(new URLSearchParams('metric=a&metric=b&metric=a&metric=b'));
    expect([v.metric, ...v.more]).toEqual(['a', 'b']);
  });

  it('reads a bare hash as the default view, and drops what it cannot read', () => {
    expect(viewFromParams(new URLSearchParams(''))).toEqual(DEFAULT_VIEW);
    expect(viewFromParams(new URLSearchParams('top=0&windowMinutes=-1&filter=bad')).top).toBe(
      DEFAULT_VIEW.top,
    );
  });

  it('brings the palette round again past its last color, and gives Other its own', () => {
    expect([0, 6, 7, 29, null].map(toneOf)).toEqual([
      'series-0',
      'series-6',
      'series-0',
      'series-1',
      'series-other',
    ]);
  });

  it('lays the week before beside each series, and adds up Other from what is not drawn on its own', () => {
    const drawn = (name: string, slot: number | null, points: (number | null)[]) => ({
      name,
      slot,
      points,
      total: 0,
    });
    const shown = [drawn('a', 0, [1, 1]), drawn('Other', null, [1, 1])];
    const before = [drawn('a', 0, [5, null]), drawn('b', 1, [2, null]), drawn('c', 2, [3, 4])];
    expect(alongside(shown, before)).toEqual([
      [5, null],
      [5, 4],
    ]);
  });

  it('lays a coarser week before on the grid by time, a bar sharing out its total by width', () => {
    const answer = {
      metric: 'm',
      counter: true,
      draw: 'totals' as const,
      pointMillis: 60_000,
      fromMillis: 1_000_000_000,
      series: [],
    };
    const week = 7 * 24 * 3_600_000;
    const before = {
      ...answer,
      pointMillis: 300_000,
      fromMillis: answer.fromMillis - week,
      series: [{ name: 'a', points: [[answer.fromMillis - week, 50]] as [number, number][] }],
    };
    expect(laidOn(before, answer, 6, week)[0].points).toEqual([10, 10, 10, 10, 10, null]);
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
