import { describe, expect, it } from 'vitest';
import type { MetricSampleDto, ThreadHealth } from './api';
import { NO_LABEL, addPoll, formatFunction, labelOf, sumMetric, toRows } from './metrics';

function counter(metric: string, labels: [string, string][], value: number): MetricSampleDto {
  return { metric, labels, value: { kind: 'counter', value } };
}

function thread(metrics: MetricSampleDto[], threadId = 't0'): ThreadHealth {
  return {
    workerId: 'w1',
    threadId,
    groupId: 'g',
    kind: 'transform',
    partitionsOwned: [],
    lastSeenMillisAgo: 0,
    stopAfterUnreachableSecs: null,
    commit: null,
    metrics,
  };
}

const errored = (pipeline: string, fn: string, kind: string, n: number) =>
  counter(
    'smeltr_rows_errored_total',
    [
      ['pipeline', pipeline],
      ['function', fn],
      ['kind', kind],
    ],
    n,
  );

describe('labelOf', () => {
  it('reads a label by name', () => {
    expect(labelOf(errored('orders', '3', 'ParseFailed', 1), 'pipeline')).toBe('orders');
  });

  it('stands in for a label the sample does not carry', () => {
    expect(labelOf(counter('smeltr_rows_polled_total', [], 1), 'source')).toBe(NO_LABEL);
  });
});

describe('sumMetric', () => {
  it('adds one metric across threads, ignoring labels and other metrics', () => {
    const threads = [
      thread([
        errored('a', '1', 'x', 2),
        counter('smeltr_rows_sent_total', [['destination', 'd']], 9),
      ]),
      thread([errored('b', '2', 'y', 3)], 't1'),
    ];
    expect(sumMetric(threads, 'smeltr_rows_errored_total')).toBe(5);
  });

  it('is zero for a metric nothing reported', () => {
    expect(sumMetric([thread([])], 'smeltr_rows_errored_total')).toBe(0);
  });
});

describe('addPoll', () => {
  const labels = ['pipeline', 'function', 'kind'];

  it('keeps distinct label combinations apart', () => {
    const totals = addPoll(
      {},
      [
        thread([
          errored('orders', '3', 'ParseFailed', 2),
          errored('orders', '4', 'CoercionRefused', 5),
        ]),
      ],
      'smeltr_rows_errored_total',
      labels,
    );
    expect(toRows(totals)).toEqual([
      { values: ['orders', '4', 'CoercionRefused'], total: 5 },
      { values: ['orders', '3', 'ParseFailed'], total: 2 },
    ]);
  });

  it('sums one combination across threads in a single poll', () => {
    const totals = addPoll(
      {},
      [
        thread([errored('orders', '3', 'ParseFailed', 2)]),
        thread([errored('orders', '3', 'ParseFailed', 4)], 't1'),
      ],
      'smeltr_rows_errored_total',
      labels,
    );
    expect(toRows(totals)).toEqual([{ values: ['orders', '3', 'ParseFailed'], total: 6 }]);
  });

  it('accumulates across polls, since a counter arrives as a delta', () => {
    const one = addPoll(
      {},
      [thread([errored('orders', '3', 'x', 2)])],
      'smeltr_rows_errored_total',
      labels,
    );
    const two = addPoll(
      one,
      [thread([errored('orders', '3', 'x', 3)])],
      'smeltr_rows_errored_total',
      labels,
    );
    expect(toRows(two)).toEqual([{ values: ['orders', '3', 'x'], total: 5 }]);
  });

  it('does not mutate the totals it was given', () => {
    const one = addPoll(
      {},
      [thread([errored('orders', '3', 'x', 2)])],
      'smeltr_rows_errored_total',
      labels,
    );
    addPoll(one, [thread([errored('orders', '3', 'x', 3)])], 'smeltr_rows_errored_total', labels);
    expect(toRows(one)).toEqual([{ values: ['orders', '3', 'x'], total: 2 }]);
  });

  it('ignores other metrics and gauges', () => {
    const threads = [
      thread([
        counter('smeltr_rows_sent_total', [['destination', 'd']], 9),
        {
          metric: 'smeltr_rows_errored_total',
          labels: [['pipeline', 'a']],
          value: { kind: 'gauge', value: 7 },
        },
      ]),
    ];
    expect(toRows(addPoll({}, threads, 'smeltr_rows_errored_total', ['pipeline']))).toEqual([]);
  });

  it('groups a single label, for the per-source and per-destination views', () => {
    const threads = [
      thread([
        counter('smeltr_rows_polled_total', [['source', 'orders']], 10),
        counter('smeltr_rows_polled_total', [['source', 'clicks']], 4),
      ]),
    ];
    expect(toRows(addPoll({}, threads, 'smeltr_rows_polled_total', ['source']))).toEqual([
      { values: ['orders'], total: 10 },
      { values: ['clicks'], total: 4 },
    ]);
  });
});

describe('toRows', () => {
  it('orders equal totals by label so rows hold still between polls', () => {
    const totals = addPoll(
      {},
      [thread([counter('m', [['source', 'b']], 1), counter('m', [['source', 'a']], 1)])],
      'm',
      ['source'],
    );
    expect(toRows(totals).map((r) => r.values[0])).toEqual(['a', 'b']);
  });
});

describe('formatFunction', () => {
  it('names the ingest sentinel', () => {
    expect(formatFunction('4294967295')).toBe('ingest');
  });

  it('leaves a step id alone', () => {
    expect(formatFunction('3')).toBe('3');
  });
});
