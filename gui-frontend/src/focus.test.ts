import { describe, expect, it } from 'vitest';
import type { QueueFillDto } from './api';
import {
  expectedSlots,
  millis,
  micros,
  percent,
  queueShare,
  rows,
  shareOfTime,
  perSecond,
  slotTimes,
  spreadSlots,
  totalMicros,
  workerPage,
} from './focus';

describe('percent', () => {
  it('reads at the precision that still says something', () => {
    expect(percent(0.164)).toBe('16.4%');
    expect(percent(0)).toBe('0%');
    expect(percent(0.0000001)).toBe('<0.1%');
    expect(percent(null)).toBe('—');
  });
});

describe('rows', () => {
  it('separates thousands and never shows a decimal', () => {
    expect(rows(1240000)).toBe((1240000).toLocaleString());
    expect(rows(1.6)).toBe('2');
  });
});

describe('totalMicros / shareOfTime', () => {
  const steps = [{ micros: 300 }, { micros: 100 }];

  it("is a step's part of what the pipeline spent", () => {
    expect(shareOfTime(steps[0].micros, totalMicros(steps))).toBe(0.75);
  });

  it('has no answer when no time was recorded, rather than making every step the whole of it', () => {
    const idle = [{ micros: 0 }, { micros: 0 }];
    expect(shareOfTime(idle[0].micros, totalMicros(idle))).toBeNull();
  });
});

describe('micros', () => {
  it('changes unit where the digits stop mattering', () => {
    expect(micros(4.25)).toBe('4.3 µs');
    expect(micros(420)).toBe('420 µs');
    expect(micros(2100)).toBe('2.1 ms');
    expect(micros(3_500_000)).toBe('3.5 s');
  });
});

const MiB = 1024 * 1024;

function queue(worker: string, bytes: number, budget = 16 * MiB): QueueFillDto {
  return { worker, bytes, budget, stale: false };
}

describe('queueShare', () => {
  it('is what is held over what may be held', () => {
    expect(queueShare(queue('w1', 8 * MiB))).toBe(0.5);
    expect(queueShare(queue('w1', 0))).toBe(0);
  });

  /** A budget of zero is a destination that stated nothing, not one that may hold nothing. A
   * division by it is an infinity, and there is no bar to draw from an infinity. */
  it('is zero rather than infinite where no budget came back', () => {
    expect(queueShare(queue('w1', 5 * MiB, 0))).toBe(0);
  });
});

describe('millis', () => {
  it('keeps a decimal only under ten', () => {
    expect(millis(2.34)).toBe('2.3 ms');
    expect(millis(137.4)).toBe('137 ms');
  });
});

describe('spreads', () => {
  const bucket = 10_000;
  const now = 1_000_000_000_123; // the filling bucket starts at 1_000_000_000_000
  const at = (agoBuckets: number, value: number) => ({
    bucketMillis: 1_000_000_000_000 - agoBuckets * bucket,
    value,
  });

  it('lays the buckets on the window grid, oldest first, gaps kept, the filling bucket left out', () => {
    const spread = { min: 1, max: 3, mean: 2, buckets: [at(6, 1), at(3, 3), at(1, 2)] };
    const slots = spreadSlots(spread, bucket, 60_000, now);
    expect(slots).toEqual([1, null, null, 3, null, 2]);
  });

  it('drops a point older than the window rather than drawing it at the edge', () => {
    const spread = { min: 9, max: 9, mean: 9, buckets: [at(7, 9)] };
    expect(spreadSlots(spread, bucket, 60_000, now)).toEqual([null, null, null, null, null, null]);
  });
});

describe('workerPage', () => {
  const queues = Array.from({ length: 19 }, (_, i) => `w-${i}`);

  it('cuts a page and counts it from 1', () => {
    const p = workerPage(queues, 1, 8);
    expect(p.rows).toEqual(queues.slice(8, 16));
    expect([p.start, p.end, p.total, p.pages]).toEqual([9, 16, 19, 3]);
  });

  it('clamps a page past the end to the last one', () => {
    const p = workerPage(queues, 9, 8);
    expect(p.page).toBe(2);
    expect(p.rows).toEqual(queues.slice(16));
  });

  it('is one empty page for no workers', () => {
    expect(workerPage([], 0, 8)).toEqual({
      rows: [],
      page: 0,
      pages: 1,
      start: 0,
      end: 0,
      total: 0,
    });
  });
});

describe('slotTimes', () => {
  it("when each of spreadSlots' slots began, ending at the bucket before now's", () => {
    expect(slotTimes(10_000, 30_000, 45_000)).toEqual([10_000, 20_000, 30_000]);
  });
});

describe('perSecond', () => {
  it('whole from ten up, one decimal below', () => {
    expect(perSecond(1234.4)).toBe('1,234');
    expect(perSecond(0.44)).toBe('0.4');
    expect(perSecond(0)).toBe('0');
  });
});

describe('expectedSlots', () => {
  it('gives each slot the span that holds it, and none where no span does', () => {
    const points = [
      { fromMillis: 0, toMillis: 300, low: 1, high: 2 },
      { fromMillis: 600, toMillis: 900, low: 3, high: 4 },
    ];
    expect(expectedSlots(points, [0, 290, 300, 600, 900])).toEqual([
      { low: 1, high: 2 },
      { low: 1, high: 2 },
      null,
      { low: 3, high: 4 },
      null,
    ]);
  });
});
