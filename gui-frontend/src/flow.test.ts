import { describe, expect, it } from 'vitest';
import type { FlowPointDto, FlowSeriesDto } from './api';
import {
  currentRate,
  denseRates,
  denseSeries,
  deviationFromUsual,
  furthestFromNormal,
  lastClosedBucket,
  sumSeries,
  usualAtThisHour,
} from './flow';

const HOUR = 60 * 60 * 1000;
const pt = (bucketMillis: number, value: number): FlowPointDto => ({ bucketMillis, value });
/** A moment at local hour `h`, `dayOffset` days into January 2024 -- January so no run
 * crosses a DST transition regardless of the machine's timezone. */
const local = (h: number, dayOffset = 0) => new Date(2024, 0, 1 + dayOffset, h).getTime();

describe('denseSeries', () => {
  it('fills every expected bucket, marking the ones nothing landed in as a gap', () => {
    expect(denseSeries([pt(0, 5), pt(20_000, 7)], 10_000, 0, 20_000)).toEqual([5, null, 7]);
  });

  it('is gaps all the way down with no points at all -- never zero-filled', () => {
    expect(denseSeries([], 10_000, 0, 20_000)).toEqual([null, null, null]);
  });

  it('aligns the requested range inward to whole buckets', () => {
    expect(denseSeries([pt(10_000, 4)], 10_000, 4_000, 16_000)).toEqual([4]);
  });
});

describe('denseRates', () => {
  it("divides each slot by its bucket's width in seconds", () => {
    expect(denseRates([pt(0, 300), pt(10_000, 50)], 10_000, 0, 10_000)).toEqual([30, 5]);
  });

  it('a wider bucket holding the same rows is the same rate', () => {
    expect(denseRates([pt(0, 300)], 10_000, 0, 10_000)[0]).toBe(30);
    expect(denseRates([pt(0, 1800)], 60_000, 0, 60_000)[0]).toBe(30);
  });

  it('a gap stays a gap -- there is no rate where there was no bucket', () => {
    expect(denseRates([pt(0, 300)], 10_000, 0, 20_000)).toEqual([30, null, null]);
  });
});

describe('lastClosedBucket', () => {
  it('the bucket before the one now falls in, never the one still filling', () => {
    expect(lastClosedBucket(10_000, 25_000)).toBe(10_000);
  });

  it('an instant exactly on a boundary still steps back a whole bucket', () => {
    expect(lastClosedBucket(10_000, 20_000)).toBe(10_000);
  });
});

describe('sumSeries', () => {
  it('adds every subject that reported into a bucket', () => {
    const series: FlowSeriesDto[] = [
      { group: 'prod-west', subjectId: 'a', points: [pt(0, 5), pt(10_000, 3)] },
      { group: 'prod-west', subjectId: 'b', points: [pt(0, 9)] },
    ];
    expect(sumSeries(series)).toEqual([pt(0, 14), pt(10_000, 3)]);
  });

  it('is empty when no subject reported anything', () => {
    expect(sumSeries([])).toEqual([]);
  });
});

describe('currentRate', () => {
  const bucketMillis = 10_000;

  it('reads the bucket that has just closed', () => {
    // At 25s the 20s bucket is still filling; the 10s one is whole.
    expect(currentRate([pt(0, 1), pt(10_000, 40), pt(20_000, 7)], bucketMillis, 25_000)).toBe(4);
  });

  it('never reads the bucket still filling, whose running total is not a rate', () => {
    // The only point is the open bucket, a third of the way through. Reading it would say
    // 0.7 rows/s of a source running at 2.
    expect(currentRate([pt(20_000, 7)], bucketMillis, 23_000)).toBeNull();
  });

  it('is null, not 0, where the closed bucket is missing -- a gap is not a stop', () => {
    expect(currentRate([pt(0, 40)], bucketMillis, 25_000)).toBeNull();
  });

  it('is null with no points at all', () => {
    expect(currentRate([], bucketMillis, 25_000)).toBeNull();
  });
});

describe('usualAtThisHour', () => {
  const bucketMillis = HOUR;

  it('averages every earlier bucket at the same local hour, excluding the current one', () => {
    const now = local(9, 2);
    const points = [
      pt(local(9, 0), 36_000), // 10 rows/s at 09:00, two days back
      pt(local(9, 1), 72_000), // 20 rows/s at 09:00, one day back
      pt(local(14, 1), 999_999), // a different hour -- excluded
      pt(now, 999_999), // the current bucket itself -- excluded
    ];
    expect(usualAtThisHour(points, bucketMillis, now)).toBe(15);
  });

  it('is null with no history at this hour yet', () => {
    expect(usualAtThisHour([], bucketMillis, local(9, 2))).toBeNull();
  });
});

describe('deviationFromUsual', () => {
  const bucketMillis = HOUR;
  /** Looked at just after 09:00 closed, so the reading is that day's 09:00 bucket. */
  const after = (dayOffset: number) => local(10, dayOffset);

  it('compares the bucket that just closed to the same hour on earlier days', () => {
    const points = [pt(local(9, 0), 36_000), pt(local(9, 1), 72_000)];
    expect(deviationFromUsual(points, bucketMillis, after(1))).toEqual({
      now: 20,
      normal: 10,
      awayPercent: 100,
    });
  });

  it('is null with no baseline to be far from', () => {
    expect(deviationFromUsual([pt(local(9, 0), 36_000)], bucketMillis, after(0))).toBeNull();
  });

  it('is null when the baseline itself is 0 -- a percentage away from zero is not a number', () => {
    const points = [pt(local(9, 0), 0), pt(local(9, 1), 36_000)];
    expect(deviationFromUsual(points, bucketMillis, after(1))).toBeNull();
  });

  it('is null where the closed bucket is missing, rather than ranking a subject at 0', () => {
    const points = [pt(local(9, 0), 36_000), pt(local(9, 1), 36_000)];
    expect(deviationFromUsual(points, bucketMillis, after(2))).toBeNull();
  });
});

describe('furthestFromNormal', () => {
  const bucketMillis = HOUR;

  it('ranks sources and destinations together by the size of the deviation, not its sign', () => {
    const closed = local(9, 1);
    const now = local(10, 1);
    const rowsIn: FlowSeriesDto[] = [
      { group: 'prod-west', subjectId: 'a', points: [pt(local(9, 0), 36_000), pt(closed, 36_000)] }, // steady, 0%
      { group: 'prod-west', subjectId: 'b', points: [pt(local(9, 0), 36_000), pt(closed, 0)] }, // dead, -100%
    ];
    const rowsOut: FlowSeriesDto[] = [
      { group: 'prod-west', subjectId: 'c', points: [pt(local(9, 0), 36_000), pt(closed, 72_000)] }, // spiking, +100%
    ];
    const ranked = furthestFromNormal(rowsIn, rowsOut, bucketMillis, now, 5);
    expect(ranked.map((r) => r.subjectId)).toEqual(['b', 'c', 'a']);
    expect(ranked.find((r) => r.subjectId === 'c')?.kind).toBe('destination');
  });

  it('drops a subject with no baseline rather than ranking it arbitrarily', () => {
    const rowsIn: FlowSeriesDto[] = [
      { group: 'prod-west', subjectId: 'new', points: [pt(local(9, 0), 100)] },
    ];
    expect(furthestFromNormal(rowsIn, [], bucketMillis, local(10, 0), 5)).toEqual([]);
  });

  it('keeps only the top `limit`', () => {
    const rowsIn: FlowSeriesDto[] = Array.from({ length: 3 }, (_, i) => ({
      group: 'prod-west',
      subjectId: `s${i}`,
      points: [pt(local(9, 0), 36_000), pt(local(9, 1), 36_000 * (i + 2))],
    }));
    const ranked = furthestFromNormal(rowsIn, [], bucketMillis, local(10, 1), 2);
    expect(ranked.map((r) => r.subjectId)).toEqual(['s2', 's1']);
  });

  it('ranks one id in two groups as two rows, since an id is unique only within one', () => {
    const now = local(10, 1);
    const points = [pt(local(9, 0), 36_000), pt(local(9, 1), 72_000)];
    const rowsIn: FlowSeriesDto[] = [
      { group: 'prod-west', subjectId: 'orders', points },
      { group: 'prod-east', subjectId: 'orders', points },
    ];
    const ranked = furthestFromNormal(rowsIn, [], bucketMillis, now, 5);
    expect(ranked.map((r) => r.group).sort()).toEqual(['prod-east', 'prod-west']);
    expect(ranked.every((r) => r.subjectId === 'orders')).toBe(true);
  });
});
