import { describe, expect, it } from 'vitest';
import type { FlowNormalDto, FlowPointDto, FlowSeriesDto } from './api';
import {
  currentRate,
  denseRates,
  denseSeries,
  furthestFromNormal,
  lastClosedBucket,
  sumSeries,
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

  it('starts at the point the range begins in, which the leader reads whole', () => {
    expect(denseSeries([pt(0, 3), pt(10_000, 4)], 10_000, 4_000, 16_000)).toEqual([3, 4]);
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

describe('furthestFromNormal', () => {
  const normal = (subjectId: string, now: number, group = 'prod-west'): FlowNormalDto => ({
    group,
    subjectId,
    now,
    low: 20,
    high: 40,
  });

  it('ranks by how far past its normal a source is, either way, then by how near an edge', () => {
    const ranked = furthestFromNormal([
      normal('middle', 30),
      normal('dropped', 5),
      normal('near-top', 39),
      normal('burst', 50),
    ]);
    expect(ranked.map((r) => [r.subjectId, r.awayPercent])).toEqual([
      ['dropped', -75],
      ['burst', 25],
      ['near-top', 0],
      ['middle', 0],
    ]);
  });

  it('keeps only the top `limit`', () => {
    const ranked = furthestFromNormal([normal('a', 30), normal('b', 5), normal('c', 60)], 2);
    expect(ranked.map((r) => r.subjectId)).toEqual(['b', 'c']);
  });

  it('ranks one id in two groups as two rows, since an id is unique only within one', () => {
    const ranked = furthestFromNormal([normal('orders', 5), normal('orders', 5, 'prod-east')]);
    expect(ranked.map((r) => r.group).sort()).toEqual(['prod-east', 'prod-west']);
  });
});
