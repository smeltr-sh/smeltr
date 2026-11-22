import { describe, expect, it } from 'vitest';
import type { FlowNormalDto, SeriesAnswerDto } from './api';
import { denseAnswer, furthestFromNormal, lastClosedBucket } from './flow';

describe('denseAnswer', () => {
  const answer = (...series: [number, number][][]): SeriesAnswerDto => ({
    metric: 'm',
    counter: true,
    draw: 'perSecond',
    pointMillis: 10_000,
    fromMillis: 0,
    series: series.map((points, i) => ({ name: String(i), values: [String(i)], points })),
  });

  it('adds every series on each point through the last, a point nothing landed in a gap', () => {
    expect(
      denseAnswer(
        answer(
          [
            [0, 5],
            [20_000, 1],
          ],
          [[0, 9]],
        ),
        30_000,
      ),
    ).toEqual([14, null, 1, null]);
  });

  it('is gaps all the way down with no series at all -- never zero-filled', () => {
    expect(denseAnswer(answer(), 20_000)).toEqual([null, null, null]);
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
