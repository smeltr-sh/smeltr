import { describe, expect, it } from 'vitest';
import { pathPoints } from './pathPoints';
import { bandPath, bandRect, maxOf, slotAt, sparklinePath, valueLabel, xAt } from './sparkline';

describe('maxOf', () => {
  it('the largest value across every series', () => {
    expect(
      maxOf([
        [5, 10],
        [3, 20],
      ]),
    ).toBe(20);
  });

  it('never 0, for an all-zero series or no series at all', () => {
    expect(maxOf([[0, 0, 0]])).toBe(1);
    expect(maxOf([])).toBe(1);
  });

  it('ignores gaps rather than counting them as 0', () => {
    expect(maxOf([[null, 5, null]])).toBe(5);
    expect(maxOf([[null, null]])).toBe(1);
  });
});

describe('sparklinePath', () => {
  it('fewer than two points makes no line', () => {
    expect(sparklinePath([], 100, 50, 10)).toBe('');
    expect(sparklinePath([5], 100, 50, 10)).toBe('');
  });

  it('plots 0 at the bottom (y = height) and the max at the top (y = 0)', () => {
    expect(pathPoints(sparklinePath([0, 10], 100, 50, 10))).toEqual([
      [
        [0, 50],
        [100, 0],
      ],
    ]);
  });

  it('spaces points evenly across the full width', () => {
    const [line] = pathPoints(sparklinePath([0, 0, 0], 100, 50, 1));
    expect(line.map(([x]) => x)).toEqual([0, 50, 100]);
  });

  it('breaks the path at a gap instead of drawing through it', () => {
    // A stroke draws Z as a line back to the start, which pathPoints does not read.
    expect(sparklinePath([0, 10, null, 5, 15], 100, 50, 15)).not.toContain('Z');
    expect(pathPoints(sparklinePath([0, 10, null, 5, 15], 100, 50, 15))).toEqual([
      [
        [0, 50],
        [25, 16.7],
      ],
      [
        [75, 33.3],
        [100, 0],
      ],
    ]);
  });

  it('drops a single point stranded between two gaps -- nothing to connect it to', () => {
    expect(sparklinePath([1, null, 3], 100, 50, 3)).toBe('');
  });

  it('a leading or trailing gap only trims that end of the line', () => {
    expect(pathPoints(sparklinePath([null, 0, 10], 100, 50, 10))).toEqual([
      [
        [50, 50],
        [100, 0],
      ],
    ]);
    expect(pathPoints(sparklinePath([0, 10, null], 100, 50, 10))).toEqual([
      [
        [0, 50],
        [50, 0],
      ],
    ]);
  });
});

describe('bandRect', () => {
  it("spans low to high on the lines' scale", () => {
    expect(bandRect(25, 75, 80, 100)).toEqual({ y: 20, height: 40 });
  });

  it('a band at a single value has no height', () => {
    expect(bandRect(50, 50, 80, 100).height).toBe(0);
  });
});

describe('valueLabel', () => {
  it('whole from ten up, one decimal below, so a quiet rate is not zero', () => {
    expect(valueLabel(43.6)).toBe('44');
    expect(valueLabel(0.44)).toBe('0.4');
    expect(valueLabel(0)).toBe('0');
  });

  it('an em dash for a gap or nothing', () => {
    expect(valueLabel(null)).toBe('—');
    expect(valueLabel(undefined)).toBe('—');
  });
});

describe('slotAt', () => {
  it('the nearest slot, clamped to the ends', () => {
    expect(slotAt(0, 5)).toBe(0);
    expect(slotAt(0.49, 5)).toBe(2);
    expect(slotAt(1, 5)).toBe(4);
    expect(slotAt(-0.2, 5)).toBe(0);
    expect(slotAt(1.3, 5)).toBe(4);
  });
});

describe('xAt', () => {
  it('a moment on the line between the first and last point, clamped', () => {
    expect(xAt(1_500, [1_000, 2_000, 3_000], 100)).toBe(25);
    expect(xAt(0, [1_000, 2_000, 3_000], 100)).toBe(0);
    expect(xAt(9_000, [1_000, 2_000, 3_000], 100)).toBe(100);
  });
});

describe('bandPath', () => {
  it('one closed area per run, broken where there is no band', () => {
    const b = { low: 0, high: 10 };
    const d = bandPath([b, b, null, b, b], 40, 10, 10);
    expect(d.match(/Z/g)).toHaveLength(2);
    expect(d.startsWith('M0.0,0.0 L10.0,0.0 L10.0,10.0 L0.0,10.0 Z')).toBe(true);
  });

  it('nothing for a lone band, which has no width to fill', () => {
    expect(bandPath([{ low: 0, high: 1 }, null], 40, 10, 10)).toBe('');
  });
});
