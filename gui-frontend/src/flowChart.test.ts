import { describe, expect, it } from 'vitest';
import {
  axisLabel,
  axisStep,
  axisTop,
  flowArea,
  flowDots,
  flowLine,
  peak,
  slotAt,
  xOf,
  type Half,
} from './flowChart';
import { pathPoints } from './pathPoints';

/** A half 100 wide and 50 deep, reaching 10 at full stretch, drawn upward from y = 60. */
const up: Half = { x: 0, width: 100, baseline: 60, reach: 50, top: 10, up: true };
/** The same box mirrored: same baseline, drawn the other way. */
const down: Half = { ...up, up: false };

describe('peak', () => {
  it('the largest value across every series', () => {
    expect(
      peak([
        [5, 10],
        [3, 20],
      ]),
    ).toBe(20);
  });

  it('ignores gaps rather than counting them as 0', () => {
    expect(peak([[null, 5, null]])).toBe(5);
  });

  it('0 with nothing to measure, so a slow fleet is not floored up to one', () => {
    expect(peak([])).toBe(0);
    expect(peak([[null, null]])).toBe(0);
    expect(peak([[0.3, 0.2]])).toBe(0.3);
  });
});

describe('axisStep', () => {
  it('a max already on a step keeps it rather than climbing to the next', () => {
    expect(axisStep(20)).toBe(10);
    expect(axisStep(100)).toBe(50);
  });

  it('reaches every max, and no max is drawn in less than two thirds of the depth', () => {
    for (let max = 1; max < 10_000; max += 7) {
      // Both sides. A top under the max puts the peak bucket outside the box, which
      // `yOf` does not clamp, and a ratio bound alone reads that as a pass.
      expect(axisTop(max)).toBeGreaterThanOrEqual(max);
      expect(max / axisTop(max)).toBeGreaterThan(2 / 3);
    }
  });

  it('reaches a max below one too, where the rungs are a power of ten apart', () => {
    for (let n = 1; n < 2000; n += 3) {
      const max = n / 3600; // one row an hour up: the smallest rate the leader reports
      expect(axisTop(max)).toBeGreaterThanOrEqual(max);
    }
  });

  it('a rung below one is the number it looks like, not what the multiply left', () => {
    expect(axisStep(0.0044)).toBe(0.0025);
  });

  it('never 0: a fleet at rest still gets an axis to draw against', () => {
    expect(axisStep(0)).toBe(1);
  });
});

describe('axisLabel', () => {
  it('whole rows/s where the step is whole', () => {
    expect(axisLabel(40, 20)).toBe('40');
  });

  it('as many places as the step needs, and no more', () => {
    expect(axisLabel(0.2, 0.2)).toBe('0.2');
    expect(axisLabel(0.1, 0.05)).toBe('0.10');
  });

  it('thousands and up carry a unit, so the label fits the gutter', () => {
    expect(axisLabel(5000, 2500)).toBe('5.0k');
    expect(axisLabel(2500, 2500)).toBe('2.5k');
    expect(axisLabel(2000, 1000)).toBe('2k');
    expect(axisLabel(4_000_000, 2_000_000)).toBe('4M');
  });

  it('0 is bare: "0k" names a unit for a quantity that has none', () => {
    expect(axisLabel(0, 2500)).toBe('0');
    expect(axisLabel(0, 0.05)).toBe('0');
  });

  it('a step below a thousand stays unscaled even where the value is not', () => {
    expect(axisLabel(1000, 500)).toBe('1000');
  });
});

describe('slotAt', () => {
  it('the first and last points sit on the edges of the plot', () => {
    expect(slotAt(0, 5, up)).toBe(0);
    expect(slotAt(100, 5, up)).toBe(4);
  });

  it('changes as the pointer passes a mark, not half a step before it', () => {
    // Five points across 100 is a step of 25, so the hand-over is at 12.5.
    expect(slotAt(12, 5, up)).toBe(0);
    expect(slotAt(13, 5, up)).toBe(1);
  });

  it("measures from the half's own left edge, not from zero", () => {
    expect(slotAt(6, 3, { ...up, x: 6 })).toBe(0);
    expect(slotAt(56, 3, { ...up, x: 6 })).toBe(1);
  });

  it('a pointer outside the plot reads the nearest end rather than an absent bucket', () => {
    expect(slotAt(-50, 5, up)).toBe(0);
    expect(slotAt(999, 5, up)).toBe(4);
  });

  it('is null where there is no step to divide by, rather than NaN', () => {
    expect(slotAt(0, 1, up)).toBeNull();
    expect(slotAt(0, 0, up)).toBeNull();
    expect(slotAt(0, 5, { ...up, width: 0 })).toBeNull();
  });

  it('lands back on every point the chart drew -- the readout names the mark under it', () => {
    for (const count of [2, 3, 7, 60, 361]) {
      for (let i = 0; i < count; i += 1) {
        expect(slotAt(xOf(i, count, up), count, up)).toBe(i);
      }
    }
  });
});

describe('flowLine', () => {
  it('fewer than two points makes no line', () => {
    expect(flowLine([], up)).toBe('');
    expect(flowLine([5], up)).toBe('');
  });

  it('leaves a reading with no neighbor to flowDots', () => {
    expect(flowLine([10, null, 10], up)).toBe('');
  });
});

describe('flowDots', () => {
  it('is empty where every reading has a neighbor', () => {
    expect(flowDots([10, 10, null, 10, 10], up)).toBe('');
  });
});

describe('flowArea', () => {
  it('starts and ends each run on the baseline, so the fill closes along it', () => {
    const [run] = pathPoints(flowArea([0, 10], up));
    expect(run[0]).toEqual([0, 60]);
    expect(run).toContainEqual([100, 10]);
    expect(run.at(-1)).toEqual([100, 60]);
  });

  it('leaves a lone reading to the dot: one point spans no width, so there is no area', () => {
    expect(flowArea([10, null, 10], up)).toBe('');
  });

  it('a gap is a hole in the fill, not a span drawn across it', () => {
    const fill = pathPoints(flowArea([10, 10, null, 10, 10], up));
    expect(fill).toHaveLength(2);
    expect(fill[0].at(-1)).toEqual([25, 60]);
    expect(fill[1][0]).toEqual([75, 60]);
  });

  it('fills downward from the same baseline when the half points down', () => {
    const [run] = pathPoints(flowArea([0, 10], down));
    expect(run[0]).toEqual([0, 60]);
    expect(run).toContainEqual([100, 110]);
    expect(run.at(-1)).toEqual([100, 60]);
  });
});
