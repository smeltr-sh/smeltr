// Vendored from `gui-frontend/src/flowChart.ts` for the fleet status
// demo; keep it in step: the website builds standalone and does not read
// gui-frontend at build time.
//
// The geometry behind `FlowChart.svelte`: two series drawn from one baseline, rows in
// above it and rows out below. The halves share a scale, so one reaching further than
// the other means it moved more rows -- that comparison is the reason for drawing them
// this way rather than laying one line over the other, where the two hide each other
// whenever they agree.
//
// A `null` point is a gap, not a zero (`flow.ts`). The line breaks there and the fill
// stops, so an absent bucket never puts a point on the baseline, which would read as a
// reading of nothing moving.

import { runs } from './sparkline';

/** One half of the chart: the box a series is drawn in, and which way it grows from the
 * baseline the two halves share. */
export type Half = {
  /** The plot's left edge. */
  x: number;
  /** The plot's width. Points are spaced evenly across all of it. */
  width: number;
  /** The y both halves grow from. */
  baseline: number;
  /** How far this half reaches when a point is worth `top`. */
  reach: number;
  /** The value at full reach. Never 0. */
  top: number;
  /** Up from the baseline, or down. */
  up: boolean;
};

/** How many grid steps a half is cut into: the baseline, one line between, and the top.
 * Three labels a half is as many as fits beside a chart this tall. */
const DIVISIONS = 2;

/** The largest value across every series, or 0 where there is none.
 *
 * Not `maxOf`, which floors at 1 because a sparkline divides by its answer directly. Here
 * `axisStep` is what refuses to return 0, so the floor is not needed and does harm: a
 * fleet moving a third of a row a second would be drawn against a scale running to one,
 * in the bottom third of the space it was given. */
export function peak(series: (number | null)[][]): number {
  return Math.max(0, ...series.flatMap((s) => s.filter((v): v is number => v !== null)));
}

/** The rungs a grid step may take, each a power of ten times one of these. A coarser
 * ladder -- 1, 2, 5, the usual one -- has a step of 5000 as the first that reaches 2469,
 * which tops the axis at 10000 and draws a fleet moving 4937 rows/s inside the bottom
 * half of its own chart. No gap here is wider than half again, so no half wastes more
 * than a third of its depth. Every rung times one and times two is still a number a
 * reader can hold, which is what the two labels above the baseline say. */
const RUNGS = [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8];

/** The smallest rung that reaches `value`, rounded to the two digits a rung has: a rung
 * times a power of ten is not exact below one, and `axisLabel` counts its places. */
function niceStep(value: number): number {
  const power = Math.pow(10, Math.floor(Math.log10(value)));
  for (const step of RUNGS) {
    if (value <= step * power) return Number((step * power).toPrecision(2));
  }
  // Above the last rung: the next power of ten, rounded the same way.
  return Number((10 * power).toPrecision(2));
}

/** The grid step a half takes to cover `max`, so the labels are numbers a reader can
 * hold rather than whatever the tallest bucket happened to be. 1 for a fleet at rest: an
 * axis running to 0 has nothing to divide. */
export function axisStep(max: number): number {
  return max > 0 ? niceStep(max / DIVISIONS) : 1;
}

/** The value at a half's full reach: `axisStep` taken the whole way up. At or above
 * `max`, so a point drawn against it stays inside the box. */
export function axisTop(max: number): number {
  return axisStep(max) * DIVISIONS;
}

/** Every grid value from the baseline to the top, the baseline's own 0 included. */
export function axisValues(max: number): number[] {
  const step = axisStep(max);
  return Array.from({ length: DIVISIONS + 1 }, (_, i) => step * i);
}

/** Thousands and up, so a busy fleet's labels stay inside the gutter rather than running
 * off the left of the chart. The largest unit leaving digits in front of the point, as
 * `bytes.ts` writes a size. */
const SCALES: [number, string][] = [
  [1_000_000_000, 'G'],
  [1_000_000, 'M'],
  [1_000, 'k'],
];

/** How many places `step` needs. A rung is two significant digits at most, so rounding
 * there first drops the noise a power-of-ten multiply leaves behind. */
function places(step: number): number {
  const text = Number(step.toPrecision(2)).toString();
  const point = text.indexOf('.');
  return point === -1 ? 0 : text.length - point - 1;
}

/** A grid value written out: as many places as `step` needs and no more, so every label
 * on an axis carries the same ones and they line up. `0` is bare -- "0k" names a unit
 * for a quantity that has none. */
export function axisLabel(value: number, step: number): string {
  if (value === 0) return '0';
  for (const [scale, suffix] of SCALES) {
    if (step >= scale) return `${(value / scale).toFixed(places(step / scale))}${suffix}`;
  }
  return value.toFixed(places(step));
}

/** The y `value` sits at in `half`. Not clamped: a point above `top` leaves the box the
 * same way `sparklinePath` lets one leave its own, rather than being quietly flattened
 * against the edge. `axisTop` is what keeps that from happening. Exported because the hover
 * mark stands on a drawn reading and has to sit exactly where the line put it. */
export function yOf(value: number, half: Half): number {
  const span = (value / half.top) * half.reach;
  return half.up ? half.baseline - span : half.baseline + span;
}

/** The x of the `index`th of `count` points, the first on `half`'s left edge and the last
 * on its right. Exported because the hover rule is drawn at a slot too, and it has to
 * stand on the same x the line was drawn at rather than near it. */
export function xOf(index: number, count: number, half: Half): number {
  return half.x + index * (half.width / (count - 1));
}

/** Which of `count` evenly spaced points lies nearest `x`: the inverse of `xOf`, and what
 * a pointer over the plot is asking. `null` below two points or across no width, where
 * `xOf` has no step to divide by and the chart draws nothing to point at.
 *
 * Nearest, not the one to the left. A bucket owns the air on both sides of it, so the
 * reading changes as the pointer passes a mark rather than half a step before it. */
export function slotAt(x: number, count: number, half: Half): number | null {
  if (count < 2 || half.width <= 0) return null;
  const index = Math.round((x - half.x) / (half.width / (count - 1)));
  return Math.min(count - 1, Math.max(0, index));
}

/** An SVG path along `points`, left to right at an even step, drawn from `half`'s
 * baseline in `half`'s direction. One subpath per run between gaps. A reading with no
 * neighbor is not here -- it is a dot, and `flowDots` draws it. */
export function flowLine(points: (number | null)[], half: Half): string {
  if (points.length < 2) return '';
  return runs(points)
    .filter((r) => r.values.length >= 2)
    .map((r) =>
      r.values
        .map(
          (v, j) =>
            `${j === 0 ? 'M' : 'L'}${xOf(r.start + j, points.length, half).toFixed(1)},${yOf(v, half).toFixed(1)}`,
        )
        .join(' '),
    )
    .join(' ');
}

/** Every reading that has no neighbor, as a zero-length subpath each -- which a round
 * cap paints as a circle, so one declared width draws the dot.
 *
 * A source polling slower than one bucket has no neighbor anywhere: every minute
 * against ten-second buckets lands in one bucket in six. Left to the line these vanish,
 * and the panel draws nothing at all while the rate above it reads. A dot sits at the
 * value, never on the baseline, so a gap is still a gap. */
export function flowDots(points: (number | null)[], half: Half): string {
  if (points.length < 2) return '';
  return runs(points)
    .filter((r) => r.values.length === 1)
    .map((r) => {
      const at = `${xOf(r.start, points.length, half).toFixed(1)},${yOf(r.values[0], half).toFixed(1)}`;
      return `M${at} L${at}`;
    })
    .join(' ');
}

/** The same runs filled back to the baseline, one closed subpath each -- so a gap is a
 * hole in the fill rather than a span drawn across it. A run of one is left to
 * `flowLine`'s dot: one reading spans no width, so there is no area under it. */
export function flowArea(points: (number | null)[], half: Half): string {
  if (points.length < 2) return '';
  return runs(points)
    .filter((r) => r.values.length >= 2)
    .map((r) => {
      const first = xOf(r.start, points.length, half).toFixed(1);
      const last = xOf(r.start + r.values.length - 1, points.length, half).toFixed(1);
      const along = r.values
        .map(
          (v, j) =>
            `L${xOf(r.start + j, points.length, half).toFixed(1)},${yOf(v, half).toFixed(1)}`,
        )
        .join(' ');
      return `M${first},${half.baseline.toFixed(1)} ${along} L${last},${half.baseline.toFixed(1)} Z`;
    })
    .join(' ');
}
