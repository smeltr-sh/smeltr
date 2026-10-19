// The geometry behind `Sparkline.svelte`. A point is `null` where a series has no data --
// a gap the line breaks at rather than draws through, never a zero (E2, `flow.ts`). That
// gap rule is `runs`, and `flowChart.ts` splits on it too, so the two charts break a line
// in the same place. What each does with a reading that has no neighbor is its own.

/** Never 0: an all-zero series divided by its real max is NaN everywhere. `null` does not
 * count toward it. */
export function maxOf(series: (number | null)[][]): number {
  return Math.max(1, ...series.flatMap((s) => s.filter((v): v is number => v !== null)));
}

/** `points` split at every `null`. A run may hold a single value: a line has nothing to
 * connect one to, but a dot can still sit at it, so which it is belongs to the caller
 * rather than here. */
export function runs(points: (number | null)[]): { start: number; values: number[] }[] {
  const out: { start: number; values: number[] }[] = [];
  let current: { start: number; values: number[] } | null = null;
  points.forEach((v, i) => {
    if (v === null) {
      current = null;
      return;
    }
    if (current === null) {
      current = { start: i, values: [] };
      out.push(current);
    }
    current.values.push(v);
  });
  return out;
}

/** An SVG path plotting `points` left to right at an even step, 0..`maxValue` scaled to
 * `height`..0 (SVG y grows downward). A `null` breaks the path rather than joining across
 * it, so a gap in the data is a gap on the line -- one subpath per run either side of it. */
export function sparklinePath(
  points: (number | null)[],
  width: number,
  height: number,
  maxValue: number,
): string {
  if (points.length < 2) return '';
  const stepX = width / (points.length - 1);
  const y = (v: number) => height - (v / maxValue) * height;
  return (
    runs(points)
      // A lone reading draws nothing here: this chart is a line, and one point has nothing
      // to connect to.
      .filter((r) => r.values.length >= 2)
      .map((r) =>
        r.values
          .map(
            (v, j) =>
              `${j === 0 ? 'M' : 'L'}${((r.start + j) * stepX).toFixed(1)},${y(v).toFixed(1)}`,
          )
          .join(' '),
      )
      .join(' ')
  );
}

/** Where a band from `low` to `high` sits on the same scale as `sparklinePath`: its top and
 * how tall it is. */
export function bandRect(
  low: number,
  high: number,
  height: number,
  maxValue: number,
): { y: number; height: number } {
  const y = (v: number) => height - (v / maxValue) * height;
  return { y: y(high), height: y(low) - y(high) };
}

/** A value as the legend and the hover write it: whole above ten, one decimal below, so a quiet
 * rate does not read as zero. `'—'` for a gap. */
export function valueLabel(v: number | null | undefined): string {
  if (v === undefined || v === null) return '—';
  return Math.abs(v) >= 10 ? String(Math.round(v)) : String(Math.round(v * 10) / 10);
}

/** The slot under a pointer `fraction` of the way across a chart of `count` points. */
export function slotAt(fraction: number, count: number): number {
  return Math.min(count - 1, Math.max(0, Math.round(fraction * (count - 1))));
}

/** Where a moment falls across a chart whose points sit at `times`, as an x on `width`,
 * clamped to its ends. */
export function xAt(at: number, times: number[], width: number): number {
  if (times.length < 2) return 0;
  const first = times[0];
  const last = times[times.length - 1];
  return Math.min(width, Math.max(0, ((at - first) / (last - first)) * width));
}

/** An SVG path filling each run of bands, point by point on the scale `sparklinePath` draws on.
 * A `null` breaks it, as it breaks a line: there was no history to learn a band from. */
export function bandPath(
  bands: ({ low: number; high: number } | null)[],
  width: number,
  height: number,
  maxValue: number,
): string {
  if (bands.length < 2) return '';
  const stepX = width / (bands.length - 1);
  const y = (v: number) => (height - (v / maxValue) * height).toFixed(1);
  const out: string[] = [];
  let run: { i: number; low: number; high: number }[] = [];
  const close = () => {
    if (run.length >= 2) {
      const top = run.map(
        (b, j) => `${j === 0 ? 'M' : 'L'}${(b.i * stepX).toFixed(1)},${y(b.high)}`,
      );
      const bottom = [...run].reverse().map((b) => `L${(b.i * stepX).toFixed(1)},${y(b.low)}`);
      out.push([...top, ...bottom, 'Z'].join(' '));
    }
    run = [];
  };
  bands.forEach((b, i) => (b === null ? close() : run.push({ i, ...b })));
  close();
  return out.join(' ');
}
