// Vendored from `gui-frontend/src/sparkline.ts` for `flowChart.ts`, which takes
// `runs` from here; keep it in step: the website builds standalone and does not
// read gui-frontend at build time. The app draws a plain sparkline with the rest
// of this module, on its Errors panel; the site has no such panel, so nothing
// here does.
//
// A point is `null` where a series has no data -- a gap the line breaks at rather
// than draws through, never a zero (E2, `flow.ts`). That gap rule is `runs`, and
// `flowChart.ts` splits on it too, so the two charts break a line in the same place.
// What each does with a reading that has no neighbor is its own.

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

/** The legend's trailing number: the series' latest value, rounded. `'—'` with none yet or a
 * gap at the very end -- never `0`, which would read as a real reading rather than an absent
 * one. */
export function lastLabel(points: (number | null)[]): string {
  const last = points.at(-1);
  return last === undefined || last === null ? '—' : String(Math.round(last));
}
