<!--
  An inline multi-series line chart. Draws the points it is handed; the
  caller keeps the history. A `null` point is a gap: the line breaks
  there rather than drawing through it (`sparkline.ts`).

  The pointer reads the chart: over it, the legend gives the slot under
  the pointer instead of the latest, with its time when the caller gave
  `times`, and the warnings that held then, each with why it began. Over a
  warning that began at a width, the caller may draw that width's rate and
  range on top (`overlay`), so the dip that warned shows.
-->
<script lang="ts">
  import { bandPath, bandRect, maxOf, slotAt, sparklinePath, valueLabel, xAt } from './sparkline';
  import { clockTime, dateTime, lasted } from './time';
  import StateMark from './StateMark.svelte';
  import type { ExpectedPointDto, WarningReason } from './api';

  type Series = { label: string; color: string; points: (number | null)[] };

  /** A range drawn behind the lines, as the same 0..max scale reads it. */
  type Band = { low: number; high: number };

  /** A span a warning held, in epoch milliseconds. `to` is `null` while it still holds. */
  type Mark = {
    from: number;
    to: number | null;
    label: string;
    failed: boolean;
    reason?: WarningReason;
  };

  /** A width's rate, by when each point began, and its range, drawn over the chart's own. */
  type Overlay = { pointMillis: number; points: [number, number][]; expected: ExpectedPointDto[] };

  let {
    series,
    band,
    times,
    marks = [],
    compact = false,
    overlay = null,
    onmark,
  }: {
    series: Series[];
    /** One band for the whole chart, or one per point, `null` where there is none. */
    band?: Band | (Band | null)[];
    /** When each point's bucket began, in epoch milliseconds. */
    times?: number[];
    marks?: Mark[];
    compact?: boolean;
    overlay?: Overlay | null;
    /** Told the warning under the pointer that kept why it began, or `null` once none is. */
    onmark?: (mark: Mark | null) => void;
  } = $props();

  const width = 320;
  const height = 80;
  const markHeight = 6;

  let bands = $derived(Array.isArray(band) ? band : null);
  /** The overlay's points and range within the chart's span. */
  let shownOverlay = $derived.by(() => {
    if (!overlay || !times || times.length < 2) return null;
    const [first, last] = [times[0], times[times.length - 1]];
    const within = (at: number) => at >= first && at <= last;
    return {
      ...overlay,
      points: overlay.points.filter(([at]) => within(at)),
      expected: overlay.expected.filter((b) => within(b.fromMillis)),
    };
  });
  let flat = $derived(band && !Array.isArray(band) ? band : null);
  let maxValue = $derived(
    maxOf([
      ...series.map((s) => s.points),
      flat ? [flat.high] : [],
      (bands ?? []).map((b) => b?.high ?? null),
      shownOverlay ? shownOverlay.points.map(([, v]) => v) : [],
      shownOverlay ? shownOverlay.expected.map((b) => b.high) : [],
    ]),
  );
  let count = $derived(Math.max(0, ...series.map((s) => s.points.length)));

  let hovered = $state<number | null>(null);
  /** The moment under the pointer, finer than its slot: an overlay's points are finer than the slots on a long chart. */
  let pointedAt = $state<number | null>(null);
  /** The slot the legend reads: the one under the pointer, else the latest. */
  let slot = $derived(hovered ?? count - 1);

  /** A moment as the chart's own span calls for: the clock within a day, the date beyond it, or
   * for a moment more than a day before the chart's end. */
  let when = $derived.by(() => {
    const end = times?.[times.length - 1] ?? 0;
    const long = times && times.length > 1 && end - times[0] > 86_400_000;
    return (at: number) => (long || end - at > 86_400_000 ? dateTime(at / 1000) : clockTime(at));
  });

  let bandAt = $derived(flat ?? bands?.[slot] ?? null);
  /** Where each warning is drawn along the chart, from `x1` to `x2`. One begun within the last slot
   * still shows, at the chart's edge. */
  let drawn = $derived(
    times
      ? marks.map((m) => {
          const x1 = Math.min(xAt(m.from, times, width), width - 2);
          const x2 = m.to === null ? width : xAt(m.to, times, width);
          return { m, x1, x2: Math.max(x1 + 1, x2) };
        })
      : [],
  );
  /** The warnings drawn under the pointer's slot, the slot's half width either side. */
  let marksAt = $derived.by(() => {
    if (hovered === null || count < 2) return [];
    const half = width / (count - 1) / 2;
    const x = (hovered / (count - 1)) * width;
    return drawn.filter((d) => d.x1 <= x + half && d.x2 >= x - half).map((d) => d.m);
  });

  /** The warning under the pointer that kept why it began, told to the caller once it changes. */
  let reasoned = $derived(marksAt.find((m) => m.reason) ?? null);
  $effect(() => {
    onmark?.(reasoned);
  });

  /** Runs of an overlay's items whose times follow one another a point apart. */
  function runsOf<T>(items: T[], at: (item: T) => number, step: number): T[][] {
    const out: T[][] = [];
    for (const item of items) {
      const run = out[out.length - 1];
      if (run && at(item) - at(run[run.length - 1]) === step) run.push(item);
      else out.push([item]);
    }
    return out.filter((r) => r.length >= 2);
  }
  let overlayPaths = $derived.by(() => {
    if (!shownOverlay || !times) return null;
    const t = times;
    const x = (at: number) => xAt(at, t, width).toFixed(1);
    const y = (v: number) => (height - (v / maxValue) * height).toFixed(1);
    const step = shownOverlay.pointMillis;
    const line = runsOf(shownOverlay.points, ([at]) => at, step)
      .map((run) => run.map(([at, v], j) => `${j ? 'L' : 'M'}${x(at)},${y(v)}`).join(' '))
      .join(' ');
    const band = runsOf(shownOverlay.expected, (b) => b.fromMillis, step)
      .map((run) => {
        const top = run.map((b, j) => `${j ? 'L' : 'M'}${x(b.fromMillis)},${y(b.high)}`);
        const bottom = [...run].reverse().map((b) => `L${x(b.fromMillis)},${y(b.low)}`);
        return [...top, ...bottom, 'Z'].join(' ');
      })
      .join(' ');
    return { line, band };
  });

  /** Why `mark` began, as the legend says it. A quiet spell is named by when it began, before the warning did. */
  function because(mark: Mark & { reason: WarningReason }): string {
    const reason = mark.reason;
    return reason.kind === 'width'
      ? `${valueLabel(reason.perSecond)} rows/s over ${lasted(reason.windowMillis)}, expected ${valueLabel(reason.low)}–${valueLabel(reason.high)}`
      : `no rows since ${when(mark.from - reason.silenceMillis)}, expected a row within ${lasted(reason.edgeMillis)}`;
  }

  /** While a warning's width is drawn over the chart, the legend reads that width under the
   * pointer: its rate and range at the nearest of its points. */
  let overlaid = $derived.by(() => {
    const reason = reasoned?.reason;
    if (!shownOverlay || reason?.kind !== 'width' || pointedAt === null) return null;
    // A point stands for the stretch it begins, so it is nearest by that stretch's middle.
    const near = (at: number) => Math.abs(at + shownOverlay!.pointMillis / 2 - pointedAt!);
    const nearest = <T,>(items: T[], at: (item: T) => number) =>
      items.reduce<T | null>(
        (best, item) => (best === null || near(at(item)) < near(at(best)) ? item : best),
        null,
      );
    const point = nearest(shownOverlay.points, ([at]) => at);
    const range = nearest(shownOverlay.expected, (b) => b.fromMillis);
    return {
      label: `rows/s over ${lasted(reason.windowMillis)}`,
      value: point?.[1] ?? null,
      range: range ?? null,
    };
  });

  function track(e: PointerEvent) {
    const box = (e.currentTarget as SVGElement).getBoundingClientRect();
    if (count < 2 || box.width === 0) return;
    const share = (e.clientX - box.left) / box.width;
    hovered = slotAt(share, count);
    pointedAt =
      times && times.length > 1 ? times[0] + share * (times[times.length - 1] - times[0]) : null;
  }
</script>

<div class="sparkline" class:compact>
  <svg
    viewBox="0 0 {width} {height}"
    preserveAspectRatio="none"
    role="img"
    aria-label={series.map((s) => s.label).join(', ')}
    onpointermove={track}
    onpointerleave={() => (hovered = null)}
  >
    {#if flat}
      {@const r = bandRect(flat.low, flat.high, height, maxValue)}
      <rect class="band" class:under={overlayPaths} x="0" y={r.y} {width} height={r.height} />
    {:else if bands}
      <path class="band" class:under={overlayPaths} d={bandPath(bands, width, height, maxValue)} />
    {/if}
    {#each drawn as { m, x1, x2 } (m.from + m.label)}
      <!-- A strip along the foot, so the band behind the line still reads while a warning holds. -->
      <rect
        class="mark"
        class:failed={m.failed}
        x={x1}
        y={height - markHeight}
        width={x2 - x1}
        height={markHeight}
      />
    {/each}
    {#if overlayPaths}
      <path class="overlay-band" d={overlayPaths.band} />
      <path
        class="overlay-line"
        d={overlayPaths.line}
        fill="none"
        stroke-width="1.5"
        vector-effect="non-scaling-stroke"
      />
    {/if}
    {#each series as s (s.label)}
      <!-- Faint under an overlay, so the width that warned reads first. -->
      <path
        class:under={overlayPaths}
        d={sparklinePath(s.points, width, height, maxValue)}
        stroke={s.color}
        fill="none"
        stroke-width="1.5"
        vector-effect="non-scaling-stroke"
      />
    {/each}
    {#if hovered !== null && count > 1}
      {@const x = (hovered / (count - 1)) * width}
      <line class="cursor" x1={x} x2={x} y1="0" y2={height} vector-effect="non-scaling-stroke" />
    {/if}
  </svg>
  {#if !compact}
    <div class="legend">
      {#if hovered !== null && times?.[hovered] !== undefined}
        <span class="legend-item mono">{when(times[hovered])}</span>
      {/if}
      {#if overlaid}
        <span class="legend-item">
          <span class="swatch overlay-swatch"></span>
          {overlaid.label}: {valueLabel(overlaid.value)}
        </span>
        {#if overlaid.range}
          <span class="legend-item">
            <span class="swatch band-swatch overlay-band-swatch"></span>
            expected: {valueLabel(overlaid.range.low)}–{valueLabel(overlaid.range.high)}
          </span>
        {/if}
      {:else}
        {#each series as s (s.label)}
          <span class="legend-item">
            <span class="swatch" style:background={s.color}></span>
            {s.label}: {valueLabel(s.points[slot])}
          </span>
        {/each}
      {/if}
      {#if bandAt && !overlaid}
        <span class="legend-item">
          <span class="swatch band-swatch"></span>
          expected: {valueLabel(bandAt.low)}–{valueLabel(bandAt.high)}
        </span>
      {/if}
      {#each marksAt as m (m.from + m.label)}
        <span class="legend-item" class:failed={m.failed} class:warned={!m.failed}>
          <StateMark state={m.failed ? 'error' : 'warning'} />
          {m.label}, {m.to === null
            ? `since ${when(m.from)}`
            : `${when(m.from)}–${when(m.to)}`}{#if m.reason}:
            {because({ ...m, reason: m.reason })}{/if}
        </span>
      {/each}
    </div>
  {/if}
</div>

<style>
  .sparkline {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }

  svg {
    display: block;
    width: 100%;
    height: 80px;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    cursor: crosshair;
  }

  /* Neutral, with its edges drawn, so no state's color hides it. */
  .band {
    fill: color-mix(in oklch, var(--ink) 7%, transparent);
    stroke: color-mix(in oklch, var(--ink) 55%, transparent);
    stroke-width: 1.25;
    stroke-dasharray: 4 3;
    vector-effect: non-scaling-stroke;
  }

  .band-swatch {
    background: color-mix(in oklch, var(--ink) 7%, transparent);
    outline: 1.25px dashed color-mix(in oklch, var(--ink) 55%, transparent);
    outline-offset: -1px;
  }

  .mark {
    fill: color-mix(in oklch, var(--warn) 60%, transparent);
  }

  /* The width that warned, in the warning's own color, over the chart's neutral line and band. */
  .under {
    opacity: 0.25;
  }
  .overlay-band {
    fill: color-mix(in oklch, var(--warn) 14%, transparent);
    stroke: var(--warn);
    stroke-width: 1.25;
    stroke-dasharray: 4 3;
    vector-effect: non-scaling-stroke;
  }

  .overlay-line {
    stroke: var(--warn);
  }

  .overlay-swatch {
    background: var(--warn);
  }

  .overlay-band-swatch {
    background: color-mix(in oklch, var(--warn) 14%, transparent);
    outline-color: var(--warn);
  }

  .mark.failed {
    fill: color-mix(in oklch, var(--error) 60%, transparent);
  }

  .cursor {
    stroke: var(--dim);
    stroke-width: 1;
  }

  .compact {
    gap: 0;
  }

  .compact svg {
    height: 2.25rem;
    border: 0;
    border-radius: 0;
    background: transparent;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem 1rem;
    font-size: var(--text-label);
    color: var(--dim);
  }

  .legend-item {
    display: flex;
    align-items: center;
    gap: 0.3rem;
  }

  .legend-item.warned {
    color: var(--warn);
  }

  .legend-item.failed {
    color: var(--error);
  }

  .swatch {
    width: 8px;
    height: 8px;
    border-radius: 2px;
    display: inline-block;
  }
</style>
