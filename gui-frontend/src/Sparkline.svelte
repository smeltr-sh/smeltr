<!--
  An inline multi-series line chart. Draws the points it is handed; the
  caller keeps the history. A `null` point is a gap: the line breaks
  there rather than drawing through it (`sparkline.ts`).

  The pointer reads the chart: over it, the legend gives the slot under
  the pointer instead of the latest, with its time when the caller gave
  `times`, and the warnings that held then.
-->
<script lang="ts">
  import { bandPath, bandRect, maxOf, slotAt, sparklinePath, valueLabel, xAt } from './sparkline';
  import { clockTime, dateTime } from './time';
  import StateMark from './StateMark.svelte';

  type Series = { label: string; color: string; points: (number | null)[] };

  /** A range drawn behind the lines, as the same 0..max scale reads it. */
  type Band = { low: number; high: number };

  /** A span a warning held, in epoch milliseconds. `to` is `null` while it still holds. */
  type Mark = { from: number; to: number | null; label: string; failed: boolean };

  let {
    series,
    band,
    times,
    marks = [],
    compact = false,
  }: {
    series: Series[];
    /** One band for the whole chart, or one per point, `null` where there is none. */
    band?: Band | (Band | null)[];
    /** When each point's bucket began, in epoch milliseconds. */
    times?: number[];
    marks?: Mark[];
    compact?: boolean;
  } = $props();

  const width = 320;
  const height = 80;

  let bands = $derived(Array.isArray(band) ? band : null);
  let flat = $derived(band && !Array.isArray(band) ? band : null);
  let maxValue = $derived(
    maxOf([
      ...series.map((s) => s.points),
      flat ? [flat.high] : [],
      (bands ?? []).map((b) => b?.high ?? null),
    ]),
  );
  let count = $derived(Math.max(0, ...series.map((s) => s.points.length)));

  let hovered = $state<number | null>(null);
  /** The slot the legend reads: the one under the pointer, else the latest. */
  let slot = $derived(hovered ?? count - 1);

  /** A moment as the chart's own span calls for: the clock within a day, the date beyond it, or
   * for a moment more than a day before the chart's end. */
  let when = $derived.by(() => {
    const end = times?.[times.length - 1] ?? 0;
    const long = times && times.length > 1 && end - times[0] > 86_400_000;
    return (at: number) => (long || end - at > 86_400_000 ? dateTime(at / 1000) : clockTime(at));
  });

  /** How long each slot lasts: a slot reads every warning that held at any time within it. */
  let step = $derived(times && times.length > 1 ? times[1] - times[0] : 0);

  let bandAt = $derived(flat ?? bands?.[slot] ?? null);
  let marksAt = $derived(
    times && hovered !== null
      ? marks.filter((m) => {
          const at = times[hovered!];
          return m.from < at + step && (m.to === null || m.to >= at);
        })
      : [],
  );

  function track(e: PointerEvent) {
    const box = (e.currentTarget as SVGElement).getBoundingClientRect();
    if (count < 2 || box.width === 0) return;
    hovered = slotAt((e.clientX - box.left) / box.width, count);
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
      <rect class="band" x="0" y={r.y} {width} height={r.height} />
    {:else if bands}
      <path class="band" d={bandPath(bands, width, height, maxValue)} />
    {/if}
    {#if times}
      <!-- A warning begun within the last slot still shows, at the chart's edge. -->
      {#each marks as m (m.from + m.label)}
        {@const x1 = Math.min(xAt(m.from, times, width), width - 2)}
        {@const x2 = m.to === null ? width : xAt(m.to, times, width)}
        <rect
          class="mark"
          class:failed={m.failed}
          x={x1}
          y="0"
          width={Math.max(1, x2 - x1)}
          {height}
        />
        <line class="edge" class:failed={m.failed} {x1} x2={x1} y1="0" y2={height} />
        {#if m.to !== null}
          <line class="edge" class:failed={m.failed} x1={x2} {x2} y1="0" y2={height} />
        {/if}
      {/each}
    {/if}
    {#each series as s (s.label)}
      <path
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
      {#each series as s (s.label)}
        <span class="legend-item">
          <span class="swatch" style:background={s.color}></span>
          {s.label}: {valueLabel(s.points[slot])}
        </span>
      {/each}
      {#if bandAt}
        <span class="legend-item">
          <span class="swatch band-swatch"></span>
          expected: {valueLabel(bandAt.low)}–{valueLabel(bandAt.high)}
        </span>
      {/if}
      {#each marksAt as m (m.from + m.label)}
        <span class="legend-item" class:failed={m.failed} class:warned={!m.failed}>
          <StateMark state={m.failed ? 'error' : 'warning'} />
          {m.label}, {m.to === null ? `since ${when(m.from)}` : `${when(m.from)}–${when(m.to)}`}
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

  .band {
    fill: color-mix(in oklch, var(--accent) 18%, transparent);
    stroke: none;
  }

  .band-swatch {
    background: color-mix(in oklch, var(--accent) 35%, transparent);
  }

  .mark {
    fill: color-mix(in oklch, var(--warn) 12%, transparent);
  }

  .mark.failed {
    fill: color-mix(in oklch, var(--error) 12%, transparent);
  }

  .edge {
    stroke: var(--warn);
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }

  .edge.failed {
    stroke: var(--error);
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
