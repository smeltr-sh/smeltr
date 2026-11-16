<!--
  Several series on one axis: a line each, or as bars, stacked, each bar what that minute, hour or day added up to.
  The colors are the categorical palette's, in rank order (`metricsView.ts`, `fold`); a series past them is Other,
  in a neutral that is no series' hue.

  The svg is sized from its measured width, so one user unit is one pixel and the pointer reads straight across
  (`FlowChart.svelte` says why).
-->
<script lang="ts">
  import { axisLabel, axisStep, axisTop, axisValues } from './flowChart';
  import type { Drawn } from './metricsView';

  let {
    series,
    bars,
    ticks,
    unit,
    at,
    format,
    label,
  }: {
    series: Drawn[];
    /** Stacked bars of totals rather than lines. */
    bars: boolean;
    /** One per point; empty where the axis names nothing. */
    ticks: string[];
    /** What the axis counts, such as `MiB/s` or `rows per hour`. */
    unit: string;
    /** What point `i` is, for the tooltip. */
    at: (i: number) => string;
    format: (value: number) => string;
    /** What the chart shows, for a screen reader. */
    label: string;
  } = $props();

  const PLOT = 240;
  const AXIS = 22;
  const PAD = 10;
  /** A monospace glyph at `--text-micro`, and the gap from the labels to the plot. */
  const GLYPH = 7.5;
  const TICK_GAP = 8;

  let width = $state(0);
  let count = $derived(Math.max(0, ...series.map((s) => s.points.length)));
  const stacked = (i: number) => series.reduce((a, s) => a + (s.points[i] ?? 0), 0);
  let max = $derived(
    bars
      ? Math.max(0, ...Array.from({ length: count }, (_, i) => stacked(i)))
      : Math.max(0, ...series.flatMap((s) => s.points.map((v) => v ?? 0))),
  );
  let top = $derived(axisTop(max));
  let step = $derived(axisStep(max));
  let grid = $derived(axisValues(max).map((v) => ({ v, label: axisLabel(v, step) })));
  let gutter = $derived(Math.max(...grid.map((g) => g.label.length), 3) * GLYPH + TICK_GAP);
  let plotW = $derived(Math.max(0, width - gutter - 4));
  const y = (v: number) => PLOT - PAD / 2 - (v / (top || 1)) * (PLOT - PAD);
  let slotW = $derived(count > 0 ? plotW / count : 0);
  const xLine = (i: number) => gutter + (count > 1 ? (i * plotW) / (count - 1) : plotW / 2);
  const xBar = (i: number) => gutter + i * slotW;
  const tone = (s: Drawn) => `series-${s.slot ?? 'other'}`;

  /** A line through `points`, broken where one is missing: a gap is missing history, not zero. */
  function path(points: (number | null)[]): string {
    let d = '';
    let open = false;
    points.forEach((v, i) => {
      if (v === null) {
        open = false;
        return;
      }
      d += `${open ? 'L' : 'M'}${xLine(i).toFixed(1)} ${y(v).toFixed(1)} `;
      open = true;
    });
    return d;
  }

  let hover = $state<number | null>(null);
  function track(e: PointerEvent) {
    const box = (e.currentTarget as SVGRectElement).ownerSVGElement?.getBoundingClientRect();
    if (!box || count === 0) return;
    const x = e.clientX - box.left - gutter;
    const i = bars ? Math.floor(x / slotW) : Math.round((x / plotW) * (count - 1));
    hover = i >= 0 && i < count ? i : null;
  }
  let hoverX = $derived(hover === null ? null : bars ? xBar(hover) + slotW / 2 : xLine(hover));
  let tip = $derived(
    hover === null
      ? []
      : series
          .map((s) => ({ s, v: s.points[hover!] ?? null }))
          .sort((a, b) => (b.v ?? -1) - (a.v ?? -1)),
  );
</script>

<div class="metrics-chart" bind:clientWidth={width}>
  {#if plotW > 1}
    <svg viewBox="0 0 {width} {PLOT + AXIS}" role="img" aria-label={label}>
      {#each grid as g (g.v)}
        <line
          class="grid"
          class:zero={g.v === 0}
          x1={gutter}
          x2={width - 4}
          y1={y(g.v)}
          y2={y(g.v)}
        />
        <text class="tick" x={gutter - TICK_GAP} y={y(g.v)}>{g.label}</text>
      {/each}
      <text class="unit" x={gutter + 4} y="10">{unit}</text>
      {#if hover !== null && bars}
        <rect class="hover-band" x={xBar(hover)} y="0" width={slotW} height={PLOT} />
      {/if}
      {#if bars}
        {#each Array.from({ length: count }, (_, i) => i) as i (i)}
          {#each series as s, k (s.name)}
            {@const below = series.slice(0, k).reduce((a, t) => a + (t.points[i] ?? 0), 0)}
            {@const v = s.points[i] ?? 0}
            {#if v > 0}
              <!-- The last bar is still filling, so it is drawn fainter than the closed ones. -->
              <rect
                class="bar {tone(s)}"
                class:filling={i === count - 1}
                x={xBar(i) + slotW * 0.12}
                y={y(below + v) + 1}
                width={Math.max(1, slotW * 0.76)}
                height={Math.max(0, y(below) - y(below + v) - 2)}
              />
            {/if}
          {/each}
        {/each}
      {:else}
        {#each series as s (s.name)}
          <path class="line {tone(s)}" d={path(s.points)} />
        {/each}
        {#if hoverX !== null}
          <line class="rule" x1={hoverX} x2={hoverX} y1="0" y2={PLOT} />
          {#each tip as { s, v } (s.name)}
            {#if v !== null}
              <circle class="mark {tone(s)}" cx={hoverX} cy={y(v)} r="4" />
            {/if}
          {/each}
        {/if}
      {/if}
      {#each ticks as t, i (i)}
        {#if t}
          <text class="clock" x={bars ? xBar(i) + slotW / 2 : xLine(i)} y={PLOT + 15}>{t}</text>
        {/if}
      {/each}
      <!-- Last, so every move lands here. Presentational: the table below says what the chart draws. -->
      <rect
        class="surface"
        role="presentation"
        x={gutter}
        y="0"
        width={plotW}
        height={PLOT}
        onpointermove={track}
        onpointerleave={() => (hover = null)}
      />
    </svg>
    {#if hover !== null && hoverX !== null}
      <div class="tip" style:left="{Math.max(0, Math.min(hoverX + 12, width - 240))}px">
        <div class="tip-at mono">{at(hover)}</div>
        {#each tip as { s, v } (s.name)}
          <div class="tip-row">
            <span class="swatch {tone(s)}"></span><span class="mono">{s.name || 'All'}</span><strong
              >{v === null ? '--' : format(v)}</strong
            >
          </div>
        {/each}
      </div>
    {/if}
  {/if}
</div>

<style>
  .metrics-chart {
    position: relative;
    width: 100%;
    /* Held until the box is measured, so what is below does not drop on first paint. */
    min-height: 262px;
  }

  svg {
    display: block;
    width: 100%;
    height: 262px;
  }

  .grid {
    stroke: var(--border);
    stroke-width: 1;
  }

  .grid.zero {
    stroke: var(--dim);
  }

  .tick,
  .clock,
  .unit {
    fill: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-micro);
  }

  .tick {
    text-anchor: end;
    dominant-baseline: middle;
  }

  .clock {
    text-anchor: middle;
  }

  .line {
    fill: none;
    stroke: var(--c);
    stroke-width: 2;
    stroke-linejoin: round;
  }

  .rule {
    stroke: var(--dim);
    stroke-dasharray: 3 3;
  }

  /* Filled with the panel's own background, so the line does not run through it. */
  .mark {
    fill: var(--panel);
    stroke: var(--c);
    stroke-width: 2;
  }

  .bar {
    fill: var(--c);
  }

  .bar.filling {
    fill-opacity: 0.4;
  }

  .hover-band {
    fill: var(--dim);
    fill-opacity: 0.08;
  }

  /* `fill: transparent` is painted, so it takes the pointer where `none` would let it through. */
  .surface {
    fill: transparent;
    pointer-events: all;
    cursor: crosshair;
  }

  .tip {
    position: absolute;
    top: 8px;
    min-width: 220px;
    padding: 8px 10px;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    box-shadow: 0 4px 14px rgb(0 0 0 / 0.12);
    pointer-events: none;
  }

  .tip-at {
    color: var(--dim);
    margin-bottom: 4px;
  }

  .tip-row {
    display: grid;
    grid-template-columns: 14px 1fr auto;
    gap: 6px;
    align-items: center;
  }

  .tip-row strong {
    font-variant-numeric: tabular-nums;
  }

  .swatch {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 2px;
    background: var(--c);
  }
</style>
