<!--
  Several series on one axis: a line each, or as bars, stacked, each bar what that minute, hour or day added up to.
  The colors are the categorical palette's, in rank order (`metricsView.ts`, `toneOf`); the series folded into Other
  is drawn in a neutral that is no series' hue.

  The svg is sized from its measured width, so one user unit is one pixel and the pointer reads straight across
  (`FlowChart.svelte` says why).
-->
<script lang="ts">
  import { axisLabel, axisStep, axisTop, axisValues } from './flowChart';
  import { toneOf, type Drawn, type Mark } from './metricsView';

  let {
    series,
    bars,
    ticks,
    unit,
    at,
    format,
    label,
    previous = null,
    marks = null,
    right = null,
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
    /** Each series' points a week before, drawn faint behind it; `null` draws none. */
    previous?: (number | null)[][] | null;
    /** Deploys and issues, in a strip above the plot; `null` draws no strip. */
    marks?: Mark[] | null;
    /** The right-hand axis, for the series marked `right`: a second metric in another unit. */
    right?: { unit: string; format: (value: number) => string } | null;
  } = $props();

  const PLOT = 240;
  const AXIS = 22;
  const PAD = 10;
  /** A monospace glyph at `--text-micro`, and the gap from the labels to the plot. */
  const GLYPH = 7.5;
  const TICK_GAP = 8;
  /** The strip above the plot the marks stand in. */
  const STRIP = 16;

  let width = $state(0);
  let count = $derived(Math.max(0, ...series.map((s) => s.points.length)));
  const stacked = (i: number) => series.reduce((a, s) => a + (s.points[i] ?? 0), 0);
  /** Last week's bars, stacked: one line across their tops. */
  const stackedBefore = (i: number) =>
    previous?.some((p) => (p[i] ?? null) !== null)
      ? previous.reduce((a, p) => a + (p[i] ?? 0), 0)
      : null;
  let max = $derived(
    bars
      ? Math.max(
          0,
          ...Array.from({ length: count }, (_, i) => Math.max(stacked(i), stackedBefore(i) ?? 0)),
        )
      : Math.max(
          0,
          ...series
            .flatMap((s, k) => (s.right ? [] : [s.points, previous?.[k] ?? []]))
            .flat()
            .map((v) => v ?? 0),
        ),
  );
  let maxR = $derived(
    Math.max(
      0,
      ...series
        .flatMap((s, k) => (s.right ? [s.points, previous?.[k] ?? []] : []))
        .flat()
        .map((v) => v ?? 0),
    ),
  );
  let topR = $derived(axisTop(maxR));
  let stepR = $derived(axisStep(maxR));
  let gridR = $derived(
    right ? axisValues(maxR).map((v) => ({ v, label: axisLabel(v, stepR) })) : [],
  );
  let gutterR = $derived(
    right ? Math.max(...gridR.map((g) => g.label.length), 3) * GLYPH + TICK_GAP : 0,
  );
  let top0 = $derived(marks ? STRIP : 0);
  let top = $derived(axisTop(max));
  let step = $derived(axisStep(max));
  let grid = $derived(axisValues(max).map((v) => ({ v, label: axisLabel(v, step) })));
  let gutter = $derived(Math.max(...grid.map((g) => g.label.length), 3) * GLYPH + TICK_GAP);
  let plotW = $derived(Math.max(0, width - gutter - 4 - gutterR));
  const y = (v: number) => PLOT - PAD / 2 - (v / (top || 1)) * (PLOT - PAD);
  const yR = (v: number) => PLOT - PAD / 2 - (v / (topR || 1)) * (PLOT - PAD);
  const yOf = (s: Drawn) => (s.right ? yR : y);
  let slotW = $derived(count > 0 ? plotW / count : 0);
  const xLine = (i: number) => gutter + (count > 1 ? (i * plotW) / (count - 1) : plotW / 2);
  const xBar = (i: number) => gutter + i * slotW;
  const tone = (s: Drawn) => toneOf(s.slot);
  /** Each axis has its own Other. */
  const key = (s: Drawn) => `${s.right ? 'r' : ''}${s.slot ?? -1}`;

  /** A line through `points`, broken where one is missing: a gap is missing history, not zero. */
  function path(points: (number | null)[], at = y): string {
    let d = '';
    let open = false;
    points.forEach((v, i) => {
      if (v === null) {
        open = false;
        return;
      }
      d += `${open ? 'L' : 'M'}${xLine(i).toFixed(1)} ${at(v).toFixed(1)} `;
      open = true;
    });
    return d;
  }

  /** Where `points` holds a value with none on either side: a line through one point draws nothing, so it is a dot. */
  const lone = (points: (number | null)[]) =>
    points.flatMap((v, i) =>
      v !== null && (points[i - 1] ?? null) === null && (points[i + 1] ?? null) === null
        ? [{ i, v }]
        : [],
    );

  let hover = $state<number | null>(null);
  function track(e: PointerEvent) {
    const box = (e.currentTarget as SVGRectElement).ownerSVGElement?.getBoundingClientRect();
    if (!box || count === 0) return;
    const x = e.clientX - box.left - gutter;
    const i = bars ? Math.floor(x / slotW) : Math.round((x / plotW) * (count - 1));
    hover = i >= 0 && i < count ? i : null;
  }
  /** The marks in point `i`'s span. */
  /** How wide a point is, in pixels. */
  let pointW = $derived(bars ? slotW : count > 1 ? plotW / (count - 1) : plotW);
  const xAt = (i: number) => (bars ? xBar(0) : xLine(0)) + i * pointW;
  const markX = (m: Mark) => xAt(m.i);
  /** The marks the pointer at point `i` is on: an issue it is within, or a deploy within a few pixels, so a deploy
   * narrower than a point is not missed. */
  const marksAt = (i: number) => {
    const reach = Math.max(bars ? 0 : 0.5, 6 / (pointW || 1));
    const at = bars ? i + 0.5 : i;
    const half = bars ? 0.5 : 0;
    return (marks ?? []).filter((m) =>
      m.end !== undefined
        ? m.i <= at + half && m.end >= at - half
        : Math.abs(m.i - at) <= Math.max(reach, half),
    );
  };
  /** Last week's bar tops as one stepped line, broken where it held nothing. */
  let beforeSteps = $derived.by(() => {
    let d = '';
    for (let i = 0; i < count; i++) {
      const v = stackedBefore(i);
      if (v !== null) d += `M${xBar(i).toFixed(1)} ${y(v).toFixed(1)} h${slotW.toFixed(1)} `;
    }
    return d;
  });

  let hoverX = $derived(hover === null ? null : bars ? xBar(hover) + slotW / 2 : xLine(hover));
  let tip = $derived(
    hover === null
      ? []
      : series
          .map((s, k) => ({ s, v: s.points[hover!] ?? null, was: previous?.[k]?.[hover!] ?? null }))
          .sort((a, b) => (b.v ?? -1) - (a.v ?? -1)),
  );
</script>

<div class="metrics-chart" bind:clientWidth={width}>
  {#if plotW > 1}
    <svg
      viewBox="0 {-top0} {width} {PLOT + AXIS + top0}"
      style:height="{PLOT + AXIS + top0}px"
      role="img"
      aria-label={label}
    >
      {#each grid as g (g.v)}
        <line
          class="grid"
          class:zero={g.v === 0}
          x1={gutter}
          x2={gutter + plotW}
          y1={y(g.v)}
          y2={y(g.v)}
        />
        <text class="tick" x={gutter - TICK_GAP} y={y(g.v)}>{g.label}</text>
      {/each}
      <text class="unit" x={gutter + 4} y="10">{unit}</text>
      {#if right}
        {#each gridR as g (g.v)}
          <text class="tick right" x={gutter + plotW + TICK_GAP} y={yR(g.v)}>{g.label}</text>
        {/each}
        <text class="unit right-unit" x={gutter + plotW - 4} y="10">{right.unit} →</text>
      {/if}
      {#if hover !== null && bars}
        <rect class="hover-band" x={xBar(hover)} y="0" width={slotW} height={PLOT} />
      {/if}
      {#if marks}
        {#each marks as m, k (k)}
          {#if m.end !== undefined}
            <rect
              class="span"
              x={markX(m)}
              y={-STRIP / 2}
              width={Math.max(2, xAt(Math.min(m.end, count - 1)) - markX(m))}
              height={PLOT + STRIP / 2}
            />
          {/if}
          <line class="mark-rule {m.kind}" x1={markX(m)} x2={markX(m)} y1={-STRIP / 2} y2={PLOT} />
          {#if m.kind === 'deploy'}
            <rect
              class="flag deploy"
              x={markX(m) - 4}
              y={-STRIP / 2 - 4}
              width="8"
              height="8"
              transform="rotate(45 {markX(m)} {-STRIP / 2})"
            />
          {:else}
            <path
              class="flag issue"
              d="M{markX(m) - 5} {-STRIP / 2 + 4} L{markX(m)} {-STRIP / 2 - 5} L{markX(m) +
                5} {-STRIP / 2 + 4} Z"
            />
          {/if}
        {/each}
      {/if}
      {#if bars}
        {#if previous}
          <path class="before" d={beforeSteps} />
        {/if}
        {#each Array.from({ length: count }, (_, i) => i) as i (i)}
          {#each series as s, k (key(s))}
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
        {#if previous}
          {#each series as s, k (key(s))}
            <path class="line before {tone(s)}" d={path(previous[k] ?? [], yOf(s))} />
          {/each}
        {/if}
        {#each series as s (key(s))}
          <path class="line {tone(s)}" d={path(s.points, yOf(s))} />
          {#each lone(s.points) as { i, v } (i)}
            <circle class="dot {tone(s)}" cx={xLine(i)} cy={yOf(s)(v)} r="2.5" />
          {/each}
        {/each}
        {#if hoverX !== null}
          <line class="rule" x1={hoverX} x2={hoverX} y1="0" y2={PLOT} />
          {#each tip as { s, v } (key(s))}
            {#if v !== null}
              <circle class="mark {tone(s)}" cx={hoverX} cy={yOf(s)(v)} r="4" />
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
        y={-top0}
        width={plotW}
        height={PLOT + top0}
        onpointermove={track}
        onpointerleave={() => (hover = null)}
      />
    </svg>
    {#if hover !== null && hoverX !== null}
      <div
        class="tip"
        class:wide={previous !== null}
        style:left={hoverX > width / 2 ? null : `${hoverX + 12}px`}
        style:right={hoverX > width / 2 ? `${width - hoverX + 12}px` : null}
      >
        <div class="tip-at mono">{at(hover)}</div>
        {#each marksAt(hover) as m, k (k)}
          <div class="tip-mark {m.kind}">{m.text}</div>
        {/each}
        {#if previous}
          <div class="tip-row head">
            <span></span><span></span><span></span><span>A week before</span>
          </div>
        {/if}
        {#each tip as { s, v, was } (key(s))}
          <div class="tip-row" class:compare={previous !== null}>
            <span class="swatch {tone(s)}"></span><span class="mono">{s.name || 'All'}</span><strong
              >{v === null ? '--' : s.right && right ? right.format(v) : format(v)}</strong
            >{#if previous}<span class="was">{was === null ? '--' : format(was)}</span>{/if}
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

  .tick.right {
    text-anchor: start;
  }

  .right-unit {
    text-anchor: end;
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

  .dot {
    fill: var(--c);
  }

  .before {
    fill: none;
    stroke: var(--dim);
    stroke-width: 1.5;
    stroke-dasharray: 4 3;
  }

  .line.before {
    stroke: var(--c);
    stroke-opacity: 0.45;
  }

  .mark-rule {
    stroke: var(--ink);
    stroke-opacity: 0.35;
    stroke-dasharray: 2 3;
  }

  .mark-rule.issue {
    stroke: var(--warn);
    stroke-opacity: 0.8;
  }

  .span {
    fill: var(--warn);
    fill-opacity: 0.1;
  }

  .flag.deploy {
    fill: var(--ink);
  }

  .flag.issue {
    fill: var(--warn);
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

  .tip.wide {
    min-width: 320px;
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

  .tip-row.compare {
    grid-template-columns: 14px 1fr auto auto;
  }

  .tip-row.head {
    grid-template-columns: 14px 1fr auto auto;
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .was {
    color: var(--dim);
    font-variant-numeric: tabular-nums;
    min-width: 5rem;
    text-align: right;
  }

  .tip-mark {
    margin-bottom: 4px;
    padding-left: 8px;
    border-left: 3px solid var(--ink);
  }

  .tip-mark.issue {
    border-left-color: var(--warn);
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
