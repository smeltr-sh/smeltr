<!--
  Vendored from `gui-frontend/src/FlowChart.svelte` for the fleet status
  demo; keep it in step: the website builds standalone and does not read
  gui-frontend at build time.

  Rows in and out drawn from one baseline: in above it, out below (`flowChart.ts`).
  The two counts usually agree, and laid over each other whichever was drawn second
  won -- one line hid the other and the panel showed a single color. Two halves
  cannot do that to each other.

  They share a scale, so reach compares between them. They are not expected to match:
  a filter drops rows, a fan-out sends one row to several destinations, a retry sends
  the same row twice (`src/leader/stats.rs`). The distance between the halves is
  ordinary pipeline behavior, not a backlog.

  The svg is sized from its measured width, so one user unit is one pixel. A viewBox
  stretched to fit scales a stroke with it, painting the vertical side of a step
  several times thicker than the horizontal -- which is what turns a chart of bursty
  buckets into a row of blocks. It is also what lets a pointer's x be read straight off
  the screen and turned back into a bucket (`slotAt`).

  The chart marks the bucket under the pointer and names it to its caller. It does not
  write the reading: the caller holds the series it handed over and the ones it did not,
  and the panel already has a line that says what the fleet is doing.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import {
    axisLabel,
    axisStep,
    axisTop,
    axisValues,
    flowArea,
    flowDots,
    flowLine,
    peak,
    slotAt,
    xOf,
    yOf,
    type Half,
  } from './flowChart';

  let {
    inPoints,
    outPoints,
    from,
    to,
    onhover,
  }: {
    inPoints: (number | null)[];
    outPoints: (number | null)[];
    /** The clock at the window's first drawn bucket and at its last. Drawn here rather
     * than by the caller because only this component knows where the plot starts: the
     * gutter is as wide as the labels need, and the data decides those. */
    from: string;
    to: string;
    /** Which bucket the pointer is over, by index into `inPoints` and `outPoints`, and
     * `null` when it is away. */
    onhover?: (slot: number | null) => void;
  } = $props();

  /** The drawn box. `PLOT` holds the two halves, `AXIS` the clock beneath them.
   * `PEAK_PAD` keeps a point at full reach off the top edge, taken from both halves
   * equally so neither is the deeper one. */
  const PLOT = 200;
  const AXIS = 20;
  const HEIGHT = PLOT + AXIS;
  const PEAK_PAD = 10;
  const RIGHT = 2;
  const BASELINE = PLOT / 2;
  const REACH = PLOT / 2 - PEAK_PAD;

  /** A monospace glyph at `--text-micro`, taken at the larger of the two type scales
   * this is drawn in -- the site's 0.75rem over the app's 0.7rem -- and the gap from the
   * labels to the plot. */
  const GLYPH = 7.5;
  const TICK_GAP = 6;

  /** The hover mark, wider than the line it stands on so it reads as a held point. */
  const MARK_R = 3.5;

  let width = $state(0);

  let max = $derived(peak([inPoints, outPoints]));
  let top = $derived(axisTop(max));
  let step = $derived(axisStep(max));

  /** Each grid value's y on both sides of the baseline. 0 lands on the baseline itself,
   * which is one line and one label, not two. */
  let grid = $derived(
    axisValues(max).map((value) => ({
      value,
      label: axisLabel(value, step),
      up: BASELINE - (value / top) * REACH,
      down: BASELINE + (value / top) * REACH,
    })),
  );

  /** Wide enough for the longest label this axis is actually drawing. A fleet moving a
   * row a minute labels its axis `0.00015`, seven characters; one moving thousands
   * labels it `5.0k`, four. A gutter sized for the widest case would spend thirty pixels
   * of plot on every chart that never needs them. */
  let gutter = $derived(Math.max(...grid.map((g) => g.label.length)) * GLYPH + TICK_GAP);
  let plotWidth = $derived(Math.max(0, width - gutter - RIGHT));

  const half = (up: boolean, x: number, w: number, t: number): Half => ({
    x,
    width: w,
    baseline: BASELINE,
    reach: REACH,
    top: t,
    up,
  });

  let upper = $derived(half(true, gutter, plotWidth, top));
  let lower = $derived(half(false, gutter, plotWidth, top));

  /** The two halves are one grid -- one window at one bucket width -- so this is the
   * length they share. Taken as the larger so a side handed nothing cannot collapse the
   * axis the pointer is measured against. */
  let count = $derived(Math.max(inPoints.length, outPoints.length));

  /** The slot the pointer was last over, and the grid it was measured against. */
  let hovered = $state<{ slot: number; of: number } | null>(null);

  /** The bucket under the pointer, dropped once that grid is gone. Changing the window
   * changes both how many buckets there are and what each one spans, so the held index
   * names a different instant and sits at a different x -- the rule would step away from
   * the pointer and the reading would follow it. It comes back on the next move. */
  let slot = $derived(hovered !== null && hovered.of === count ? hovered.slot : null);

  // The caller is told here rather than from the pointer handler, so a window change that
  // drops the slot reaches it too. `onhover` itself is untracked: an inline handler is a
  // new function on every update of the caller, and tracking it would run this on each one.
  $effect(() => {
    const at = slot;
    untrack(() => onhover?.(at));
  });

  let ruleX = $derived(slot === null ? null : xOf(slot, count, upper));
  let inAt = $derived(slot === null ? null : (inPoints[slot] ?? null));
  let outAt = $derived(slot === null ? null : (outPoints[slot] ?? null));

  /** The pointer's x in the svg's own units, which are pixels: the viewBox is the measured
   * width, so nothing is scaled and a box measured on screen reads straight across. */
  function track(event: PointerEvent) {
    const box = (event.currentTarget as SVGRectElement).ownerSVGElement?.getBoundingClientRect();
    if (!box) return;
    const at = slotAt(event.clientX - box.left, count, upper);
    hovered = at === null ? null : { slot: at, of: count };
  }
</script>

<div class="flow-chart" bind:clientWidth={width}>
  {#if plotWidth > 1}
    <svg
      viewBox="0 0 {width} {HEIGHT}"
      preserveAspectRatio="none"
      role="img"
      aria-label="Rows in per second drawn above a baseline and rows out below it, over the chosen window"
    >
      {#each grid as g (g.value)}
        {#if g.value !== 0}
          <line class="grid" x1={gutter} y1={g.up} x2={width - RIGHT} y2={g.up} />
          <line class="grid" x1={gutter} y1={g.down} x2={width - RIGHT} y2={g.down} />
        {/if}
      {/each}

      {#if ruleX !== null}
        <line class="rule" x1={ruleX} y1="0" x2={ruleX} y2={PLOT} />
      {/if}

      <path class="fill in" d={flowArea(inPoints, upper)} />
      <path class="fill out" d={flowArea(outPoints, lower)} />
      <path class="stroke in" d={flowLine(inPoints, upper)} />
      <path class="stroke out" d={flowLine(outPoints, lower)} />
      <path class="dot in" d={flowDots(inPoints, upper)} />
      <path class="dot out" d={flowDots(outPoints, lower)} />

      <line class="base" x1={gutter} y1={BASELINE} x2={width - RIGHT} y2={BASELINE} />

      {#each grid as g (g.value)}
        <text class="tick" x={gutter - TICK_GAP} y={g.up}>{g.label}</text>
        {#if g.value !== 0}
          <text class="tick" x={gutter - TICK_GAP} y={g.down}>{g.label}</text>
        {/if}
      {/each}

      <text class="side" x={gutter + 6} y={BASELINE - 7}>in</text>
      <text class="side" x={gutter + 6} y={BASELINE + 15}>out</text>

      <text class="clock start" x={gutter} y={PLOT + 15}>{from}</text>
      <text class="clock end" x={width - RIGHT} y={PLOT + 15}>{to}</text>

      {#if ruleX !== null && inAt !== null}
        <circle class="mark in" cx={ruleX} cy={yOf(inAt, upper)} r={MARK_R} />
      {/if}
      {#if ruleX !== null && outAt !== null}
        <circle class="mark out" cx={ruleX} cy={yOf(outAt, lower)} r={MARK_R} />
      {/if}

      <!-- Last, so it is over the labels as well as the plot and every move lands here.
           The axis strip below is left out: the clock is not a bucket to point at.

           Presentational, and it carries no keyboard way in, because it adds nothing a
           reader cannot already get: the caller's line reads the last closed bucket
           whenever nothing is hovered, and that line is text. -->
      <rect
        class="surface"
        role="presentation"
        x={gutter}
        y="0"
        width={plotWidth}
        height={PLOT}
        onpointermove={track}
        onpointerleave={() => (hovered = null)}
      />
    </svg>
  {/if}
</div>

<style>
  .flow-chart {
    width: 100%;
    /* The svg is not drawn until the box has been measured. With no height to hold until
       then, everything below the panel is laid out 220px high and drops on first paint. */
    min-height: 220px;
  }

  svg {
    display: block;
    width: 100%;
    height: 220px;
  }

  /* Hairlines whatever the box does, and true 2px lines: a stroke is otherwise
     scaled by the viewBox, and this one's width is measured, not fixed. */
  .grid,
  .base,
  .rule,
  .stroke,
  .dot,
  .mark {
    vector-effect: non-scaling-stroke;
  }

  .grid {
    stroke: var(--border);
    stroke-width: 1;
  }

  .base {
    stroke: var(--dim);
    stroke-width: 1;
  }

  .stroke {
    fill: none;
    stroke-width: 2;
    stroke-linejoin: round;
    stroke-linecap: round;
  }

  .fill {
    fill-opacity: 0.1;
    stroke: none;
  }

  /* A reading standing alone, drawn as a zero-length subpath the round cap turns into a
     circle. Wider than the line so it reads as a point rather than a speck -- it is the
     whole of what that bucket holds. */
  .dot {
    fill: none;
    stroke-width: 5;
    stroke-linecap: round;
  }

  /* A hue is set on the role it belongs to, never on the side alone. `.in` by itself
     weighs the same as `.stroke`, so a fill declared there lands on the line path as
     well, painting the polyline closed and solid over the wash it should sit on. */
  .stroke.in,
  .dot.in {
    stroke: var(--accent);
  }

  .stroke.out,
  .dot.out {
    stroke: var(--counter);
  }

  .fill.in {
    fill: var(--accent);
  }

  .fill.out {
    fill: var(--counter);
  }

  /* Dashed, so it is not read as a second baseline, and under the data: the reading is
     what the pointer is asking about, not the line marking it. */
  .rule {
    stroke: var(--dim);
    stroke-width: 1;
    stroke-dasharray: 3 3;
  }

  /* Filled with the panel's own background, so the line it stands on does not run through
     it and the mark reads as one point rather than a bead on a wire. */
  .mark {
    fill: var(--panel);
    stroke-width: 2;
  }

  .mark.in {
    stroke: var(--accent);
  }

  .mark.out {
    stroke: var(--counter);
  }

  /* `fill: transparent` is painted, so it takes the pointer where `none` would let it
     through to whatever is underneath. */
  .surface {
    fill: transparent;
    pointer-events: all;
    cursor: crosshair;
  }

  .tick {
    fill: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-micro);
    text-anchor: end;
    dominant-baseline: middle;
  }

  .clock {
    fill: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-micro);
  }

  .start {
    text-anchor: start;
  }

  .end {
    text-anchor: end;
  }

  /* Drawn inside the plot, where a fill can run under them. The halo is the panel's own
     background, so the text sits on a line without cutting it. */
  .side {
    fill: var(--dim);
    font-size: var(--text-micro);
    paint-order: stroke;
    stroke: var(--panel);
    stroke-width: 3px;
    stroke-linejoin: round;
  }
</style>
