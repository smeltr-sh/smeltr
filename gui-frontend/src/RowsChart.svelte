<!--
  A source's rows per second, or one of its sender's, against its range over a span, as the leader
  reads them (`throughput::chart`), with the warnings that held then. History draws one around each
  warning, and a sender's page draws one over the chart range.
-->
<script lang="ts">
  import Sparkline from './Sparkline.svelte';
  import {
    errorMessage,
    errorText,
    getSourceChart,
    type Sender,
    type SourceChartResponse,
  } from './api';
  import { expectedSlots } from './focus';

  type Mark = { from: number; to: number | null; label: string; failed: boolean };

  let {
    group,
    source,
    sender,
    fromMillis,
    toMillis,
    marks = [],
    left,
    right,
  }: {
    group: string;
    source: string;
    sender?: Sender;
    fromMillis: number;
    toMillis: number;
    marks?: Mark[];
    /** What the chart's left and right edges are, under it. */
    left: string;
    right: string;
  } = $props();

  let answer = $state<SourceChartResponse | null>(null);
  let problem = $state('');

  /** The newest ask, so an older answer arriving late is dropped. The chart keeps the last answer
   * while the next is read, so a span that moves with now does not blink. */
  let asked = 0;
  $effect(() => {
    const mine = ++asked;
    getSourceChart(group, source, fromMillis, toMillis, sender)
      .then((got) => {
        if (mine !== asked) return;
        answer = got.ok ? got : null;
        problem = got.ok ? '' : errorText(got.errors);
      })
      .catch((e) => {
        if (mine === asked) problem = errorMessage(e);
      });
  });

  /** When each point began, from the one `fromMillis` is in until `toMillis`. */
  let times = $derived.by(() => {
    if (!answer) return [];
    const step = answer.pointMillis;
    const first = Math.floor(fromMillis / step) * step;
    return Array.from(
      { length: Math.max(1, Math.ceil((toMillis - first) / step)) },
      (_, i) => first + i * step,
    );
  });
  let points = $derived.by(() => {
    const at = new Map(answer?.points ?? []);
    return times.map((t) => at.get(t) ?? null);
  });
</script>

{#if problem}
  <p class="problem">{problem}</p>
{:else if !answer}
  <p class="dim small">Reading&hellip;</p>
{:else if answer.points.length === 0}
  <p class="dim small">No rows are kept from then.</p>
{:else}
  <Sparkline
    series={[{ label: 'rows/s', color: 'var(--ink)', points }]}
    band={expectedSlots(answer.expected, times)}
    {times}
    {marks}
  />
  <div class="axis mono dim" aria-hidden="true"><span>{left}</span><span>{right}</span></div>
{/if}

<style>
  .axis {
    display: flex;
    justify-content: space-between;
    margin-top: 0.2rem;
    font-size: var(--text-micro);
  }

  .small {
    font-size: var(--text-label);
  }
</style>
