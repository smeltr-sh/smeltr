<!--
  A small calculator beside a Kafka destination's buffer: what the worker would make of that buffer
  over a link, worked out here before anything is deployed (`linkEstimate.ts`, a mirror of the
  worker's `derive_sizes`). An estimate, and it says so: the worker measures the round trip and asks
  the broker for its ceiling at deployment, and those two figures are typed here.

  The same native `popover` as `Help.svelte`, placed under its own button by `float.ts`.
-->
<script module lang="ts">
  let seq = 0;
</script>

<script lang="ts">
  import { bytes, parseBytes, exactBytes } from './bytes';
  import { DEFAULT_BROKER_CEILING, estimateLink } from './linkEstimate';
  import CalculatorIcon from './CalculatorIcon.svelte';
  import { follow } from './float';

  /** The buffer the field beside this holds, default filled in. Re-read each time the popover opens. */
  let { bufferBytes }: { bufferBytes: number } = $props();

  const id = `estimate-${++seq}`;
  let ask: HTMLButtonElement;
  let pop: HTMLDivElement;

  let bufferText = $state('');
  let roundTripMs = $state(100);
  let partitions = $state(12);
  let ceilingText = $state('');

  let buffer = $derived(parseBytes(bufferText) ?? bufferBytes);
  let ceiling = $derived(parseBytes(ceilingText) ?? DEFAULT_BROKER_CEILING);
  let estimate = $derived(
    estimateLink({ bufferBytes: buffer, roundTripMs, partitions, brokerCeiling: ceiling }),
  );

  let stop: (() => void) | undefined;

  function number(e: Event, fallback: number): number {
    const n = Number((e.currentTarget as HTMLInputElement).value);
    return Number.isFinite(n) && n >= 0 ? n : fallback;
  }
</script>

<button
  bind:this={ask}
  type="button"
  class="ask"
  popovertarget={id}
  aria-label="Estimate what this buffer pushes over a link"
  title="Estimate throughput"><CalculatorIcon /></button
>
<div
  bind:this={pop}
  {id}
  popover
  class="pop"
  onbeforetoggle={(e) => {
    stop?.();
    stop = undefined;
    if ((e as ToggleEvent).newState !== 'open') return;
    bufferText = exactBytes(bufferBytes);
    stop = follow(pop, () => ask.getBoundingClientRect()).stop;
  }}
>
  <p class="lead">
    An estimate of what one worker pushes over this link. At deployment the worker measures the real
    round trip and asks the brokers for their largest request.
  </p>
  <div class="inputs">
    <label>
      <span>buffer</span>
      <input
        type="text"
        inputmode="decimal"
        spellcheck="false"
        value={bufferText}
        oninput={(e) => (bufferText = e.currentTarget.value)}
      />
    </label>
    <label>
      <span>round trip (ms)</span>
      <input
        type="number"
        min="1"
        value={roundTripMs}
        oninput={(e) => (roundTripMs = number(e, roundTripMs))}
      />
    </label>
    <label>
      <span>partitions</span>
      <input
        type="number"
        min="1"
        value={partitions}
        oninput={(e) => (partitions = number(e, partitions))}
      />
    </label>
    <label>
      <span>broker’s largest request</span>
      <input
        type="text"
        inputmode="decimal"
        spellcheck="false"
        placeholder={bytes(DEFAULT_BROKER_CEILING)}
        value={ceilingText}
        oninput={(e) => (ceilingText = e.currentTarget.value)}
      />
    </label>
  </div>
  <dl class="out">
    <dt>ceiling per worker</dt>
    <dd class="mono lead-figure">{bytes(estimate.ceilingPerSecond)}/s</dd>
    <dt>in flight</dt>
    <dd class="mono">{bytes(estimate.producerBytes)}</dd>
    <dt>batch</dt>
    <dd class="mono">{bytes(estimate.batchBytes)}</dd>
    <dt>linger</dt>
    <dd class="mono">{estimate.lingerMs.toFixed(0)} ms</dd>
    <dt>requests out</dt>
    <dd class="mono">{estimate.requestsInFlight}</dd>
  </dl>
  <p class="foot">
    Half the buffer is in flight at a time. Past the point where the brokers throttle, a bigger
    buffer adds no throughput.
  </p>
</div>

<style>
  /* The same footprint as the `?` beside it, so the two sit on one line at one height. */
  .ask {
    flex: none;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 1.15em;
    height: 1.15em;
    padding: 0;
    margin-left: 0.3rem;
    border: 0;
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    font-size: var(--text-note);
    cursor: pointer;
  }

  .ask :global(svg) {
    width: 1em;
    height: 1em;
  }

  .ask:hover,
  .ask:focus-visible {
    color: var(--ink);
  }

  .pop {
    width: min(22rem, calc(100vw - 1rem));
    padding: 0.7rem 0.85rem 0.8rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    color: var(--ink);
    font-size: var(--text-note);
    font-weight: 400;
    line-height: 1.5;
    box-shadow: 0 4px 14px rgb(0 0 0 / 0.18);
  }

  .lead,
  .foot {
    margin: 0;
    color: var(--ink);
  }

  .inputs {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.45rem 0.6rem;
    margin: 0.6rem 0;
  }

  .inputs label {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .inputs input {
    width: 100%;
    box-sizing: border-box;
    font-family: var(--mono);
  }

  .out {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 0.15rem 0.75rem;
    margin: 0 0 0.6rem;
    padding-top: 0.5rem;
    border-top: 1px solid var(--border);
  }

  .out dt {
    color: var(--dim);
  }

  .out dd {
    margin: 0;
    text-align: right;
  }

  .lead-figure {
    font-size: var(--text-section);
    font-weight: 600;
  }

  .mono {
    font-family: var(--mono);
    font-variant-numeric: tabular-nums;
  }
</style>
