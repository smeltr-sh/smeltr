<!--
  Where a stream starts reading: the source form's start, and a capture's. Each says what reading
  from nowhere in particular means to it, so `fallback` is what an absent `start` is, and choosing it
  writes nothing.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import Field from './Field.svelte';
  import type { StartPosition } from './kafka';
  import { fromLocalInput, toLocalInput } from './time';

  let {
    start = $bindable(),
    label,
    fallback,
    help,
    note: said,
  }: {
    start: StartPosition | undefined;
    /** In the case of the form it sits in. */
    label: string;
    /** What an absent `start` reads from. */
    fallback: 'earliest' | 'latest';
    /** The label's `?`. */
    help: Snippet;
    /** Under the field, in place of the default it reads from. */
    note?: Snippet;
  } = $props();

  let kind = $derived(start?.type ?? fallback);

  function choose(next: string) {
    if (next === fallback) start = undefined;
    else if (next === 'earliest') start = { type: 'earliest' };
    else if (next === 'latest') start = { type: 'latest' };
    else start = { type: 'timestamp', millis: Date.now() };
  }
</script>

<div class="pair">
  <Field {label} {help}>
    {#snippet note()}
      {#if said}
        {@render said()}
      {:else}
        Default <code>{fallback}</code>.
      {/if}
    {/snippet}
    <select value={kind} onchange={(e) => choose(e.currentTarget.value)}>
      <option value="earliest">earliest</option>
      <option value="latest">latest</option>
      <option value="timestamp">a time</option>
    </select>
  </Field>
  {#if start?.type === 'timestamp'}
    <Field label="from" note="Your own clock.">
      <input
        type="datetime-local"
        value={toLocalInput(start.millis)}
        oninput={(e) => {
          const millis = fromLocalInput(e.currentTarget.value);
          if (millis !== undefined) start = { type: 'timestamp', millis };
          // A cleared field shows the time it still holds: what is sent is what is shown.
          else if (start?.type === 'timestamp') e.currentTarget.value = toLocalInput(start.millis);
        }}
      />
    </Field>
  {/if}
</div>

<style>
  .pair {
    display: flex;
    gap: 0.8rem;
    align-items: flex-start;
  }

  .pair > :global(*) {
    flex: 1;
  }
</style>
