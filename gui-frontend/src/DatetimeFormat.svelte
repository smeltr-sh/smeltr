<!--
  A datetime's `parseTime` format, folded while it is empty: an empty format detects an epoch by its magnitude or one
  of the common text shapes, per row, which is what nearly every field wants.
-->
<script lang="ts">
  import { untrack } from 'svelte';

  let { format = $bindable(), disabled = false }: { format: string; disabled?: boolean } = $props();

  // Read once: a format already set is shown until it is folded by hand, and clearing it does not fold it away
  // under the author's caret.
  let open = $state(untrack(() => format !== ''));
</script>

<div class="datetime-format">
  <button
    type="button"
    class="fmt-toggle"
    class:custom={format !== ''}
    aria-expanded={open}
    title="Empty reads an epoch by its magnitude or one of the common text shapes, per row. Open to give a parseTime format."
    onclick={() => (open = !open)}>{format === '' ? 'auto' : 'format'} {open ? '▾︎' : '▸︎'}</button
  >
  {#if open}
    <input
      {disabled}
      type="text"
      aria-label="format"
      placeholder="e.g. %Y/%m/%d %H:%M:%S"
      title="A parseTime format. Empty goes back to auto."
      bind:value={format}
    />
  {/if}
</div>

<style>
  .datetime-format {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  .fmt-toggle {
    background: none;
    border: 1px dashed var(--border);
    border-radius: var(--radius);
    color: var(--dim);
    font-family: var(--mono);
    font-size: var(--text-micro);
    padding: 0.1rem 0.45rem;
    cursor: pointer;
    white-space: nowrap;
  }

  .fmt-toggle.custom {
    color: var(--ink);
    border-style: solid;
    border-color: var(--ink);
  }

  input {
    flex: 1;
    min-width: 0;
    font-size: var(--text-note);
  }
</style>
