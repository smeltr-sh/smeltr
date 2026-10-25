<!--
  A value set in a field, with the ✕ that takes it out. A tag list's names and the find box's
  filters are both one.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    removeLabel,
    onremove,
    children,
  }: {
    /** What the ✕ says to a screen reader: `remove …`. */
    removeLabel: string;
    onremove: () => void;
    children: Snippet;
  } = $props();
</script>

<span class="chip"
  >{@render children()}<button
    type="button"
    class="chip-remove"
    aria-label={removeLabel}
    onclick={onremove}>✕</button
  ></span
>

<style>
  .chip {
    display: inline-flex;
    flex: none;
    align-items: center;
    gap: 0.3rem;
    background: color-mix(in srgb, var(--ink) 8%, transparent);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0.1rem 0.25rem 0.1rem 0.55rem;
    white-space: nowrap;
  }

  .chip-remove {
    background: none;
    border: none;
    color: var(--dim);
    font-size: var(--text-micro);
    line-height: 1;
    padding: 0.15rem;
    border-radius: 999px;
    cursor: pointer;
  }

  .chip-remove:hover {
    color: var(--error);
  }
</style>
