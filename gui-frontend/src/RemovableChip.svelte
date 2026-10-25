<!--
  A value set in a field, with the ✕ that takes it out. A tag list's names and the find box's
  filters are both one, and so is where the workspace's records stand, in the accent.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let {
    removeLabel,
    onremove,
    label,
    accent = false,
    children,
  }: {
    /** What the ✕ says to a screen reader: `remove …`. */
    removeLabel: string;
    /** Without it the chip has nothing to take out, and no ✕. */
    onremove?: () => void;
    /** What the chip says to a screen reader, when its text is not enough. */
    label?: string;
    /** A state the page is in rather than a value: square, in the accent. */
    accent?: boolean;
    children: Snippet;
  } = $props();
</script>

<span class={['chip', accent && 'accent']} aria-label={label}
  >{@render children()}{#if onremove}<button
      type="button"
      class="chip-remove"
      aria-label={removeLabel}
      title={removeLabel}
      onclick={onremove}>✕</button
    >{/if}</span
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

  .chip.accent {
    padding: 0.3rem 0.6rem;
    border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
    border-radius: var(--radius);
    background: color-mix(in srgb, var(--accent) 10%, transparent);
    color: var(--accent);
  }

  .chip.accent .chip-remove:not(:hover) {
    color: inherit;
  }

  .chip.accent .chip-remove {
    font-size: inherit;
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
