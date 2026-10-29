<!--
  The ✕ control (STYLE_GUIDE.md, Glyphs): `CrossIcon`, dim until hovered, on a 24px target. With `removes` it takes
  an item out of the list it is in, which Save makes stick, so it reddens on hover; without, it closes or clears what
  is on screen and saves nothing. `RemovableChip` draws its own, at the chip's smaller size.
-->
<script lang="ts">
  import CrossIcon from './CrossIcon.svelte';

  let {
    label,
    removes = false,
    onclick,
    href,
  }: {
    /** What it does, for a screen reader and the hover title: `Remove row 3`, `Close`. */
    label: string;
    removes?: boolean;
    onclick?: (e: MouseEvent) => void;
    /** Where it leaves to, when closing is a navigation; it is then a link. */
    href?: string;
  } = $props();
</script>

{#if href}
  <a class={['cross', removes && 'removes']} {href} aria-label={label} title={label} {onclick}
    ><CrossIcon /></a
  >
{:else}
  <button
    type="button"
    class={['cross', removes && 'removes']}
    aria-label={label}
    title={label}
    {onclick}><CrossIcon /></button
  >
{/if}

<style>
  .cross {
    display: inline-grid;
    flex: none;
    place-items: center;
    /* The 24px target a pointer needs (WCAG 2.5.8). */
    min-width: 1.5rem;
    min-height: 1.5rem;
    padding: 0.2rem;
    border: 1px solid transparent;
    background: none;
    color: var(--dim);
    font-size: var(--text-label);
    line-height: 1;
    text-decoration: none;
  }

  .cross:hover {
    color: var(--ink);
  }

  .removes:hover {
    color: var(--error);
  }
</style>
