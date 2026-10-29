<!--
  The ✕ control (STYLE_GUIDE.md, Glyphs): `CrossIcon` on an `.icon` button. With `removes` it takes
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
  <a class={['btn', 'icon', removes && 'removes']} {href} aria-label={label} title={label} {onclick}
    ><CrossIcon /></a
  >
{:else}
  <button
    type="button"
    class={['icon', removes && 'removes']}
    aria-label={label}
    title={label}
    {onclick}><CrossIcon /></button
  >
{/if}

<style>
  .icon {
    flex: none;
  }

  .removes:hover {
    color: var(--error);
  }
</style>
