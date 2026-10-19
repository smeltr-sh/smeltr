<!--
  A row of filter chips: All, then one per value, one of them on. A chip may carry a count, a sign,
  and a `?` that explains it; the `?` sits beside the chip's button, inside the pill, so a click on
  it explains without filtering.
-->
<script lang="ts" module>
  import type { Glyph } from './stepGlyphs';

  export type ChipItem<K extends string> = {
    key: K;
    label: string;
    count?: number;
    glyph?: Glyph;
    help?: string;
  };
</script>

<script lang="ts" generics="K extends string">
  import Help from './Help.svelte';

  let {
    items,
    value = $bindable(null),
    label,
    allCount,
    toggles = false,
    onpick,
  }: {
    items: ChipItem<K>[];
    /** The chip on; null is All. */
    value?: K | null;
    /** What the chips filter, for a screen reader. */
    label: string;
    allCount?: number;
    /** A click on the chip already on turns it off, back to All. */
    toggles?: boolean;
    onpick?: (value: K | null) => void;
  } = $props();

  function pick(k: K | null) {
    value = toggles && value === k ? null : k;
    onpick?.(value);
  }
</script>

<div class="chips" role="group" aria-label={label}>
  <span class="chip" class:on={value === null}>
    <button type="button" aria-pressed={value === null} onclick={() => pick(null)}
      >All{#if allCount !== undefined}<span class="n">{allCount}</span>{/if}</button
    >
  </span>
  {#each items as item (item.key)}
    <span class="chip" class:on={value === item.key} class:helped={item.help}>
      <button type="button" aria-pressed={value === item.key} onclick={() => pick(item.key)}
        >{#if item.glyph}<svg viewBox="0 0 64 64" aria-hidden="true"
            ><path
              d={item.glyph.d}
              fill="currentColor"
              fill-rule={item.glyph.evenodd ? 'evenodd' : 'nonzero'}
            /></svg
          >{/if}{item.label}{#if item.count !== undefined}<span class="n">{item.count}</span
          >{/if}</button
      >
      {#if item.help}<Help label={item.label}>{item.help}</Help>{/if}
    </span>
  {/each}
</div>

<style>
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.3rem;
  }

  .chip {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    font-size: var(--text-note);
    color: var(--dim);
  }

  /* Only the button filters, so only it lights the pill. */
  .chip:has(> button:hover) {
    border-color: var(--ink);
  }

  .chip.on {
    border-color: var(--ink);
    color: var(--ink);
    font-weight: 600;
  }

  .chip.helped {
    padding-right: 0.35rem;
  }

  .chip button {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.15rem 0.6rem;
    border: none;
    border-radius: 999px;
    background: none;
    font: inherit;
    color: inherit;
    cursor: pointer;
  }

  .chip.helped button {
    padding-right: 0;
  }

  .chip svg {
    width: 0.95rem;
    height: 0.95rem;
  }

  .n {
    font-weight: 400;
    color: var(--dim);
    font-variant-numeric: tabular-nums;
  }
</style>
