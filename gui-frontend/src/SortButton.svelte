<!--
  The control inside a sortable column head (`sort.svelte.ts`): the head's label is the button, and the
  sorted one says which way. The head itself stays the caller's `th`, so the table's own head styles
  reach it; the caller puts `aria-sort={sort.aria(key)}` on it, where a screen reader looks.
-->
<script lang="ts" generics="K extends string">
  import type { Snippet } from 'svelte';
  import type { TableSort } from './sort.svelte';

  let { sort, key, children }: { sort: TableSort<K>; key: K; children: Snippet } = $props();

  const sorted = $derived(sort.key === key);
</script>

<button type="button" class="sort-head" class:sorted onclick={() => sort.toggle(key)}
  >{@render children()}<span class="arrow" aria-hidden="true"
    >{sorted ? (sort.dir === 1 ? '↑' : '↓') : ''}</span
  >{#if sorted}<span class="state">, sorted {sort.dir === 1 ? 'ascending' : 'descending'}</span
    >{/if}</button
>

<style>
  /* Wears its column head's own face: the head is a label first and a control second. */
  .sort-head {
    display: inline-flex;
    align-items: baseline;
    gap: 0.25rem;
    max-width: 100%;
    padding: 0;
    margin: 0;
    border: none;
    background: none;
    font: inherit;
    letter-spacing: inherit;
    text-transform: inherit;
    text-decoration: inherit;
    text-align: inherit;
    color: inherit;
    cursor: pointer;
  }

  /* A glyph before the name centers on its line, as the words sit on their baseline. */
  .sort-head :global(svg) {
    align-self: center;
  }

  .sort-head:hover,
  .sort-head.sorted {
    color: var(--ink);
  }

  .sort-head:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
    border-radius: var(--radius);
  }

  /* The arrow's meaning, for a screen reader: a head outside a table has no `aria-sort` to say it. */
  .state {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip-path: inset(50%);
    white-space: nowrap;
  }

  /* Holds its width when empty, so a column does not shift as its head is sorted. */
  .arrow {
    display: inline-block;
    min-width: 0.7em;
  }
</style>
