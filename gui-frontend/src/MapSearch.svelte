<!--
  The map's search box (design doc "The map", Search). Focused, it folds open to the suggestions
  `mapTerms.suggestions` makes for the query as typed: picking a term puts it in the query, picking
  an object selects it.
-->
<script lang="ts">
  import type { MapNode } from './api';
  import { applySuggestion, type Suggestion } from './mapTerms';
  import SearchBox from './SearchBox.svelte';

  let {
    query = $bindable(),
    items,
    onselect,
  }: {
    query: string;
    /** What the box offers for `query`, in order, grouped by `section`. */
    items: Suggestion[];
    /** An object was picked from the list. */
    onselect: (node: MapNode) => void;
  } = $props();

  function pick(s: Suggestion): boolean {
    if (s.kind === 'term') {
      query = applySuggestion(query, s.value);
      return false;
    }
    if (s.kind === 'node') {
      // Going to an object is the search's answer: a filter left behind could hide it.
      query = '';
      onselect(s.node);
    }
    return true;
  }
</script>

<div class="map-search">
  <SearchBox
    bind:query
    {items}
    pickable={(s) => s.kind !== 'none'}
    onpick={pick}
    id="map-suggestion"
    label="Search the map"
    placeholder="Search"
    mono
  >
    {#snippet option(s)}
      {#if s.kind === 'term'}
        <span class="mono">{s.value}</span>
        <span class="detail">{s.label}{s.detail ? ` · ${s.detail}` : ''}</span>
        <span class="count">{s.count}</span>
      {:else if s.kind === 'node'}
        <span class="mono">{s.label}</span>
        <span class="detail">{s.detail}</span>
      {:else}
        {s.label}
      {/if}
    {/snippet}
  </SearchBox>
</div>

<style>
  .map-search {
    display: flex;
    flex: 1 1 28rem;
    max-width: 32rem;
  }

  .mono {
    font-family: var(--mono);
    font-size: var(--text-label);
    color: var(--ink);
  }

  .detail {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: var(--text-label);
    color: var(--dim);
  }

  .count {
    font-size: var(--text-micro);
    color: var(--dim);
  }
</style>
