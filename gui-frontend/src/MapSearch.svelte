<!--
  The map's search box (design doc "The map", Search). Focused, it folds open to the suggestions
  `mapTerms.suggestions` makes for the query as typed: picking a term puts it in the query, picking
  an object selects it. Arrow keys move through the list, Enter picks, Escape folds it.
-->
<script lang="ts">
  import type { MapNode } from './api';
  import { applySuggestion, type Suggestion } from './mapTerms';
  import { float } from './float';

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

  let input: HTMLInputElement | undefined = $state();
  let open = $state(false);
  let active = $state(0);

  let picks = $derived(items.filter((s) => s.kind !== 'none'));
  let shown = $derived(open && items.length > 0);

  function pick(s: Suggestion) {
    if (s.kind === 'term') {
      query = applySuggestion(query, s.value);
      active = 0;
      input?.focus();
    } else if (s.kind === 'node') {
      // Going to an object is the search's answer: a filter left behind could hide it.
      query = '';
      open = false;
      onselect(s.node);
    }
  }

  function onkeydown(e: KeyboardEvent) {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (!open) open = true;
      else active = (active + 1) % Math.max(1, picks.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      active = (active - 1 + picks.length) % Math.max(1, picks.length);
    } else if (e.key === 'Enter' && shown && picks[active]) {
      e.preventDefault();
      pick(picks[active]);
    } else if (e.key === 'Escape' && open) {
      e.preventDefault();
      e.stopPropagation();
      open = false;
    }
  }

  $effect(() => {
    if (active >= picks.length) active = 0;
  });
</script>

<div class="search-wrap">
  <input
    bind:this={input}
    bind:value={query}
    class="search"
    type="text"
    role="combobox"
    aria-expanded={shown}
    aria-controls="map-suggestions"
    aria-autocomplete="list"
    aria-activedescendant={shown && picks[active] ? `map-suggestion-${active}` : undefined}
    aria-label="Search the map"
    placeholder="Search"
    autocomplete="off"
    spellcheck="false"
    onfocus={() => (open = true)}
    onblur={() => (open = false)}
    oninput={() => {
      open = true;
      active = 0;
    }}
    {onkeydown}
  />
  {#if query}
    <button
      type="button"
      class="clear"
      aria-label="Clear the search"
      onclick={() => {
        query = '';
        input?.focus();
      }}>&times;</button
    >
  {/if}
  {#if shown}
    <!-- Pressing anywhere in the list, its scrollbar included, keeps focus in the box. -->
    <ul
      class="suggestions"
      id="map-suggestions"
      role="listbox"
      aria-label="Suggestions"
      popover="manual"
      {@attach float(() => input?.getBoundingClientRect(), { matchWidth: true })}
      onmousedown={(e) => e.preventDefault()}
    >
      {#each items as s, i (i)}
        {#if s.section && s.section !== items[i - 1]?.section}
          <li class="section" role="presentation">{s.section}</li>
        {/if}
        {#if s.kind === 'none'}
          <li class="none" role="presentation">{s.label}</li>
        {:else}
          {@const index = picks.indexOf(s)}
          <li
            id="map-suggestion-{index}"
            role="option"
            aria-selected={index === active}
            class:highlighted={index === active}
            onmousedown={(e) => e.preventDefault()}
            onclick={() => pick(s)}
            onkeydown={(e) => e.key === 'Enter' && pick(s)}
            onmouseenter={() => (active = index)}
          >
            {#if s.kind === 'term'}
              <span class="mono">{s.value}</span>
              <span class="detail">{s.label}{s.detail ? ` · ${s.detail}` : ''}</span>
              <span class="count">{s.count}</span>
            {:else}
              <span class="mono">{s.label}</span>
              <span class="detail">{s.detail}</span>
            {/if}
          </li>
        {/if}
      {/each}
    </ul>
  {/if}
</div>

<style>
  .search-wrap {
    position: relative;
    flex: 1 1 28rem;
    max-width: 32rem;
  }

  .search {
    width: 100%;
    padding-right: 2rem;
    font-family: var(--mono);
  }

  .clear {
    position: absolute;
    top: 50%;
    right: 0.3rem;
    transform: translateY(-50%);
    border: 0;
    background: none;
    padding: 0 0.4rem;
    color: var(--dim);
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }

  .suggestions {
    color: var(--ink);
    max-height: 26rem;
    padding: 0.25rem;
    list-style: none;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.18);
  }

  .suggestions li[role='option'] {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
    gap: 0.6rem;
    align-items: baseline;
    padding: 0.3rem 0.5rem;
    border-radius: var(--radius-card);
    cursor: pointer;
  }

  .suggestions li.highlighted {
    background: color-mix(in srgb, var(--ink) 12%, var(--panel));
  }

  .section {
    padding: 0.5rem 0.5rem 0.2rem;
    font-size: var(--text-micro);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }

  .none {
    padding: 0.4rem 0.5rem;
    font-size: var(--text-label);
    color: var(--dim);
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
