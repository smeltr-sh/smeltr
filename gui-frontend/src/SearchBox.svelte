<!--
  A search box that, focused, folds open to what it offers for the text as typed. Arrow keys move
  through the list, Enter picks, Escape folds it. The map's search and the workspace's find box
  are both one.
-->
<script lang="ts" generics="T extends { section?: string }">
  import type { Snippet } from 'svelte';
  import { float } from './float';

  let {
    query = $bindable(),
    items,
    pickable = () => true,
    onpick,
    onleave,
    id,
    label,
    placeholder,
    mono = false,
    before,
    after,
    option,
  }: {
    query: string;
    /** What the box offers, in order, grouped by `section`. */
    items: T[];
    /** One that cannot be picked is a note in the list. */
    pickable?: (item: T) => boolean;
    /** An item was picked. True folds the list; otherwise it stays open for another pick. */
    onpick: (item: T) => boolean | void;
    /** Focus left the box: whatever is typed is what was meant. */
    onleave?: () => void;
    /** Unique on the page: the list's options are named from it. */
    id: string;
    label: string;
    placeholder: string;
    /** The text is code. */
    mono?: boolean;
    /** Inside the box, ahead of the text: what already narrows the search. */
    before?: Snippet;
    /** Inside the box, after the text. */
    after?: Snippet;
    /** An item's row in the list. */
    option: Snippet<[T]>;
  } = $props();

  let input: HTMLInputElement | undefined = $state();
  let field: HTMLLabelElement | undefined = $state();
  let open = $state(false);
  let active = $state(0);

  let picks = $derived(items.filter(pickable));
  let shown = $derived(open && items.length > 0);

  function pick(item: T) {
    active = 0;
    if (onpick(item) === true) open = false;
    else input?.focus();
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
  <label class="field" bind:this={field}>
    {@render before?.()}
    <input
      bind:this={input}
      bind:value={query}
      class:mono
      type="text"
      role="combobox"
      aria-expanded={shown}
      aria-controls="{id}-list"
      aria-autocomplete="list"
      aria-activedescendant={shown && picks[active] ? `${id}-${active}` : undefined}
      aria-label={label}
      {placeholder}
      autocomplete="off"
      spellcheck="false"
      onfocus={() => (open = true)}
      onblur={() => {
        open = false;
        onleave?.();
      }}
      oninput={() => {
        open = true;
        active = 0;
      }}
      {onkeydown}
    />
    {@render after?.()}
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
  </label>
  {#if shown}
    <!-- Pressing anywhere in the list, its scrollbar included, keeps focus in the box. -->
    <ul
      class="suggestions"
      id="{id}-list"
      role="listbox"
      aria-label="Suggestions"
      popover="manual"
      {@attach float(() => field?.getBoundingClientRect(), { matchWidth: true })}
      onmousedown={(e) => e.preventDefault()}
    >
      {#each items as item, i (i)}
        {#if item.section && item.section !== items[i - 1]?.section}
          <li class="section" role="presentation">{item.section}</li>
        {/if}
        {#if !pickable(item)}
          <li class="none" role="presentation">{@render option(item)}</li>
        {:else}
          {@const index = picks.indexOf(item)}
          <li
            id="{id}-{index}"
            role="option"
            aria-selected={index === active}
            class:highlighted={index === active}
            onclick={() => pick(item)}
            onkeydown={(e) => e.key === 'Enter' && pick(item)}
            onmouseenter={() => (active = index)}
          >
            {@render option(item)}
          </li>
        {/if}
      {/each}
    </ul>
  {/if}
</div>

<style>
  /* Room for a few words and the mode beside them, and no narrower: a caller's row wraps first. */
  .search-wrap {
    flex: 1;
    min-width: 18rem;
  }

  /* The box is the label, so what sits in it beside the text reads as part of it. */
  .field {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--field);
    padding: 0 0.3rem 0 0.55rem;
    color: var(--dim);
    font-weight: 400;
  }

  .field:focus-within {
    border-color: var(--accent);
  }

  input {
    flex: 1;
    min-width: 0;
    border: 0;
    outline: 0;
    background: transparent;
    padding: 0.45rem 0;
  }

  input.mono {
    font-family: var(--mono);
  }

  .clear {
    flex: none;
    border: 0;
    background: none;
    padding: 0 0.3rem;
    color: var(--dim);
    font-size: 1.1rem;
    line-height: 1;
    cursor: pointer;
  }

  /* Placed by `float`, over whatever box holds the search. */
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
</style>
