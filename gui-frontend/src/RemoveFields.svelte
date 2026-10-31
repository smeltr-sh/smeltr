<!--
  A Remove step's fields: the fields declared before the step, each ticked while it is kept. Remove all and Keep all
  act on what the search shows, as the Fields menu's Select all does. A name that matches no field can still be
  removed, from a link under the search box.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import type { DotAnswer } from './api';

  let {
    fields = $bindable(),
    ask,
    readOnly,
  }: {
    /** The names the step removes, in the order the record holds them. */
    fields: string[];
    /** Asks what may follow a `.` at the step's start (`AskAt`); its answer after `$` is what the record holds. */
    ask?: (text: string, dot: number) => Promise<DotAnswer | null>;
    readOnly: boolean;
  } = $props();

  let held = $state<string[]>([]);
  let query = $state('');

  // Asked once, as the step opens: the step's own edits do not change what the record holds where it starts.
  $effect(() => {
    let current = true;
    untrack(() => ask?.('$.', 1))?.then(
      (answer) => {
        if (current && answer) held = answer.fields.map(([name]) => name);
      },
      // With no answer the list is the step's own fields; the search box still removes by name.
      () => {},
    );
    return () => {
      current = false;
    };
  });

  let listed = $derived([...held, ...fields.filter((f) => !held.includes(f))]);
  let matches = $derived.by(() => {
    const q = query.trim().toLowerCase();
    return q ? listed.filter((f) => f.toLowerCase().includes(q)) : listed;
  });
  let removed = $derived(new Set(fields));
  let typed = $derived(query.trim());

  /** Removes or keeps each of `names`, keeping `fields` in the order of `listed`. */
  function set(names: string[], remove: boolean) {
    const next = new Set(fields);
    for (const n of names) {
      if (remove) next.add(n);
      else next.delete(n);
    }
    fields = [...listed, ...names].filter((f, i, all) => next.has(f) && all.indexOf(f) === i);
  }

  /** `name` around the search's match, so the match can be marked. */
  function parts(name: string): [string, string, string] {
    const q = query.trim().toLowerCase();
    const at = q ? name.toLowerCase().indexOf(q) : -1;
    if (at < 0) return [name, '', ''];
    return [name.slice(0, at), name.slice(at, at + q.length), name.slice(at + q.length)];
  }
</script>

<div class="picker">
  <input type="search" bind:value={query} placeholder="Find a field" aria-label="Find a field" />
  {#if !readOnly}
    <div class="actions">
      {#if matches.length}
        <button type="button" onclick={() => set(matches, true)}
          >Remove all{typed ? ` ${matches.length}` : ''}</button
        >
        <button type="button" onclick={() => set(matches, false)}
          >Keep all{typed ? ` ${matches.length}` : ''}</button
        >
      {/if}
      <span class="dim">{fields.length} of {listed.length} removed</span>
    </div>
  {/if}
  <div class="list" role="group" aria-label="Fields">
    {#each matches as f (f)}
      {@const [before, match, after] = parts(f)}
      <label class="row">
        <input
          type="checkbox"
          checked={!removed.has(f)}
          disabled={readOnly}
          onchange={(e) => set([f], !e.currentTarget.checked)}
        />
        <span class="mono name">{before}<mark>{match}</mark>{after}</span>
        <span class="dim">{removed.has(f) ? 'removed' : 'kept'}</span>
      </label>
    {/each}
    {#if typed && !matches.length && !readOnly}
      <button
        type="button"
        class="link-button add"
        onclick={() => (set([typed], true), (query = ''))}
        >Remove <span class="mono">{typed}</span></button
      >
    {/if}
  </div>
</div>

<style>
  .mono {
    font-family: var(--mono);
  }

  .dim {
    color: var(--dim);
    font-size: var(--text-label);
  }

  .picker {
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
    padding: 0.5rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
  }

  .actions {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  .list {
    max-height: 14rem;
    overflow-y: auto;
    border-top: 1px solid var(--border);
  }

  .row {
    display: grid;
    grid-template-columns: 1.2rem minmax(0, 1fr) auto;
    align-items: center;
    gap: 0.4rem;
    margin: 0;
    padding: 0.1rem 0.25rem;
    font-weight: 400;
    font-size: var(--text-label);
    cursor: pointer;
  }

  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  mark {
    background: color-mix(in srgb, var(--accent) 25%, transparent);
    color: inherit;
  }

  .add {
    padding: 0.25rem;
    font-size: var(--text-label);
  }
</style>
