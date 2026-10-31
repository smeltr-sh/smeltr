<!--
  A Remove step's fields: the record before the step as a tree, each field and object ticked while it is kept. Unticking
  an object removes everything under it, so its fields show struck through with their ticks locked. Remove all and Keep
  all act on what the search shows, as the Fields menu's Select all does. A name that matches no field can still be
  removed, from a link under the search box.
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import type { DictionaryOut } from './api';
  import { declaredRecord, type Declared } from './recordDiff';
  import { keySpans, pushKey } from './names';

  let {
    fields = $bindable(),
    record,
    readOnly,
  }: {
    /** The names the step removes, in the order the record holds them. */
    fields: string[];
    /** The record before the step, as the preview's data dictionary lists it. */
    record?: () => Promise<DictionaryOut | null>;
    readOnly: boolean;
  } = $props();

  type Node = { path: string; key: string; depth: number; children: Node[] };

  function nodes(d: Declared, prefix: string, depth: number): Node[] {
    if (d.kind !== 'struct') return [];
    return [...d.fields].map(([key, child]) => {
      const path = pushKey(prefix, key);
      return { path, key, depth, children: nodes(child, path, depth + 1) };
    });
  }

  let roots = $state<Node[]>([]);
  let query = $state('');
  let collapsed = $state<Set<string>>(new Set());

  // Asked again each time the steps ahead of it settle; the answer before stays until the next comes.
  $effect(() => {
    const ask = record;
    let current = true;
    untrack(() => ask?.())?.then(
      (d) => {
        // The sample's side also holds the keys only its rows know, which a parser read without declaring.
        const listed = d?.sampled ?? d?.compiled;
        if (current && listed) roots = nodes(declaredRecord(listed.fields), '', 0);
      },
      // With no answer the list is the step's own fields; the search box still removes by name.
      () => {},
    );
    return () => {
      current = false;
    };
  });

  function flat(ns: Node[]): Node[] {
    return ns.flatMap((n) => [n, ...flat(n.children)]);
  }
  let tree = $derived(flat(roots));
  let known = $derived(new Set(tree.map((n) => n.path)));
  // A removed name the record does not list stays a row of its own, so it can be kept again.
  let all = $derived([
    ...tree,
    ...fields
      .filter((f) => !known.has(f))
      .map((path) => ({ path, key: path, depth: 0, children: [] })),
  ]);
  let removed = $derived(new Set(fields));

  /** The names above `path`, nearest last. */
  function ancestors(path: string): string[] {
    const keys = keySpans(path).map((k) => k.key);
    return keys.slice(0, -1).map((_, i) => keys.slice(0, i + 1).reduce(pushKey, ''));
  }
  /** The removed object `path` goes with, if any. */
  function removedWith(path: string): string | undefined {
    return ancestors(path).find((a) => removed.has(a));
  }
  function gone(path: string): boolean {
    return removed.has(path) || removedWith(path) !== undefined;
  }
  function someInside(n: Node): boolean {
    return n.children.some((c) => removed.has(c.path) || someInside(c));
  }
  function leaves(n: Node): Node[] {
    return n.children.length ? n.children.flatMap(leaves) : [n];
  }
  let allLeaves = $derived(all.filter((n) => !n.children.length));
  let goneLeaves = $derived(allLeaves.filter((n) => gone(n.path)).length);

  /** Removes or keeps each of `names`. A removed object takes what is under it, so a name under it leaves the list. */
  function set(names: string[], remove: boolean) {
    let next = new Set(fields);
    for (const n of names) {
      if (remove) next.add(n);
      else for (const f of next) if (f === n || ancestors(f).includes(n)) next.delete(f);
    }
    next = new Set([...next].filter((f) => !ancestors(f).some((a) => next.has(a))));
    const order = all.map((n) => n.path);
    fields = [...order, ...names].filter((f, i, every) => next.has(f) && every.indexOf(f) === i);
  }

  let q = $derived(query.trim().toLowerCase());
  let typed = $derived(query.trim());
  let matches = $derived(q ? all.filter((n) => n.path.toLowerCase().includes(q)) : all);
  /** The rows shown: under a search, each match and the objects above it; otherwise all but what a closed object hides. */
  let rows = $derived.by(() => {
    if (q) {
      const keep = new Set(matches.flatMap((m) => [...ancestors(m.path), m.path]));
      return all.filter((n) => keep.has(n.path));
    }
    return all.filter((n) => !ancestors(n.path).some((a) => collapsed.has(a)));
  });

  function toggleOpen(path: string) {
    const next = new Set(collapsed);
    if (!next.delete(path)) next.add(path);
    collapsed = next;
  }

  /** `name` around the search's match, so the match can be marked. */
  function parts(name: string): [string, string, string] {
    const at = q ? name.toLowerCase().indexOf(q) : -1;
    if (at < 0) return [name, '', ''];
    return [name.slice(0, at), name.slice(at, at + q.length), name.slice(at + q.length)];
  }

  function status(n: Node): string {
    const with_ = removedWith(n.path);
    if (with_) return `removed with ${keySpans(with_).at(-1)?.key ?? with_}`;
    if (removed.has(n.path)) return 'removed';
    return someInside(n) ? 'some removed' : 'kept';
  }
</script>

<div class="picker">
  <input type="search" bind:value={query} placeholder="Find a field" aria-label="Find a field" />
  {#if !readOnly}
    <div class="actions">
      {#if matches.length}
        <button
          type="button"
          onclick={() => set(q ? matches.map((n) => n.path) : roots.map((n) => n.path), true)}
          >Remove all{typed ? ` ${matches.length}` : ''}</button
        >
        <button
          type="button"
          onclick={() =>
            set(
              matches.map((n) => n.path),
              false,
            )}>Keep all{typed ? ` ${matches.length}` : ''}</button
        >
      {/if}
      <span class="dim">{goneLeaves} of {allLeaves.length} fields removed</span>
    </div>
  {/if}
  <div class="list" role="group" aria-label="Fields">
    {#each rows as n (n.path)}
      {@const under = removedWith(n.path)}
      {@const [before, match, after] = parts(n.key)}
      {@const count = leaves(n).length}
      <div class="row" class:under style:padding-left="{0.25 + n.depth * 1.1}rem">
        {#if n.children.length}
          <button
            type="button"
            class="twisty"
            aria-label={`${collapsed.has(n.path) ? 'Open' : 'Close'} ${n.path}`}
            aria-expanded={!collapsed.has(n.path)}
            onclick={() => toggleOpen(n.path)}>{collapsed.has(n.path) && !q ? '▸' : '▾'}</button
          >
        {:else}
          <span></span>
        {/if}
        <label class="tick">
          <input
            type="checkbox"
            checked={!gone(n.path)}
            indeterminate={!gone(n.path) && someInside(n)}
            disabled={readOnly || !!under}
            onchange={(e) => set([n.path], !e.currentTarget.checked)}
          />
          <span class="mono name"
            >{before}<mark>{match}</mark>{after}{#if n.children.length}<span class="dim"
                >&nbsp;· {count} {count === 1 ? 'field' : 'fields'}</span
              >{/if}</span
          >
        </label>
        <span class="dim">{status(n)}</span>
      </div>
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
    max-height: 20rem;
    overflow-y: auto;
    border-top: 1px solid var(--border);
  }

  .row {
    display: grid;
    grid-template-columns: 1rem minmax(0, 1fr) auto;
    align-items: center;
    gap: 0.4rem;
    padding: 0.1rem 0.25rem;
    font-size: var(--text-label);
  }

  .tick {
    display: grid;
    grid-template-columns: 1.2rem minmax(0, 1fr);
    align-items: center;
    gap: 0.4rem;
    margin: 0;
    font-weight: 400;
    cursor: pointer;
  }

  .under .name {
    color: var(--dim);
    text-decoration: line-through;
  }

  .twisty {
    border: 0;
    background: none;
    padding: 0;
    color: var(--dim);
    cursor: pointer;
    font: inherit;
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
