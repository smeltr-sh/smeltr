<!--
  An editor's blast radius (user story A5): what a change to this object reaches, on the branch
  being edited, folded to one line. Open, each row names its first few and links the rest as the
  list narrowed to them. A name in another scope opens there.
-->
<script lang="ts">
  import Collapsible from './Collapsible.svelte';
  import {
    getConnections,
    setScope,
    type Connected,
    type ConnectionKind,
    type ConnectionsResponse,
  } from './api';
  import { SHOWN, listHash, mapHash, objectHash, rows, summary, type Row } from './connections';

  let { kind, id }: { kind: ConnectionKind; id: string } = $props();

  let open = $state(false);
  let answer = $state<ConnectionsResponse | null>(null);
  $effect(() => {
    const asked = { kind, id };
    answer = null;
    // A line that cannot be answered is left off: the editor works without it.
    getConnections(asked.kind, asked.id)
      .then((res) => {
        if (res.ok && asked.kind === kind && asked.id === id) answer = res;
      })
      .catch(() => {});
  });

  let all = $derived(answer ? rows(kind, answer) : []);
  let line = $derived(answer ? summary(all, answer.groups) : '');
  let onMap = $derived(answer ? mapHash(kind, id, answer.drawnIn) : undefined);

  /** Follows a link into `scope`. */
  function into(scope: string, hash: string) {
    return (e: MouseEvent) => {
      e.preventDefault();
      setScope(scope, hash);
    };
  }

  /** The rest of a row, one link per scope they sit in, so each opens the list it is on. */
  function rest(row: Row): { scope: string; items: Connected[] }[] {
    const by = new Map<string, Connected[]>();
    for (const o of row.items.slice(SHOWN)) by.set(o.scope, [...(by.get(o.scope) ?? []), o]);
    return [...by].map(([scope, items]) => ({ scope, items }));
  }
</script>

{#if answer}
  <Collapsible bind:open>
    {#snippet summary()}
      <span class="eyebrow">Connections</span>
      <span class="dim line">{line}</span>
    {/snippet}
    <dl class="rows">
      {#each all as row (row.label)}
        <dt>{row.label}</dt>
        <dd>
          {#each row.items.slice(0, SHOWN) as o, i (`${o.scope}\u0000${o.id}`)}
            {#if i > 0}<span class="dim">{', '}</span>{/if}<a
              class="link name"
              href={objectHash(o)}
              onclick={into(o.scope, objectHash(o))}>{o.id}</a
            >
          {/each}
          {#each rest(row) as more (more.scope)}
            <span class="dim">and</span>
            {@const hash = listHash(
              more.items[0].kind,
              more.items.map((o) => o.id),
            )}
            <a class="link" href={hash} onclick={into(more.scope, hash)}>{more.items.length} more</a
            >
          {/each}
        </dd>
      {/each}
      <dt>groups</dt>
      <dd>{answer.groups.join(', ')}</dd>
      {#if onMap}
        <dt></dt>
        <dd><a class="link" href={onMap}>Show on map</a></dd>
      {/if}
    </dl>
  </Collapsible>
{/if}

<style>
  /* Spaced by margin, not the whitespace after it: a summary can be a flex box, which drops that. */
  .eyebrow {
    flex: none;
    margin-right: 0.5rem;
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .line {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .rows {
    display: grid;
    grid-template-columns: max-content 1fr;
    gap: 0.35rem 1.5rem;
    margin: 0;
  }

  dt {
    color: var(--dim);
  }

  dd {
    margin: 0;
  }

  .name {
    font-family: var(--mono);
  }

  .link {
    color: var(--ink);
    text-decoration: underline;
    text-underline-offset: 2px;
  }
</style>
