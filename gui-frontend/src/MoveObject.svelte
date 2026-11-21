<!--
  Moving a source or a destination into another area, in one commit: a source takes the routes that start at it. A
  destination takes none, since a route belongs to the source it starts at, so the routes that end at it stay behind.
  It shows where the object runs before and after, so the person sees which groups stop or start running it. The
  leader refuses a move that leaves something behind still using it, and the refusal shows in the dialog.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY, SAVE_FIRST } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import {
    SHARED_SCOPE,
    configAt,
    currentBranch,
    currentScope,
    getConnections,
    moveObject,
    type ConnectionsResponse,
  } from './api';
  import type { Area } from './areas';
  import { names } from './names.svelte';
  import { untrack } from 'svelte';

  let {
    open,
    kind,
    id,
    areas,
    dirty,
    readAt,
    onmoved,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    kind: 'source' | 'destination';
    id: string;
    /** Where it may go: the areas the person edits, but this one. */
    areas: Area[];
    /** Whether the page holds unsaved edits, which a move would leave behind a commit they never saw. */
    dirty: boolean;
    /** The commit the page read, which the move is made against, as the page's own Save is. */
    readAt: string | null;
    onmoved: (to: string) => void;
    oncancel: () => void;
  } = $props();

  const here = currentScope();
  let to = $state('');
  let answer = $state<ConnectionsResponse | null>(null);
  /** The routes that end at a destination, which stay where they are: this area's and, from `shared`, the groups'. */
  let behind: string[] = $state([]);
  const busy = new Busy();

  $effect(() => {
    if (!open) return;
    // Picked once, on opening: the choice is the person's from there.
    to = untrack(() => areas[0]?.id ?? '');
    answer = null;
    behind = [];
    const asked = id;
    if (kind === 'destination') {
      const read = untrack(() => [here, ...(here === SHARED_SCOPE ? areas.map((a) => a.id) : [])]);
      Promise.all(
        read.map(async (area) =>
          ((await configAt(currentBranch(), area)).config?.routes ?? [])
            .filter((r) => r.deliver?.some((d) => d.to.includes(asked)))
            .map((r) => names.of(currentBranch(), area, 'routes', r.id)),
        ),
      )
        .then((found) => {
          if (asked === id) behind = found.flat();
        })
        .catch(() => {});
    }
    getConnections(kind, asked)
      .then((res) => {
        if (asked === id && res.ok) answer = res;
      })
      .catch(() => {});
  });

  /** The routes that start at a source here, which go with it. */
  const routes = $derived(
    kind === 'source' && answer
      ? answer.downstream
          .filter((c) => c.kind === 'route' && c.area === here)
          .map((c) => names.of(currentBranch(), here, 'routes', c.id))
      : [],
  );
  const nameOf = (area: string) => areas.find((a) => a.id === area)?.name ?? area;
  const runsAfter = $derived(to === SHARED_SCOPE ? 'every worker group' : nameOf(to));

  async function move() {
    if (busy.on || dirty || !to) return;
    await busy.run(async () => {
      const res = await moveObject(kind, id, to, readAt);
      if (res.ok) onmoved(to);
    });
  }
</script>

<Dialog {open} title={`move ${kind} ${id}`} {oncancel} onsubmit={move}>
  <div class="form-body">
    <Field label="move to">
      <select bind:value={to}>
        {#each areas as a (a.id)}
          <option value={a.id}>{a.name}</option>
        {/each}
      </select>
    </Field>
    <dl class="rows">
      {#if routes.length > 0}
        <dt>goes with it</dt>
        <dd class="mono">{routes.join(', ')}</dd>
      {/if}
      {#if behind.length > 0}
        <dt>stay behind</dt>
        <dd class="mono">{behind.join(', ')}</dd>
      {/if}
      <dt>runs in now</dt>
      <dd>{answer ? answer.groups.join(', ') || 'no worker group' : '…'}</dd>
      <dt>runs in after</dt>
      <dd>{runsAfter}</dd>
    </dl>
  </div>
  {#snippet actions()}
    <button type="button" onclick={oncancel}>Cancel</button>
    <Button type="submit" class="primary" disabled={busy.on ? BUSY : dirty ? SAVE_FIRST : ''}
      >Move</Button
    >
  {/snippet}
</Dialog>

<style>
  .form-body {
    display: flex;
    flex-direction: column;
    gap: 0.7rem;
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
</style>
