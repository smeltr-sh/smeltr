<!--
  Moving an object into another area, in one commit, the way an export and an import move objects between leaders.
  The leader lists each object the move would take or break, the least the move takes ticked; each tick asks again,
  and what the ticks would leave broken shows under the list. What moves replaces whatever the other area holds under
  its id. Move is the commit, against the commit the page read.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY, SAVE_FIRST } from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import {
    PART_NAMES,
    SHARED_SCOPE,
    currentScope,
    moveObjects,
    type ImportPart,
    type MoveRow,
  } from './api';
  import type { Area } from './areas';
  import { untrack } from 'svelte';

  let {
    open,
    part,
    id,
    name = id,
    areas,
    dirty,
    readAt,
    pullOne = false,
    onmoved,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    part: ImportPart;
    id: string;
    /** What the person calls it, where that is not its id. */
    name?: string;
    /** The areas the person edits: it may go to any but this one. */
    areas: Area[];
    /** Whether the page holds unsaved edits, which a move would leave behind a commit they never saw. */
    dirty: boolean;
    /** The commit the page read, which the move is made against, as the page's own Save is. */
    readAt?: string | null;
    /** A source one worker of a group reads at a time (`SOURCE_KIND`), whose read position the group keeps. */
    pullOne?: boolean;
    onmoved: (to: string) => void;
    oncancel: () => void;
  } = $props();

  const here = currentScope();
  const elsewhere = $derived(areas.filter((a) => a.id !== here));
  let to = $state('');
  let rows = $state<MoveRow[]>([]);
  let exceptions = $state<string[]>([]);
  /** Counts the asks, so an answer to one the person has since changed is dropped. */
  let asked = 0;
  const busy = new Busy();

  const key = (r: { part: ImportPart; id: string }) => `${r.part}\u0000${r.id}`;
  const nameOf = (area: string) => areas.find((a) => a.id === area)?.name ?? area;

  $effect(() => {
    if (!open) return;
    // Picked once, on opening: the choice is the person's from there.
    to = untrack(() => elsewhere[0]?.id ?? '');
    untrack(choose);
  });

  /** Asks the leader what moving with `ticked` would do, or with the least the move takes when it is undefined. */
  async function list(ticked: { part: ImportPart; id: string }[] | undefined) {
    if (!to) return;
    const mine = ++asked;
    const res = await moveObjects({ part, id, to, ticked }, readAt);
    if (mine !== asked || !res.ok) return;
    rows = res.rows ?? [];
    exceptions = res.exceptions ?? [];
  }

  const ticks = () => rows.filter((r) => r.ticked).map((r) => ({ part: r.part, id: r.id }));

  function tick(r: MoveRow, on: boolean) {
    r.ticked = on;
    list(ticks());
  }

  /** Starts again from the least the move takes, for the area chosen. */
  function choose() {
    rows = [];
    exceptions = [];
    list(undefined);
  }

  /** A group keeps a source's progress and learned normal as its own, so a group the source moves into from another
   * starts without them. */
  const startsOver = $derived.by(() => {
    if (part !== 'source' || here === SHARED_SCOPE || to === SHARED_SCOPE) return undefined;
    if (pullOne)
      return `${nameOf(to)} has none of ${nameOf(here)}'s progress, so it reads everything again there, and learns its normal again.`;
    return `${nameOf(to)} has none of what ${nameOf(here)} learned as normal for this source, so ${nameOf(to)} learns it again.`;
  });

  /** Why Move is disabled, or '' when it can go. */
  const blocked = $derived.by(() => {
    if (busy.on) return BUSY;
    if (dirty) return SAVE_FIRST;
    if (rows.length === 0) return BUSY;
    if (exceptions.length > 0) return 'Resolve the exceptions above to move.';
    return '';
  });

  async function move() {
    if (blocked) return;
    await busy.run(async () => {
      const res = await moveObjects({ part, id, to, ticked: ticks(), go: true }, readAt);
      if (res.ok) onmoved(to);
      else if (res.exceptions) exceptions = res.exceptions;
    });
  }
</script>

<Dialog
  {open}
  title={`move ${PART_NAMES[part].toLowerCase()} ${name}`}
  wide
  {oncancel}
  onsubmit={move}
>
  <div class="body">
    <Field label="move to" note={startsOver}>
      <select bind:value={to} onchange={choose}>
        {#each elsewhere as a (a.id)}
          <option value={a.id}>{a.name}</option>
        {/each}
      </select>
    </Field>
    {#if rows.length === 0}
      <p class="dim">Loading…</p>
    {:else}
      <table>
        <thead>
          <tr><th>Kind</th><th>Name</th><th>In {nameOf(to)}</th><th>Move</th></tr>
        </thead>
        <tbody>
          {#each rows as r (key(r))}
            <tr>
              <td>{PART_NAMES[r.part]}</td>
              <td class="mono">{r.name ?? r.id}</td>
              <td class:dim={r.standing === 'same'} class:taken={r.standing === 'taken'}
                >{r.standing === 'new'
                  ? 'New'
                  : r.standing === 'same'
                    ? 'Same as there'
                    : 'Taken'}</td
              >
              {#if r.part === 'sample' && !(r.part === part && r.id === id)}
                <td class="dim">With its tests</td>
              {:else}
                <td>
                  <input
                    type="checkbox"
                    aria-label="Move {PART_NAMES[r.part]} {r.name ?? r.id}"
                    checked={r.ticked}
                    disabled={busy.on || (r.part === part && r.id === id)}
                    onchange={(e) => tick(r, e.currentTarget.checked)}
                  />
                </td>
              {/if}
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
    {#each exceptions as e (e)}
      <p class="exception">{e}</p>
    {/each}
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel}>Cancel</Button>
    <Button type="submit" class="primary" disabled={blocked}>Move</Button>
  {/snippet}
</Dialog>

<style>
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .mono {
    font-family: var(--mono);
  }
  .taken {
    color: var(--warn);
    font-weight: 600;
  }
  .exception {
    color: var(--accent);
  }
  p {
    margin: 0;
  }
  th {
    text-align: left;
  }
</style>
