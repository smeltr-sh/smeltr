<!--
  The records a workspace runs over, chosen from one menu: a saved sample, the capture the leader
  holds unsaved, or a new sample, which Add sample makes. It has a search, since a fleet's samples
  run to the hundreds.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import LockIcon from './LockIcon.svelte';
  import { count, dateTime } from './time';
  import { capturedAt, type SampleSummary } from './api';

  let {
    samples,
    selected,
    lastUsed,
    records,
    lockedBy,
    held,
    onpick,
    onadd,
  }: {
    samples: SampleSummary[];
    /** The sample the records on screen came from. */
    selected: string;
    /** The sample this pipeline last ran over, which the list puts first. */
    lastUsed: string;
    /** How many records it holds, once run. */
    records: number | null;
    /** The names of this pipeline's tests that lock each sample, by sample id. */
    lockedBy: Map<string, string[]>;
    /** The capture the leader holds for the person signed in, listed first among the samples. Picking it passes
     * `choice` to `onpick`. */
    held?: { choice: string; records: number; source?: string };
    onpick: (id: string) => void;
    /** Opens Add sample to make a new sample; absent where the person may make none. */
    onadd?: () => void;
  } = $props();

  let open = $state(false);
  let query = $state('');

  const HELD_NAME = 'Your capture';
  let selectedName = $derived(
    held?.choice === selected
      ? HELD_NAME
      : (samples.find((s) => s.id === selected)?.name ?? selected),
  );

  // -- samples -------------------------------------------------------------

  const locked = (s: SampleSummary) => (lockedBy.get(s.id)?.length ?? 0) > 0;
  let saved = $derived(
    [...samples].sort(
      (a, b) =>
        Number(b.id === lastUsed) - Number(a.id === lastUsed) ||
        Number(locked(b)) - Number(locked(a)) ||
        (b.written ?? 0) - (a.written ?? 0),
    ),
  );
  /** A row of the list, saying after its name what it holds and where it is from. */
  const row = (
    id: string,
    name: string,
    records: number,
    source: string | undefined,
    ...notes: (string | false)[]
  ) => ({
    id,
    name,
    source,
    said: [count(records, 'record'), source, ...notes].filter(Boolean).join(' · '),
  });
  /** The held capture, then the saved samples. */
  let listed = $derived([
    ...(held ? [row(held.choice, HELD_NAME, held.records, held.source, 'not saved')] : []),
    ...saved.map((s) => {
      const at = capturedAt(s);
      return row(
        s.id,
        s.name,
        s.records,
        s.source,
        at !== undefined && dateTime(at / 1000),
        s.id === lastUsed && 'last used',
      );
    }),
  ]);

  let q = $derived(query.trim().toLowerCase());
  let found = $derived(
    listed.filter((s) => `${s.name} ${s.source ?? ''}`.toLowerCase().includes(q)),
  );
</script>

<ChipPicker
  kind="columns"
  align="left"
  bind:open
  ariaLabel="Choose a sample"
  onopen={() => (query = '')}
>
  {#snippet face()}
    <span class="dim">Sample</span>
    <strong>{selectedName}</strong>
    {#if records !== null}<span class="dim">{count(records, 'record')}</span>{/if} ▾
  {/snippet}
  {#snippet children()}
    <div class="menu">
      {#if onadd}
        <button
          type="button"
          class="add"
          onclick={() => {
            open = false;
            onadd();
          }}>Add sample</button
        >
      {/if}
      <!-- svelte-ignore a11y_autofocus -->
      <input
        type="search"
        bind:value={query}
        placeholder="Find a sample"
        aria-label="Find a sample"
        autofocus
      />
      <div class="list" role="listbox" aria-label="Samples">
        {#each found as s (s.id)}
          {@const lockers = lockedBy.get(s.id) ?? []}
          <button
            type="button"
            class="item"
            role="option"
            aria-selected={s.id === selected}
            onclick={() => {
              open = false;
              onpick(s.id);
            }}
          >
            <span class="name ellipsis">
              {#if lockers.length > 0}<span title={`Locked by the test ${lockers.join(', ')}`}
                  ><LockIcon /></span
                >{/if}
              {s.name}
            </span>
            <span class="dim">{s.said}</span>
          </button>
        {/each}
      </div>
    </div>
  {/snippet}
</ChipPicker>

<style>
  .menu {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    width: min(30rem, 80vw);
    padding: 0.5rem;
  }

  .dim {
    color: var(--dim);
    font-weight: 400;
  }

  .add {
    align-self: flex-end;
  }

  .list {
    display: flex;
    flex-direction: column;
    max-height: 22rem;
    overflow-y: auto;
  }

  .item {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    border: none;
    border-radius: var(--radius);
    background: none;
    padding: 0.3rem 0.4rem;
    font: inherit;
    font-size: var(--text-label);
    color: var(--ink);
    text-align: left;
    cursor: pointer;
  }

  .item:hover,
  .item[aria-selected='true'] {
    background: color-mix(in srgb, var(--ink) 8%, transparent);
  }

  .item[aria-selected='true'] .name {
    font-weight: 600;
  }

  .name {
    flex: 1;
    min-width: 0;
  }
</style>
