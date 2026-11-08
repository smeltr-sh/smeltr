<!--
  The records a workspace runs over, chosen from one menu: a saved sample, the capture the leader
  holds unsaved, or a new sample. A new sample is captured from a source that feeds the pipeline,
  or made from a paste or a file; `AddSampleDialog` makes either kind for an editor. Each list has
  its own search, since a fleet's sources and samples run to the hundreds.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import LockIcon from './LockIcon.svelte';
  import { count, dateTime } from './time';
  import { capturedAt, getConnections, type Connected, type SampleSummary } from './api';

  let {
    samples,
    selected,
    lastUsed,
    records,
    lockedBy,
    pipeline,
    mayCapture,
    held,
    onpick,
    onadd,
    oncapture,
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
    /** The pipeline, whose sources a capture is offered from; empty before its first save. */
    pipeline: string;
    mayCapture: (source: Connected) => boolean;
    /** The capture the leader holds for the person signed in, listed first among the samples. Picking it passes
     * `choice` to `onpick`. */
    held?: { choice: string; records: number; source?: string };
    onpick: (id: string) => void;
    /** Opens the dialog that makes a new sample; absent for a viewer, who may not save one. */
    onadd?: () => void;
    /** Starts a capture at `source`; absent for whoever may not capture. */
    oncapture?: (source: Connected) => void;
  } = $props();

  let open = $state(false);
  /** The menu lists the sources to capture from, in place of the samples. */
  let choosingSource = $state(false);
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

  // -- capture -------------------------------------------------------------

  let sources = $state<Connected[]>([]);
  async function loadSources() {
    if (!pipeline || !oncapture) return;
    try {
      const res = await getConnections('pipeline', pipeline);
      sources = res.upstream.filter((c) => c.kind === 'source' && mayCapture(c));
    } catch {
      // Unreadable connections leave nothing to capture from, which the count of sources says.
      sources = [];
    }
  }

  let q = $derived(query.trim().toLowerCase());
  let found = $derived(
    listed.filter((s) => `${s.name} ${s.source ?? ''}`.toLowerCase().includes(q)),
  );
  let foundSources = $derived(sources.filter((c) => c.id.toLowerCase().includes(q)));

  function listSources(on: boolean) {
    choosingSource = on;
    query = '';
  }
</script>

<ChipPicker
  kind="columns"
  align="left"
  bind:open
  ariaLabel="Choose a sample"
  onopen={() => {
    listSources(false);
    void loadSources();
  }}
>
  {#snippet face()}
    <span class="dim">Sample</span>
    <strong>{selectedName}</strong>
    {#if records !== null}<span class="dim">{count(records, 'record')}</span>{/if} ▾
  {/snippet}
  {#snippet children()}
    <div class="menu">
      <div class="head">
        {#if choosingSource}
          <button type="button" class="back" onclick={() => listSources(false)}>← Samples</button>
        {:else}
          {#if oncapture}
            <button type="button" onclick={() => listSources(true)}>Capture from a source</button>
          {/if}
          {#if onadd}
            <button
              type="button"
              onclick={() => {
                open = false;
                onadd();
              }}>Paste or file</button
            >
          {/if}
        {/if}
      </div>

      {#if !choosingSource}
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
      {:else}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          type="search"
          bind:value={query}
          placeholder="Find a source"
          aria-label="Find a source"
          autofocus
        />
        <div class="list" role="listbox" aria-label="Sources">
          {#each foundSources as c (`${c.area}/${c.id}`)}
            <button
              type="button"
              class="item"
              role="option"
              aria-selected="false"
              onclick={() => {
                open = false;
                oncapture?.(c);
              }}><span class="name mono ellipsis">◉ {c.id}</span></button
            >
          {/each}
        </div>
        <div class="foot dim">{foundSources.length} of {count(sources.length, 'source')}</div>
      {/if}
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

  .head {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
  }

  .back {
    margin-right: auto;
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

  .foot {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-label);
  }
</style>
