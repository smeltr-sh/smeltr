<!--
  The records a workspace runs over, chosen from one menu: a saved sample, a capture of a source
  that feeds the pipeline, or a new one pasted or read from a file, which `AddSampleDialog` makes.
  Each tab has its own search, since a fleet's sources and samples run to the hundreds.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import LockIcon from './LockIcon.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
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
    onpick: (id: string) => void;
    /** Opens the dialog that makes a new sample. */
    onadd: () => void;
    /** Starts a capture at `source`. */
    oncapture: (source: Connected) => void;
  } = $props();

  type Tab = 'saved' | 'capture';
  const TABS: Tab[] = ['saved', 'capture'];
  let open = $state(false);
  let tab = $state<Tab>('saved');
  let query = $state('');

  let selectedName = $derived(samples.find((s) => s.id === selected)?.name ?? selected);

  // -- saved ---------------------------------------------------------------

  const locked = (s: SampleSummary) => (lockedBy.get(s.id)?.length ?? 0) > 0;
  let saved = $derived(
    [...samples].sort(
      (a, b) =>
        Number(b.id === lastUsed) - Number(a.id === lastUsed) ||
        Number(locked(b)) - Number(locked(a)) ||
        (capturedAt(b) ?? (b.captured ?? 0) * 1000) - (capturedAt(a) ?? (a.captured ?? 0) * 1000),
    ),
  );

  // -- capture -------------------------------------------------------------

  let sources = $state<Connected[]>([]);
  async function loadSources() {
    if (!pipeline) return;
    try {
      const res = await getConnections('pipeline', pipeline);
      sources = res.upstream.filter((c) => c.kind === 'source' && mayCapture(c));
    } catch {
      // Unreadable connections leave nothing to capture from, which the tab says.
      sources = [];
    }
  }

  let found = $derived.by(() => {
    const q = query.trim().toLowerCase();
    if (tab === 'saved')
      return q
        ? saved.filter((s) => `${s.name} ${s.source ?? ''}`.toLowerCase().includes(q))
        : saved;
    return [];
  });
  let foundSources = $derived.by(() => {
    const q = query.trim().toLowerCase();
    return q ? sources.filter((c) => c.id.toLowerCase().includes(q)) : sources;
  });

  function show(next: Tab) {
    tab = next;
    query = '';
  }
</script>

<ChipPicker
  kind="columns"
  align="left"
  bind:open
  ariaLabel="Choose a sample"
  onopen={() => {
    show('saved');
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
      <SecondaryTabs tabs={TABS} active={tab} onselect={show} ariaLabel="Sample">
        {#snippet label(t)}
          {t === 'saved' ? 'Saved' : 'Capture'}
          <span class="dim">{t === 'saved' ? samples.length : sources.length}</span>
        {/snippet}
        {#snippet after()}
          <button
            type="button"
            class="add"
            onclick={() => {
              open = false;
              onadd();
            }}>Paste or file</button
          >
        {/snippet}
      </SecondaryTabs>

      {#if tab === 'saved'}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          type="search"
          bind:value={query}
          placeholder="Find a sample"
          aria-label="Find a sample"
          autofocus
        />
        <div class="list" role="listbox" aria-label="Saved samples">
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
              <span class="name">
                {#if lockers.length > 0}<span title={`Locked by the test ${lockers.join(', ')}`}
                    ><LockIcon /></span
                  >{/if}
                {s.name}
              </span>
              <span class="dim"
                >{count(s.records, 'record')}{capturedAt(s) !== undefined
                  ? ` · ${dateTime(capturedAt(s)! / 1000)}`
                  : ''}{s.id === lastUsed ? ' · last used' : ''}</span
              >
            </button>
          {/each}
        </div>
      {:else if tab === 'capture'}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          type="search"
          bind:value={query}
          placeholder="Find a source"
          aria-label="Find a source"
          autofocus
        />
        <div class="list" role="listbox" aria-label="Sources">
          {#each foundSources as c (`${c.scope}/${c.id}`)}
            <button
              type="button"
              class="item"
              role="option"
              aria-selected="false"
              onclick={() => {
                open = false;
                oncapture(c);
              }}><span class="name mono">◉ {c.id}</span></button
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

  .add {
    margin: 0 0 0.25rem auto;
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
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .mono {
    font-family: var(--mono);
  }

  .foot {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: var(--text-label);
  }
</style>
