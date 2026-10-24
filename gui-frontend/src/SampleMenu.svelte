<!--
  The records a workspace runs over, chosen from one menu: a saved sample, a capture of a source
  that feeds the pipeline, or records pasted or read from a file and saved as a sample. Each tab has
  its own search, since a fleet's sources and samples run to the hundreds.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import LockIcon from './LockIcon.svelte';
  import { mintSampleId } from './ids';
  import { count } from './time';
  import { reported } from './errors.svelte';
  import {
    getConnections,
    putSample,
    recordsFromText,
    type Connected,
    type SampleSummary,
  } from './api';

  let {
    samples,
    selected,
    lastUsed,
    records,
    lockedBy,
    pipeline,
    mayCapture,
    onpick,
    onsaved,
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
    /** Records pasted or read from a file, saved: the new sample's id. */
    onsaved: (id: string) => void;
    /** Starts a capture at `source`. */
    oncapture: (source: Connected) => void;
  } = $props();

  type Tab = 'saved' | 'capture' | 'paste';
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
        (b.captured ?? 0) - (a.captured ?? 0),
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

  // -- paste or file ---------------------------------------------------------

  let text = $state('');
  let name = $state('');
  let busy = $state(false);
  let pasted = $derived(recordsFromText(text));

  async function pickFile(e: Event) {
    const file = (e.target as HTMLInputElement).files?.[0];
    if (!file) return;
    text = await file.text();
    if (!name) name = file.name.replace(/\.[^.]+$/, '');
  }

  /** A new sample's id is minted, never the name: a test names its sample by
   * id, so a rename must leave the id alone. */
  async function save() {
    if (!name.trim() || pasted.length === 0) return;
    busy = true;
    try {
      const id = mintSampleId();
      const res = await putSample({ id, name: name.trim(), records: pasted });
      if (res.ok) {
        text = '';
        name = '';
        open = false;
        onsaved(id);
      }
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
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
      <div class="tabs" role="tablist" aria-label="Sample">
        <button
          type="button"
          role="tab"
          aria-selected={tab === 'saved'}
          onclick={() => show('saved')}>Saved <span class="dim">{samples.length}</span></button
        >
        <button
          type="button"
          role="tab"
          aria-selected={tab === 'capture'}
          onclick={() => show('capture')}>Capture <span class="dim">{sources.length}</span></button
        >
        <button
          type="button"
          role="tab"
          aria-selected={tab === 'paste'}
          onclick={() => show('paste')}>Paste or file</button
        >
      </div>

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
              <span class="dim">{s.records}{s.id === lastUsed ? ' · last used' : ''}</span>
            </button>
          {/each}
        </div>
      {:else if tab === 'capture'}
        <!-- svelte-ignore a11y_autofocus -->
        <input type="search" bind:value={query} aria-label="Find a source" autofocus />
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
      {:else}
        <textarea bind:value={text} rows="10" spellcheck="false" aria-label="Records"></textarea>
        <div class="foot">
          <label class="file"
            >Choose file<input
              type="file"
              accept=".json,.ndjson,.jsonl,.txt,.log"
              onchange={pickFile}
            /></label
          >
          <span class="grow"></span>
          <input bind:value={name} aria-label="Sample name" />
          <button
            type="button"
            class="primary"
            disabled={busy || !name.trim() || pasted.length === 0}
            onclick={save}>{busy ? 'Saving…' : 'Save sample'}</button
          >
        </div>
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

  /* ChangeDetailPage's tabs. */
  .tabs {
    display: flex;
    gap: 1.2rem;
    border-bottom: 1px solid var(--border);
  }

  .tabs button {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    background: none;
    padding: 0.35rem 0;
    border-width: 0 0 3px;
    border-radius: 0;
    border-color: transparent;
    color: var(--dim);
    font-size: var(--text-body);
  }

  .tabs button[aria-selected='true'] {
    color: var(--ink);
    font-weight: 600;
    border-color: var(--accent);
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

  .grow {
    flex-grow: 1;
  }

  textarea {
    width: 100%;
    box-sizing: border-box;
    font-family: var(--mono);
    resize: vertical;
  }

  /* A button's face over the file input it stands for. */
  .file {
    position: relative;
    margin: 0;
    padding: 0.3rem 0.7rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-weight: 400;
    cursor: pointer;
  }

  .file input {
    position: absolute;
    inset: 0;
    opacity: 0;
    cursor: pointer;
  }
</style>
