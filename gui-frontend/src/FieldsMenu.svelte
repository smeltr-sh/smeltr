<!--
  The workspace's Fields menu, which is also its data dictionary: every field the records or the
  compiled pipeline have, with its type and how many records hold it. A tick shows the field as a
  column; a count filters the records to those that hold the field. Select all and Select none act
  on what the search shows, so a search and one press choose a family of fields.
-->
<script lang="ts" module>
  /** One field, as the menu lists it. */
  export interface MenuField {
    path: string;
    type: string | null;
    /** The records that hold it. */
    count: number;
  }
</script>

<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import Help from './Help.svelte';
  import Toggle from './Toggle.svelte';
  import type { DictionaryOut } from './api';

  let {
    fields,
    records,
    shown,
    dictionary,
    onshow,
    ondefault,
    onhas,
  }: {
    fields: MenuField[];
    /** How many records there are, or null with none to count. */
    records: number | null;
    shown: (path: string) => boolean;
    dictionary: DictionaryOut | undefined;
    /** Shows or hides each of `paths`. */
    onshow: (paths: string[], show: boolean) => void;
    ondefault: () => void;
    /** Filters the records to those holding `path`. */
    onhas: (path: string) => void;
  } = $props();

  // A list this long is read by searching it; the rest wait behind one line.
  const LISTED = 50;

  let open = $state(false);
  let query = $state('');
  let all = $state(false);
  let matches = $derived.by(() => {
    const q = query.trim().toLowerCase();
    return q ? fields.filter((f) => f.path.toLowerCase().includes(q)) : fields;
  });
  let listed = $derived(all ? matches : matches.slice(0, LISTED));
  let shownCount = $derived(fields.filter((f) => shown(f.path)).length);

  /** `path` around the search's match, so the match can be marked. */
  function parts(path: string): [string, string, string] {
    const q = query.trim().toLowerCase();
    const at = q ? path.toLowerCase().indexOf(q) : -1;
    if (at < 0) return [path, '', ''];
    return [path.slice(0, at), path.slice(at, at + q.length), path.slice(at + q.length)];
  }

  let exportOpen = $state(false);
  let includeSampled = $state(false);
  let copied = $state(false);

  async function copyMapping(mapping: unknown) {
    try {
      await navigator.clipboard.writeText(JSON.stringify(mapping, null, 2));
      copied = true;
      setTimeout(() => (copied = false), 1500);
    } catch {
      // No clipboard access: the mapping is on screen to select by hand.
    }
  }
</script>

<ChipPicker kind="columns" bind:open ariaLabel="Choose fields" onopen={() => (all = false)}>
  {#snippet face()}
    Fields <span class="dim">{shownCount} of {fields.length}</span>
  {/snippet}
  {#snippet children()}
    <div class="menu">
      <!-- svelte-ignore a11y_autofocus -->
      <input
        type="search"
        class="find"
        bind:value={query}
        placeholder="Find a field"
        aria-label="Find a field"
        autofocus
      />
      <div class="actions">
        <button
          type="button"
          onclick={() =>
            onshow(
              matches.map((f) => f.path),
              true,
            )}>Select all{query.trim() ? ` ${matches.length}` : ''}</button
        >
        <button
          type="button"
          onclick={() =>
            onshow(
              matches.map((f) => f.path),
              false,
            )}>Select none</button
        >
        <span class="grow"></span>
        <span class="dim"
          >{matches.filter((f) => shown(f.path)).length} of {matches.length} shown</span
        >
      </div>
      <div class="list" role="group" aria-label="Fields">
        <div class="row head" aria-hidden="true">
          <span></span><span>field</span><span>type</span><span class="records"
            >{records === null ? '' : 'records'}</span
          >
        </div>
        {#each listed as f (f.path)}
          {@const [before, match, after] = parts(f.path)}
          <label class="row">
            <input
              type="checkbox"
              checked={shown(f.path)}
              onchange={(e) => onshow([f.path], e.currentTarget.checked)}
            />
            <span class="mono path">{before}<mark>{match}</mark>{after}</span>
            <span class="mono dim type" title={f.type ?? undefined}>{f.type ?? ''}</span>
            {#if records !== null}<button
                type="button"
                class="count"
                class:none={f.count === 0}
                aria-label={`records with ${f.path}`}
                disabled={f.count === 0}
                onclick={(e) => {
                  e.preventDefault();
                  open = false;
                  onhas(f.path);
                }}>{f.count} of {records}</button
              >{/if}
          </label>
        {/each}
        {#if listed.length < matches.length}
          <button type="button" class="more" onclick={() => (all = true)}
            >{matches.length - listed.length} more</button
          >
        {/if}
      </div>
      <div class="foot">
        {#if dictionary}
          {@const d = dictionary}
          {@const dict = includeSampled && d.sampled ? d.sampled : d.compiled}
          <ChipPicker kind="columns" align="left" bind:open={exportOpen}>
            {#snippet face()}Export mapping ▾{/snippet}
            {#snippet children()}
              <div class="export">
                {#if d.sampled}<label class="inline-checkbox">
                    <Toggle
                      bind:checked={includeSampled}
                      ariaLabel="include fields seen in the sample"
                    />
                    include fields seen in the sample
                  </label>{/if}
                <div class="actions">
                  <button type="button" onclick={() => copyMapping(dict.elasticsearch)}>
                    {copied ? 'Copied' : 'Copy as Elasticsearch mapping'}
                  </button>
                  <Help label="Elasticsearch mapping">
                    The mapping leaves out a <code>variant</code> field. Give the field a type to include
                    it.
                  </Help>
                </div>
                <pre class="mono">{JSON.stringify(dict.elasticsearch, null, 2)}</pre>
              </div>
            {/snippet}
          </ChipPicker>
        {/if}
        <span class="grow"></span>
        <button type="button" onclick={ondefault}>Default</button>
      </div>
    </div>
  {/snippet}
</ChipPicker>

<style>
  .menu {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    width: min(40rem, 80vw);
    padding: 0.5rem;
  }

  .find {
    width: 100%;
  }

  .actions,
  .foot {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  .grow {
    flex-grow: 1;
  }

  .dim {
    color: var(--dim);
  }

  .list {
    max-height: 26rem;
    overflow-y: auto;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    padding: 0.25rem 0;
  }

  .row {
    display: grid;
    grid-template-columns: 1.2rem minmax(0, 1fr) 6rem 6rem;
    align-items: center;
    gap: 0.4rem;
    margin: 0;
    padding: 0.1rem 0.25rem;
    font-weight: 400;
    font-size: var(--text-label);
    cursor: pointer;
  }

  .row.head {
    color: var(--dim);
    font-size: var(--text-micro);
    cursor: default;
  }

  .path,
  .type {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  mark {
    background: color-mix(in srgb, var(--accent) 25%, transparent);
    color: inherit;
  }

  .mono {
    font-family: var(--mono);
  }

  .records,
  .count {
    justify-self: end;
  }

  .count {
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    font-family: var(--mono);
    color: var(--ink);
    text-decoration: underline dotted;
    text-underline-offset: 3px;
    cursor: pointer;
  }

  .count.none {
    color: var(--accent);
    text-decoration: none;
    cursor: default;
  }

  .more {
    border: none;
    background: none;
    padding: 0.2rem 1.85rem;
    font: inherit;
    font-size: var(--text-label);
    color: var(--dim);
    cursor: pointer;
  }

  .export {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    width: min(32rem, 70vw);
    padding: 0.5rem;
  }

  .export pre {
    max-height: 16rem;
    overflow: auto;
    margin: 0;
    font-size: var(--text-micro);
  }

  .inline-checkbox {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    margin: 0;
    font-size: var(--text-label);
    font-weight: 400;
    color: var(--dim);
  }
</style>
