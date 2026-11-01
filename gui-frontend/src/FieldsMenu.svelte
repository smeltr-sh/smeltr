<!--
  The workspace's Fields menu, which is also its data dictionary: every field the records or the
  compiled pipeline have, with its type and how many records hold it. A tick shows the field as a
  column; a count filters the records to those that hold the field. Select all and Select none act
  on what the search shows, so a search and one press choose a family of fields.

  Where the pipeline produces a schema, the menu also lists the schema's fields the records lack, one
  level at a time under the path the search names, in the order the name boxes offer them
  (`schemaTree`), so an optional field is in reach while the pipeline is built.
-->
<script lang="ts" module>
  /** One field, as the menu lists it. */
  export interface MenuField {
    path: string;
    type: string | null;
    /** The records that hold it. */
    count: number;
    /** The schema requiring it, for a field the rows lack. */
    requiredBy?: string;
  }
</script>

<script lang="ts">
  import ChevronIcon from './ChevronIcon.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import Help from './Help.svelte';
  import Toggle from './Toggle.svelte';
  import type { DictionaryOut } from './api';
  import { producedSchemas } from './producedSchemas.svelte';
  import RequirementMark from './RequirementMark.svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import FieldDoc from './FieldDoc.svelte';
  import Segmented from './Segmented.svelte';
  import ShowAll from './ShowAll.svelte';
  import {
    askedUnder,
    fields as countFields,
    firstOf,
    fmt,
    groupOf,
    matching,
    requirementOf,
  } from './schemaTree';

  let {
    fields,
    records,
    shown,
    dictionary,
    onshow,
    ondefault,
    onhas,
    onassign,
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
    /** Writes `path` in an eval step, where the pipeline can be edited. */
    onassign?: (path: string) => void;
  } = $props();

  // A list this long is read by searching it; the rest wait behind one line.
  const LISTED = 50;

  let open = $state(false);
  let query = $state('');
  let all = $state(false);
  /** The paths whose documentation shows under their row. */
  const detailed = new SvelteSet<string>();
  let matches = $derived.by(() => {
    const q = query.trim().toLowerCase();
    return q ? fields.filter((f) => f.path.toLowerCase().includes(q)) : fields;
  });
  let listed = $derived(all ? matches : matches.slice(0, LISTED));
  let shownCount = $derived(fields.filter((f) => shown(f.path)).length);

  // -- the produced schemas' fields ---------------------------------------------

  const produced = producedSchemas();
  let tree = $derived(produced?.tree ?? null);
  type Which = 'unset' | 'set' | 'all';
  const WHICH: { value: Which; label: string }[] = [
    { value: 'unset', label: 'Not set' },
    { value: 'set', label: 'Set' },
    { value: 'all', label: 'All' },
  ];
  let which = $state<Which>('set');
  /** The fields not set the menu names before it offers the rest. */
  const UNSET_NAMED = 8;
  let tally = $derived(tree && open ? tree.tally(produced!.held) : null);
  let unsetAt = $derived(tree ? tree.at(query) : null);
  let unset = $derived.by(() => {
    if (!tree || !unsetAt || which === 'set') return null;
    const found = matching(
      tree.children(unsetAt.parent, produced!.held).filter((c) => !c.set),
      unsetAt.key,
    );
    return { found, listed: all ? found : firstOf(found, UNSET_NAMED) };
  });
  /** The record's fields the menu lists: every one, unless it lists only what is not set. */
  let setListed = $derived(tree && which === 'unset' ? [] : listed);
  /** What is not set has no records to count. */
  let uncounted = $derived(tree !== null && which === 'unset');

  const showMatches = (show: boolean) =>
    onshow(
      matches.map((f) => f.path),
      show,
    );

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
    <div class="menu" class:wide={tree !== null} class:uncounted>
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
        {#if tree}
          <Segmented label="Which fields" options={WHICH} bind:value={which} />
        {/if}
        <!-- What is not set has no column to show. -->
        {#if !tree || which !== 'unset'}
          <button type="button" onclick={() => showMatches(true)}
            >Select all{query.trim() ? ` ${matches.length}` : ''}</button
          >
          <button type="button" onclick={() => showMatches(false)}>Select none</button>
          <span class="grow"></span>
          <span class="dim"
            >{matches.filter((f) => shown(f.path)).length} of {matches.length} shown</span
          >
        {/if}
      </div>
      {#if tree && tally}
        <p class="tally">
          <strong>{tree.titles}</strong>
          {#each [['required', 'Required'], ['recommended', 'Recommended'], ['optional', 'Optional']] as const as [r, label], i (r)}
            {#if i > 0}<span class="dim">·</span>{/if}
            <span>{label} <strong>{fmt(tally[r].set)} of {fmt(tally[r].of)}</strong> set</span>
          {/each}
        </p>
      {/if}
      <div class="list" role="group" aria-label="Fields">
        <div class="row head" aria-hidden="true">
          <span></span><span>field</span><span>type</span>{#if tree}<span>requirement</span
            >{/if}{#if !uncounted}<span class="records">{records === null ? '' : 'records'}</span
            >{/if}{#if tree}<span></span>{/if}
        </div>
        {#each setListed as f (f.path)}
          {@const req = tree?.requirement(f.path)}
          {@const [before, match, after] = parts(f.path)}
          <label class="row">
            <input
              type="checkbox"
              checked={shown(f.path)}
              onchange={(e) => onshow([f.path], e.currentTarget.checked)}
            />
            <!-- A field no record holds after the step and the pipeline does not write is one it took away. -->
            <span
              class="mono path"
              class:dim={f.count === 0 && f.type === null}
              class:missing={f.requiredBy !== undefined}
              title={f.requiredBy && `${f.requiredBy} requires it; the rows lack it`}
              >{before}<mark>{match}</mark>{after}{#if f.requiredBy}{' '}<span class="required-by"
                  >required by {f.requiredBy}</span
                >{/if}</span
            >
            <span class="mono dim type" title={f.type ?? undefined}>{f.type ?? ''}</span>
            {#if tree}<span class="req"
                >{#if req}<RequirementMark of={req} />{/if}</span
              >{/if}
            {#if records !== null}<button
                type="button"
                class="count link-button"
                class:none={f.count === 0}
                aria-label={`records with ${f.path}`}
                disabled={f.count === 0}
                onclick={(e) => {
                  e.preventDefault();
                  open = false;
                  onhas(f.path);
                }}>{f.count} of {records}</button
              >{:else}<span></span>{/if}
            {#if tree}<span></span>{/if}
          </label>
        {/each}
        {#if unset}
          {#each unset.listed as c, i (c.path)}
            {@const before = i > 0 ? groupOf(unset.listed[i - 1]) : groupOf(c)}
            <div
              class="row unset"
              class:after-fields={(i === 0 && setListed.length > 0) ||
                (before <= 2 && groupOf(c) > 2)}
            >
              {#if c.field}
                {@const shows = detailed.has(c.path)}
                <button
                  type="button"
                  class="toggle"
                  title={shows ? 'Hide details' : 'Show details'}
                  aria-label={shows ? 'Hide details' : 'Show details'}
                  aria-expanded={shows}
                  onclick={() => (shows ? detailed.delete(c.path) : detailed.add(c.path))}
                  ><ChevronIcon open={shows} /></button
                >
              {:else}<span></span>{/if}
              <span class="mono path" class:object={c.nested > 0}
                >{c.path}{c.nested > 0 ? '.…' : ''}</span
              >
              <span class="mono dim type"
                >{c.nested > 0 ? countFields(c.nested) : (c.field?.type ?? '')}</span
              >
              {#if c.nested > 0}
                <span class="req"
                  ><RequirementMark
                    of={c.required > 0
                      ? 'required'
                      : c.recommended > 0
                        ? 'recommended'
                        : 'optional'}
                    text={askedUnder(c)[0] ?? 'optional'}
                  /></span
                >
              {:else}
                {@const req = requirementOf(c)}
                <span class="req"><RequirementMark of={req} /></span>
              {/if}
              {#if !uncounted}<span class="count dash" aria-hidden="true"
                  >{records === null ? '' : '—'}</span
                >{/if}
              {#if c.nested > 0}
                <button
                  type="button"
                  class="act"
                  onclick={() => {
                    query = `${c.path}.`;
                    all = false;
                  }}>Show fields</button
                >
              {:else if onassign}
                <button
                  type="button"
                  class="act"
                  aria-label={`assign ${c.path}`}
                  onclick={() => {
                    open = false;
                    onassign(c.path);
                  }}>+ assign</button
                >
              {:else}<span></span>{/if}
            </div>
            {#if c.field && detailed.has(c.path)}
              <div class="details"><FieldDoc field={c.field} /></div>
            {/if}
          {/each}
        {/if}
        {#if setListed.length < matches.length && which !== 'unset'}
          <button type="button" class="more" onclick={() => (all = true)}
            >{matches.length - listed.length} more</button
          >
        {/if}
      </div>
      {#if unset && unsetAt}
        {#if unset.listed.length < unset.found.length}
          <ShowAll
            shown={unset.listed.length}
            of={`${countFields(unset.found.length)} not set${unsetAt.parent ? ` under ${unsetAt.parent}` : ''}`}
            onshowall={() => (all = true)}
          />
        {/if}
      {/if}
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

  /* A schema's fields add a requirement and an action to each row. */
  .menu.wide {
    width: min(56rem, 90vw);
  }

  .menu.wide .row {
    grid-template-columns: 1.2rem minmax(0, 1fr) 7rem 7.5rem 5rem 6rem;
  }

  .menu.uncounted .row {
    grid-template-columns: 1.2rem minmax(0, 1fr) 7rem 7.5rem 6rem;
  }

  .tally {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin: 0;
    font-size: var(--text-label);
  }

  .req {
    font-size: var(--text-micro);
  }

  .toggle {
    border: none;
    background: none;
    padding: 0;
    font: inherit;
    color: var(--dim);
    cursor: pointer;
  }

  /* A field's documentation, under its row and indented to its path. */
  .details {
    margin: 0.15rem 0 0.4rem 1.6rem;
    padding: 0.5rem 0.75rem;
    border-left: 2px solid var(--border);
    background: color-mix(in srgb, var(--ink) 4%, var(--panel));
  }

  .row.unset {
    cursor: default;
  }

  .row.after-fields {
    margin-top: 0.25rem;
    border-top: 1px solid var(--border);
    padding-top: 0.3rem;
  }

  .path.object {
    color: var(--accent);
  }

  .act {
    justify-self: end;
    font-size: var(--text-micro);
    padding: 0.05rem 0.5rem;
  }

  .count.dash {
    color: var(--dim);
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
    font-family: var(--mono);
    color: var(--ink);
    text-underline-offset: 3px;
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

  /* A field a schema requires and the rows lack: listed so its column can be shown or hidden. */
  .missing {
    color: var(--warn);
  }

  .required-by {
    font-family: var(--font-body);
    font-size: var(--text-micro);
  }
</style>
