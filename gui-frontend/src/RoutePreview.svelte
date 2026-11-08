<!--
  Route preview: where a saved sample's records would go, through
  `POST /api/areas/:area/preview/routes` (`runtime::preview_routes`). The routes it runs
  are the ones on screen, saved or not, so this answers what the edit in
  front of you would do.

  Nothing is delivered and no destination exists. A count here is what the
  delivery rules chose, and a document is what would have been sent.
-->
<script lang="ts">
  import Button, { BUSY, LOADING } from './Button.svelte';
  import ShowAll from './ShowAll.svelte';
  import ChevronIcon from './ChevronIcon.svelte';
  import FoldAllButton from './FoldAllButton.svelte';
  import RecordFields from './RecordFields.svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import { count } from './time';
  import {
    listSamples,
    routePreview,
    errorText,
    type FileConfigDto,
    type RoutePreviewResult,
    type SampleSummary,
  } from './api';
  import { nameOf } from './exceptions';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';

  let {
    /** The whole bundle as the editor has it, unsaved edits included. */
    config,
    /** A run somebody else made. Given one, this shows it and picks nothing;
     * given none, it picks a sample and runs for itself. Two of these on a
     * screen must not be two pickers over two runs of the same thing. */
    result: given,
  }: { config: FileConfigDto | null; result?: RoutePreviewResult | null } = $props();

  let samples: SampleSummary[] = $state([]);
  // Until the list arrives, no sample is not the same as none naming a source.
  let loaded = $state(false);
  let selected = $state('');
  let own = $state<RoutePreviewResult | null>(null);
  let driven = $derived(given !== undefined);
  let result = $derived(driven ? (given ?? null) : own);
  let running = $state(false);
  let error = $state('');
  /** The destination whose documents are open, by id. */
  let openDestination = $state<number | null>(null);
  /** How many documents an open destination shows before a button shows the rest. */
  const SHOWN = 50;
  /** Whether the open destination shows every document, not the first fifty. */
  let allDocuments = $state(false);
  /** The open destination's documents shown whole, by their place in its list. */
  let openDocuments = new SvelteSet<number>();
  /** The records whose sent documents are shown whole, by row. Any number can be open at once, as in a
   * pipeline's preview. */
  let expanded = new SvelteSet<number>();

  async function loadSamples() {
    try {
      const res = await listSamples();
      // Only a sample that names a source can be routed: routing selects on
      // one, so a pasted sample with none matches nothing.
      samples = res.samples.filter((s) => s.source);
      loaded = true;
      if (!selected && samples.length > 0) selected = samples[0].id;
    } catch (e) {
      error = unreported(e);
    }
  }
  loadSamples();

  async function run() {
    if (!config || !selected) return;
    running = true;
    error = '';
    openDestination = null;
    allDocuments = false;
    openDocuments.clear();
    expanded.clear();
    try {
      const res = await routePreview(JSON.stringify(config), selected);
      if (!res.ok) {
        error = errorText(res.errors);
        own = null;
      } else {
        own = res;
      }
    } catch (e) {
      error = unreported(e);
      own = null;
    } finally {
      running = false;
    }
  }

  /** Every destination the sample would reach, with how many records and the
   * documents each would receive. */
  let reached = $derived.by(() => {
    if (!result) return [];
    const by = new Map<number, string[]>();
    for (const row of result.rows) {
      for (const t of row.through) {
        for (const doc of t.documents) {
          for (const d of doc.destinations) {
            const docs = by.get(d) ?? [];
            docs.push(doc.document);
            by.set(d, docs);
          }
        }
      }
    }
    return [...by.entries()]
      .map(([id, documents]) => ({
        id,
        name: result?.destination_names[id] ?? `destination ${id}`,
        documents,
      }))
      .sort((a, b) => b.documents.length - a.documents.length);
  });

  let unrouted = $derived(result ? result.rows.filter((r) => r.matched.length === 0).length : 0);
  let dropped = $derived(
    result
      ? result.rows.filter(
          (r) => r.matched.length > 0 && !r.through.some((t) => t.documents.length > 0),
        ).length
      : 0,
  );

  /** What a route that sent nothing did with the record, by its answer. A source form goes to a
   * destination once per record, so a later route's deliver answer may find it already sent. */
  const EMPTY_ROUTE: Partial<Record<string, string>> = {
    drop: 'dropped, and counted',
    deliver: 'already sent by an earlier route',
  };

  /** What a record sent, each document with the route it came through and where it went. */
  function sentBy(row: RoutePreviewResult['rows'][number]) {
    return row.through.flatMap((t) =>
      t.documents.filter((d) => d.destinations.length > 0).map((d) => ({ route: t.route, ...d })),
    );
  }

  /** A document as it reads: its fields when it is a JSON object, else its text. */
  function parsed(document: string): Record<string, unknown> | null {
    try {
      const value: unknown = JSON.parse(document);
      return value !== null && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null;
    } catch {
      return null;
    }
  }

  let foldable = $derived((result?.rows ?? []).filter((r) => sentBy(r).length > 0));
  let allOpen = $derived(foldable.length > 0 && foldable.every((r) => expanded.has(r.row)));

  function toggleRow(row: number) {
    if (expanded.has(row)) expanded.delete(row);
    else expanded.add(row);
  }

  function toggleAll() {
    if (allOpen) expanded.clear();
    else for (const r of foldable) expanded.add(r.row);
  }

  function routeName(i: number): string {
    return result?.route_names[i] ?? '';
  }

  // A record's lines are its routes in the order they ran, so the sort orders records and each keeps
  // its lines beneath it; a column sorts a record by what its lines say together.
  const rowSort = new TableSort<'record' | 'route' | 'exceptions' | 'answer' | 'to' | 'sent'>();
  let sortedRows = $derived(
    rowSort.apply(result?.rows ?? [], {
      record: (r) => r.row,
      route: (r) => r.matched.map(routeName).join(', '),
      exceptions: (r) => r.through.reduce((n, t) => n + t.exceptions.length, 0),
      answer: (r) =>
        r.matched.length === 0 && result
          ? nameOf(result.unmatched)
          : r.through.find((t) => t.exceptions.length > 0)?.answer,
      to: (r) =>
        r.matched.length === 0 && result && typeof result.unmatched === 'object'
          ? result.unmatched.deliver.join(', ')
          : [
              ...new Set(
                r.through.flatMap((t) =>
                  t.documents.flatMap((d) =>
                    d.destinations.map((id) => result?.destination_names[id] ?? String(id)),
                  ),
                ),
              ),
            ].join(', '),
      sent: (r) =>
        r.through
          .flatMap((t) => t.documents.filter((d) => d.destinations.length > 0))
          .map((d) => d.document)
          .join(' '),
    }),
  );
</script>

{#snippet whole(document: string)}
  {@const fields = parsed(document)}
  {#if fields}
    <RecordFields before={fields} after={fields} showTypes={false} />
  {:else}
    <div class="mono whole">{document}</div>
  {/if}
{/snippet}

<div class="preview" class:empty={driven && !result && !error}>
  {#if !driven}
    <div class="controls">
      <label for="route-sample"><span class="field-name">Route a sample</span></label>
      <select id="route-sample" bind:value={selected}>
        {#each samples as s (s.id)}
          <option value={s.id}>{s.name} ({s.records})</option>
        {/each}
      </select>
      <Button
        class="primary"
        onclick={run}
        disabled={running ? BUSY : !selected ? 'Choose a sample first.' : !config ? LOADING : ''}
      >
        {running ? 'Running…' : 'Run'}
      </Button>
    </div>

    {#if loaded && samples.length === 0}
      <p>
        No sample names a source. Routing selects on a source, so a sample has to say which one its
        records stand for &mdash; set that on the <a class="link" href="#/build">Samples</a> page.
      </p>
    {/if}
  {/if}

  <InlineError message={error} />

  {#if result}
    {#each result.warnings ?? [] as w (w)}<p class="warn">{w}</p>{/each}
    <div class="tiles">
      {#each reached as d (d.id)}
        <button
          type="button"
          class="tile"
          class:open={openDestination === d.id}
          onclick={() => {
            openDestination = openDestination === d.id ? null : d.id;
            allDocuments = false;
            openDocuments.clear();
          }}
        >
          <span class="count">{d.documents.length}</span>
          <span class="name mono">{d.name}</span>
        </button>
      {/each}
      {#if unrouted > 0}
        <div class="tile flat">
          <span class="count">{unrouted}</span><span class="name">unrouted</span>
        </div>
      {/if}
      {#if dropped > 0}
        <div class="tile flat">
          <span class="count">{dropped}</span><span class="name">dropped</span>
        </div>
      {/if}
      {#if reached.length === 0 && unrouted === 0 && dropped === 0}
        <p class="dim">No records.</p>
      {/if}
    </div>

    {#if openDestination !== null}
      {@const open = reached.find((d) => d.id === openDestination)}
      {#if open}
        <div class="documents">
          <div class="documents-head">
            <strong class="mono">{open.name}</strong>
            <span class="dim"
              >{count(open.documents.length, 'document')}, as they would be sent</span
            >
          </div>
          {#each allDocuments ? open.documents : open.documents.slice(0, SHOWN) as doc, k}
            {@const shown = openDocuments.has(k)}
            <div class="doc">
              <div class="doc-line">
                <button
                  type="button"
                  class="icon"
                  aria-expanded={shown}
                  aria-label={`document ${k + 1}`}
                  onclick={() => (shown ? openDocuments.delete(k) : openDocuments.add(k))}
                  ><ChevronIcon open={shown} /></button
                ><span class="mono ellipsis">{doc}</span>
              </div>
              {#if shown}
                <div class="expanded-cell">{@render whole(doc)}</div>
              {/if}
            </div>
          {/each}
          {#if !allDocuments && open.documents.length > SHOWN}
            <div class="doc dim ellipsis">
              <ShowAll
                more={open.documents.length - SHOWN}
                what="document"
                onshowall={() => (allDocuments = true)}
              />
            </div>
          {/if}
        </div>
      {/if}
    {/if}

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th class="fold-cell"
              >{#if foldable.length > 0}<FoldAllButton
                  icon
                  {allOpen}
                  what="records"
                  onclick={toggleAll}
                />{/if}</th
            >
            <th aria-sort={rowSort.aria('record')}
              ><SortButton sort={rowSort} key="record">Record</SortButton></th
            >
            <th aria-sort={rowSort.aria('route')}
              ><SortButton sort={rowSort} key="route">Route</SortButton></th
            >
            <th aria-sort={rowSort.aria('exceptions')}
              ><SortButton sort={rowSort} key="exceptions">Exceptions</SortButton></th
            >
            <th aria-sort={rowSort.aria('answer')}
              ><SortButton sort={rowSort} key="answer">Answer</SortButton></th
            >
            <th aria-sort={rowSort.aria('to')}
              ><SortButton sort={rowSort} key="to">Goes to</SortButton></th
            >
            <th aria-sort={rowSort.aria('sent')}
              ><SortButton sort={rowSort} key="sent">Sent</SortButton></th
            >
          </tr>
        </thead>
        <tbody>
          {#each sortedRows as row (row.row)}
            {@const sent = sentBy(row)}
            {@const open = expanded.has(row.row)}
            {#if row.matched.length === 0}
              <tr>
                <td></td>
                <td class="mono">{row.row}</td>
                <td><span class="dim">no route matched</span></td>
                <td></td>
                <td class="mono">{nameOf(result.unmatched)}</td>
                <td>
                  {#if row.refused}
                    <span class="dim">{row.refused}</span>
                  {:else if typeof result.unmatched === 'object'}
                    <!-- Not nothing: the deployment's `unmatched` answers for this row, and an empty
                         cell beside "no route matched" reads as lost. -->
                    <span class="mono">{result.unmatched.deliver.join(', ')}</span>
                    <span class="dim">no pipeline</span>
                  {:else if result.unmatched === 'drop'}
                    <span class="dim">dropped, and counted</span>
                  {:else}
                    <span class="dim">blocks the source</span>
                  {/if}
                </td>
                <td></td>
              </tr>
            {/if}
            {#each row.through as t, i}
              <tr class="record-row" class:open>
                <td class="fold-cell">
                  {#if i === 0 && sent.length > 0}
                    <button
                      type="button"
                      class="icon"
                      aria-expanded={open}
                      aria-label={`row ${row.row}`}
                      onclick={() => toggleRow(row.row)}><ChevronIcon {open} /></button
                    >
                  {/if}
                </td>
                <td class="mono">{i === 0 ? row.row : ''}</td>
                <td class="mono">{routeName(t.route)}</td>
                <td>
                  {#each t.exceptions as e}
                    <div>
                      <span class="mono">{e.kind}</span> <span class="dim">{e.message}</span>
                    </div>
                  {/each}
                </td>
                <td class="mono">{t.exceptions.length > 0 ? t.answer : ''}</td>
                <td>
                  {#if t.answer === 'block'}
                    <span class="dim">blocks the source</span>
                  {:else if t.documents.length === 0}
                    <span class="dim">{EMPTY_ROUTE[t.answer] ?? 'dropped by the pipeline'}</span>
                  {/if}
                  {#each t.documents as doc}
                    <div>
                      <span class="mono"
                        >{doc.destinations
                          .map((d) => result?.destination_names[d] ?? d)
                          .join(', ')}</span
                      >
                      {#each Object.entries(doc.destination ?? {}) as [name, value]}<span
                          class="dim">{name} {value}</span
                        >{/each}
                    </div>
                  {/each}
                </td>
                <td class="sent">
                  {#each t.documents as doc}
                    <div class="mono">
                      {#if doc.destinations.length > 0}{doc.document}{:else}<span class="dim"
                          >nothing</span
                        >{/if}
                    </div>
                  {/each}
                </td>
              </tr>
            {/each}
            {#if row.refused && row.matched.length > 0}
              <!-- A record held by a block, or refused for memory: why it went no further. -->
              <tr>
                <td></td>
                <td class="mono">{row.through.length === 0 ? row.row : ''}</td>
                <td class="mono"
                  >{row.through.length === 0 ? row.matched.map(routeName).join(', ') : ''}</td
                >
                <td colspan="4"><span class="dim">{row.refused}</span></td>
              </tr>
            {/if}
            {#if open && sent.length > 0}
              <tr aria-label={`record ${row.row}`}>
                <td class="expanded-cell" colspan="7">
                  {#each sent as doc}
                    <div class="sent-head">
                      <span class="mono">{routeName(doc.route)}</span><span class="mono dim"
                        >{doc.destinations
                          .map((d) => result?.destination_names[d] ?? d)
                          .join(', ')}</span
                      >
                    </div>
                    {@render whole(doc.document)}
                  {/each}
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  .warn {
    color: var(--warn);
  }
  .preview {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 0.9rem;
    margin-bottom: 1.25rem;
  }

  /* Driven and given nothing yet: a panel with no content is a bar across
     the page saying nothing. */
  .preview.empty {
    display: none;
  }
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .controls label {
    font-size: var(--text-label);
    color: var(--dim);
  }
  /* The label register: the field's name. */
  .field-name {
    display: flex;
    align-items: center;
    font-weight: 600;
  }
  select {
    flex-grow: 1;
  }
  .tiles {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .tile {
    display: flex;
    align-items: baseline;
    gap: 0.4rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--bg);
    color: var(--ink);
    padding: 0.35rem 0.6rem;
    font: inherit;
    cursor: pointer;
  }
  .tile.flat {
    cursor: default;
    color: var(--dim);
  }
  .tile.open {
    border-color: var(--ink);
    font-weight: 600;
  }
  .tile .count {
    font-weight: 600;
  }
  .tile .name {
    font-size: var(--text-label);
  }
  .documents {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    margin-bottom: 0.75rem;
    overflow: hidden;
  }
  .documents-head {
    display: flex;
    gap: 0.5rem;
    align-items: baseline;
    background: var(--bg);
    padding: 0.35rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }
  .doc {
    font-size: var(--text-micro);
    padding: 0.2rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }
  .doc:last-child {
    border-bottom: none;
  }
  .doc-line {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    min-width: 0;
  }
  .doc .expanded-cell {
    margin: 0.2rem 0;
    padding: 0.3rem 0.6rem;
    font-size: var(--text-label);
  }
  /* A document that is not a JSON object, whole: it wraps anywhere rather than widening the page. */
  .whole {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
  .sent-head {
    margin: 0.4rem 0 0.2rem;
  }
  .sent-head:first-child {
    margin-top: 0;
  }
  .table-wrap {
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-label);
  }
  th,
  td {
    text-align: left;
    padding: 0.3rem 0.4rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  th {
    color: var(--dim);
    font-weight: 600;
    font-size: var(--text-note);
  }
  /* A document is one line, cut at the cell's edge. */
  td.sent div {
    max-width: 28rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  td .dim + .dim,
  td .mono + .dim {
    margin-left: 0.5rem;
  }
</style>
