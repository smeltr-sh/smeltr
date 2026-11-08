<!--
  Route preview: where a saved sample's records would go, through
  `POST /api/areas/:area/preview/routes` (`runtime::preview_routes`). The routes it runs
  are the ones on screen, saved or not, so this answers what the edit in
  front of you would do.

  Nothing is delivered and no destination exists. A count here is what the
  delivery rules chose, and a document is what would have been sent. The documents are drawn
  as a pipeline's preview draws its records (`RecordsGrid`): one row for each document a
  record made on each route, beside the record as the sample holds it.
-->
<script lang="ts">
  import Button, { BUSY, LOADING } from './Button.svelte';
  import RecordsGrid, { type GridRow } from './RecordsGrid.svelte';
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import {
    getSample,
    listSamples,
    routePreview,
    errorText,
    type FileConfigDto,
    type RoutePreviewResult,
    type SampleRecordDto,
    type SampleSummary,
  } from './api';
  import { nameOf } from './exceptions';

  let {
    /** The whole bundle as the editor has it, unsaved edits included. */
    config,
    /** A run somebody else made. Given one, this shows it and picks nothing;
     * given none, it picks a sample and runs for itself. Two of these on a
     * screen must not be two pickers over two runs of the same thing. */
    result: given,
    /** The sample `result` ran over, by id. */
    sample: givenSample = '',
  }: {
    config: FileConfigDto | null;
    result?: RoutePreviewResult | null;
    sample?: string;
  } = $props();

  let samples: SampleSummary[] = $state([]);
  // Until the list arrives, no sample is not the same as none naming a source.
  let loaded = $state(false);
  let selected = $state('');
  let own = $state<RoutePreviewResult | null>(null);
  /** The sample `own` ran over. */
  let ran = $state('');
  let driven = $derived(given !== undefined);
  let result = $derived(driven ? (given ?? null) : own);
  let running = $state(false);
  let error = $state('');
  /** The destination whose documents alone the grid shows, by id. */
  let openDestination = $state<number | null>(null);

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
    try {
      const sample = selected;
      const res = await routePreview(JSON.stringify(config), sample);
      if (!res.ok) {
        error = errorText(res.errors);
        own = null;
      } else {
        own = res;
        ran = sample;
      }
    } catch (e) {
      error = unreported(e);
      own = null;
    } finally {
      running = false;
    }
  }

  // The sample's records, the side of each row before its route ran.
  let sampleRecords = $state<SampleRecordDto[]>([]);
  let sampleId = $derived(driven ? givenSample : ran);
  $effect(() => {
    const id = sampleId;
    sampleRecords = [];
    if (!id) return;
    getSample(id).then(
      (res) => {
        if (id === sampleId) sampleRecords = res.sample?.records ?? [];
      },
      (e) => (error = unreported(e)),
    );
  });

  /** Every destination the sample would reach, with how many documents each would receive. */
  let reached = $derived.by(() => {
    if (!result) return [];
    const by = new Map<number, number>();
    for (const row of result.rows)
      for (const t of row.through)
        for (const doc of t.documents)
          for (const d of doc.destinations) by.set(d, (by.get(d) ?? 0) + 1);
    return [...by.entries()]
      .map(([id, documents]) => ({
        id,
        name: result?.destination_names[id] ?? `destination ${id}`,
        documents,
      }))
      .sort((a, b) => b.documents - a.documents);
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

  function routeName(i: number): string {
    return result?.route_names[i] ?? '';
  }

  /** A document as the grid reads it: its fields when it is a JSON object, else its text as
   * `_raw`, the field a source's raw text arrives in. */
  function parsed(document: string): Record<string, unknown> {
    try {
      const value: unknown = JSON.parse(document);
      if (value !== null && typeof value === 'object' && !Array.isArray(value))
        return value as Record<string, unknown>;
    } catch {
      // Not JSON: its text.
    }
    return { _raw: document };
  }

  /** What a row says beside its fields: the route, where the document goes and the answer. */
  type Lead = { route: string; to: string; toNote?: string; answer: string; note?: string };

  /** One row for each document a record made on each route, and one for a route or record that
   * made none, saying why. The destination tile that is open keeps only the documents it gets. */
  let table = $derived.by(() => {
    if (!result) return null;
    const leads = new Map<number, Lead>();
    const rows: GridRow[] = [];
    const add = (record: number, after: unknown, lead: Lead, exceptions: GridRow['exceptions']) => {
      const key = rows.length;
      leads.set(key, lead);
      rows.push({
        key,
        label: String(record),
        before: sampleRecords[record] ?? {},
        after,
        dropped: after === null,
        exceptions,
      });
    };
    const names = (ids: number[]) =>
      ids.map((d) => result?.destination_names[d] ?? String(d)).join(', ');
    for (const row of result.rows) {
      const sample = sampleRecords[row.row] ?? {};
      if (row.matched.length === 0) {
        if (openDestination !== null) continue;
        const answer = nameOf(result.unmatched);
        const to = typeof result.unmatched === 'object' ? result.unmatched.deliver.join(', ') : '';
        add(
          row.row,
          to ? sample : null,
          {
            route: '',
            to,
            toNote: row.refused
              ? row.refused
              : to
                ? 'no pipeline'
                : result.unmatched === 'drop'
                  ? 'dropped, and counted'
                  : 'blocks the source',
            answer,
            note: 'no route matched',
          },
          [],
        );
        continue;
      }
      for (const t of row.through) {
        const exceptions = t.exceptions.map((e) => ({ message: `${e.kind}: ${e.message}` }));
        const answer = t.exceptions.length > 0 ? t.answer : '';
        const sent = t.documents.filter((d) => d.destinations.length > 0);
        if (sent.length === 0 && openDestination === null) {
          add(
            row.row,
            null,
            {
              route: routeName(t.route),
              to: '',
              toNote:
                t.answer === 'block'
                  ? 'blocks the source'
                  : (EMPTY_ROUTE[t.answer] ?? 'dropped by the pipeline'),
              answer,
            },
            exceptions,
          );
        }
        for (const doc of sent) {
          if (openDestination !== null && !doc.destinations.includes(openDestination)) continue;
          add(
            row.row,
            parsed(doc.document),
            {
              route: routeName(t.route),
              to: names(doc.destinations),
              toNote: Object.entries(doc.destination ?? {})
                .map(([name, value]) => `${name} ${value}`)
                .join(' '),
              answer,
            },
            exceptions,
          );
        }
      }
      if (row.refused && row.matched.length > 0 && openDestination === null) {
        // A record held by a block, or refused for memory: why it went no further.
        add(
          row.row,
          null,
          { route: row.matched.map(routeName).join(', '), to: '', toNote: row.refused, answer: '' },
          [],
        );
      }
    }
    return { rows, leads };
  });
</script>

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
          aria-pressed={openDestination === d.id}
          onclick={() => (openDestination = openDestination === d.id ? null : d.id)}
        >
          <span class="count">{d.documents}</span>
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

    <RecordsGrid
      rows={table?.rows ?? null}
      remember="route-preview"
      openOn={result}
      leadWidths={['14rem', '14rem', '6rem']}
    >
      {#snippet leadHead()}
        <th class="cell">Route</th>
        <th class="cell">Goes to</th>
        <th class="cell">Answer</th>
      {/snippet}
      {#snippet lead(row)}
        {@const l = table?.leads.get(row.key)}
        <td class="cell ellipsis"
          >{#if l?.note}<span class="dim">{l.note}</span>{:else}{l?.route}{/if}</td
        >
        <td class="cell ellipsis"
          >{l?.to}{#if l?.toNote}{l.to ? ' ' : ''}<span class="dim">{l.toNote}</span>{/if}</td
        >
        <td class="cell ellipsis">{l?.answer}</td>
      {/snippet}
    </RecordsGrid>
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
</style>
