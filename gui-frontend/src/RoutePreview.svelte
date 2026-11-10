<!--
  Route preview: where a sample's records would go, through
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
  import RecordsGrid, { type FindChip, type GridRow, type LeadColumn } from './RecordsGrid.svelte';
  import { untrack } from 'svelte';
  import { unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import RouteDiagram from './RouteDiagram.svelte';
  import { routeCounts } from './routeCounts';
  import SampleMenu from './SampleMenu.svelte';
  import { editable } from './access';
  import { routable, recordsOf, SampleChoices } from './sampleChoices.svelte';
  import {
    heldSession,
    routePreview,
    errorText,
    type FileConfigDto,
    type RoutePreviewResult,
    type SampleRecordDto,
  } from './api';
  import { nameOf } from './exceptions';

  let {
    /** The whole bundle as the editor has it, unsaved edits included. */
    config,
    /** Whether the routes on screen have edits not yet saved. A test locks the routes as saved, so locking waits until
     * they are saved. */
    dirty,
  }: {
    config: FileConfigDto | null;
    dirty: boolean;
  } = $props();

  const choices = new SampleChoices((e) => (error = unreported(e)));
  let offered = $derived(routable(choices));
  let selected = $state('');
  let result = $state<RoutePreviewResult | null>(null);
  /** The sample `result` ran over. */
  let ran = $state('');
  /** The routes `result` ran on. */
  let ranOn = $state('');
  let running = $state(false);
  let error = $state('');
  // Arriving on the tab runs the first choice, once the samples and the bundle are read: nothing is delivered and no
  // destination is opened, so there is no reason to wait for a click.
  $effect(() => {
    if (selected || !offered.first || !config) return;
    selected = offered.first;
    untrack(run);
  });
  let grid = $state<RecordsGrid | undefined>();

  /** Why Lock this as a test is off, or `''` when it is on. A test runs the routes as saved over a saved sample, so it
   * locks a run of exactly those. */
  let lockBlocked = $derived(
    heldSession(selected)
      ? 'A test locks a saved sample. Save your capture as a sample first: Save input as sample, in the pipeline editor.'
      : dirty
        ? 'Save your route changes first.'
        : !result || ran !== selected
          ? 'Choose a sample and run it without errors first.'
          : ranOn !== JSON.stringify(config)
            ? 'The routes changed since this run. Run it again.'
            : '',
  );

  function goLockTest() {
    location.hash = `#/build/tests/lock-e2e/${encodeURIComponent(ran)}`;
  }

  async function run() {
    if (!config || !selected) return;
    running = true;
    error = '';
    try {
      const [sample, sent] = [selected, JSON.stringify(config)];
      const res = await routePreview(sent, sample);
      if (!res.ok) {
        error = errorText(res.errors);
        result = null;
      } else {
        result = res;
        ran = sample;
        ranOn = sent;
      }
    } catch (e) {
      error = unreported(e);
      result = null;
    } finally {
      running = false;
    }
  }

  // The sample's records, the side of each row before its route ran.
  let sampleRecords = $state<SampleRecordDto[]>([]);
  $effect(() => {
    const id = ran;
    sampleRecords = [];
    if (!id) return;
    recordsOf(id).then(
      (records) => {
        if (id === ran) sampleRecords = records;
      },
      (e) => (error = unreported(e)),
    );
  });

  let counts = $derived(result && routeCounts(result));
  let reached = $derived(counts?.reached ?? []);
  let unrouted = $derived(counts?.unrouted ?? 0);
  let dropped = $derived(counts?.dropped.length ?? 0);

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
  type Lead = {
    route: string;
    /** Where the document goes, by destination id. */
    dests: number[];
    to: string;
    toNote?: string;
    answer: string;
    note?: string;
  };

  /** One row for each document a record made on each route, and one for a route or record that
   * made none, saying why. */
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
        const answer = nameOf(result.unmatched);
        const to = typeof result.unmatched === 'object' ? result.unmatched.deliver.join(', ') : '';
        add(
          row.row,
          to ? sample : null,
          {
            route: '',
            dests: [],
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
        if (sent.length === 0) {
          add(
            row.row,
            null,
            {
              route: routeName(t.route),
              dests: [],
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
          add(
            row.row,
            parsed(doc.document),
            {
              route: routeName(t.route),
              dests: doc.destinations,
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
      if (row.refused && row.matched.length > 0) {
        // A record held by a block, or refused for memory: why it went no further.
        add(
          row.row,
          null,
          {
            route: row.matched.map(routeName).join(', '),
            dests: [],
            to: '',
            toNote: row.refused,
            answer: '',
          },
          [],
        );
      }
    }
    return { rows, leads };
  });

  const leadOf = (row: GridRow) => table?.leads.get(row.key);
  const leads: LeadColumn[] = [
    {
      name: 'Route',
      cell: (row) => {
        const l = leadOf(row);
        return l?.note ? { text: '', note: l.note } : { text: l?.route ?? '' };
      },
    },
    {
      name: 'Goes to',
      cell: (row) => ({ text: leadOf(row)?.to ?? '', note: leadOf(row)?.toNote }),
    },
    { name: 'Answer', cell: (row) => ({ text: leadOf(row)?.answer ?? '' }) },
  ];

  /** The find box's chip for a destination's tile. It keeps the documents that destination gets. */
  function toDestination(d: { id: number; name: string }): FindChip {
    return {
      kind: 'rows',
      key: `to ${d.name}`,
      label: `to ${d.name}`,
      failing: false,
      keeps: (row) =>
        table?.leads.get(row.key)?.dests.some((i) => result?.destination_names[i] === d.name) ??
        false,
    };
  }
</script>

<div class="preview">
  <div class="controls">
    <SampleMenu
      samples={offered.samples}
      {selected}
      records={result && ran === selected ? result.rows.length : null}
      held={offered.held}
      onpick={(choice) => (selected = choice)}
    />
    <Button
      class="primary"
      onclick={run}
      disabled={running ? BUSY : !selected ? 'Choose a sample first.' : !config ? LOADING : ''}
    >
      {running ? 'Running…' : 'Run'}
    </Button>
    {#if editable()}
      <span class="grow"></span>
      <Button type="button" onclick={goLockTest} disabled={lockBlocked}>Lock this as a test</Button>
    {/if}
  </div>

  {#if offered.none}
    <p>
      No sample names a source. Routing selects on a source, so a sample has to say which one its
      records stand for &mdash; set that on the <a class="link" href="#/build">Samples</a> page.
    </p>
  {/if}

  <InlineError message={error} />

  {#if result}
    {#each result.warnings ?? [] as w (w)}<p class="warn">{w}</p>{/each}
    {#if counts && result.rows.length > 0}
      <RouteDiagram {result} {counts} sampleName={choices.name(ran)} />
    {/if}
    <div class="tiles">
      {#each reached as d (d.id)}
        <button
          type="button"
          class="tile"
          class:open={grid?.hasChip(toDestination(d))}
          aria-pressed={grid?.hasChip(toDestination(d)) ?? false}
          onclick={() => grid?.toggleChip(toDestination(d))}
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

    <!-- The field choice and recent expressions are remembered under the empty id, which no
         pipeline can take. -->
    <RecordsGrid bind:this={grid} rows={table?.rows ?? null} remember="" openOn={result} {leads} />
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

  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
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
