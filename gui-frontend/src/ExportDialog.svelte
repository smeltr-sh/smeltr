<!--
  Exporting one pipeline: each object the file can hold, by kind and id, ticked to be exported, and the link that
  downloads what is ticked. The routes toggle adds the routes that run the pipeline, with their sources and destinations,
  and asks the leader again. A sample goes with the tests that read it, and a secret or certificate with what names
  it, so neither has a tick of its own.
-->
<script lang="ts">
  import Button from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import Toggle from './Toggle.svelte';
  import {
    exportContents,
    exportUrl,
    type ExportContents,
    type ImportPart,
    type Named,
  } from './api';
  import { bytes } from './bytes';
  import { reported } from './errors.svelte';

  let {
    open,
    pipeline,
    name,
    oncancel,
  }: { open: boolean; pipeline: string; name: string; oncancel: () => void } = $props();

  let routes = $state(false);
  let contents = $state<Partial<ExportContents> | null>(null);
  /** The objects not to export, by `key`. Every object is exported until it is unticked. */
  let leftOut = $state<Record<string, boolean>>({});

  $effect(() => {
    if (open) void load(pipeline, routes);
  });

  $effect(() => {
    if (open) leftOut = {};
  });

  async function load(id: string, withRoutes: boolean) {
    contents = null;
    try {
      const res = await exportContents(id, withRoutes);
      if (res.ok && id === pipeline && withRoutes === routes) contents = res;
    } catch (e) {
      reported(e);
    }
  }

  const PARTS: Record<ImportPart, string> = {
    pipeline: 'Pipeline',
    lookupTable: 'Lookup table',
    grokPattern: 'Grok pattern',
    schema: 'Schema',
    sample: 'Sample',
    test: 'Test',
    route: 'Route',
    source: 'Source',
    destination: 'Destination',
  };

  type Row = { part: ImportPart; id: string; shown: string };
  const key = (r: { part: ImportPart; id: string }) => `${r.part}\u0000${r.id}`;
  const plain = (part: ImportPart, ids: string[] | undefined): Row[] =>
    (ids ?? []).map((id) => ({ part, id, shown: id }));
  /** A pipeline or route is shown by its name: its id is a key nobody chose. */
  const named = (part: ImportPart, items: Named[] | undefined): Row[] =>
    (items ?? []).map(({ id, name }) => ({ part, id, shown: name }));

  /** Each object that can be ticked, in the order an import lists them. A lookup table is shown with its file's name as
   * uploaded, without the content hash prefix the leader stores it under, and the file's size. */
  let rows = $derived<Row[]>(
    contents === null
      ? []
      : [
          ...named('pipeline', contents.pipelines),
          ...(contents.lookupTables ?? []).map((t) => ({
            part: 'lookupTable' as const,
            id: t.id,
            shown:
              t.file === undefined
                ? t.id
                : `${t.id} (${t.file.replace(/^lookups\/([0-9a-f]{16}-)?/, '')}, ${bytes(t.bytes ?? 0)})`,
          })),
          ...plain('grokPattern', contents.grokPatterns),
          ...plain('schema', contents.schemas),
          ...plain(
            'test',
            (contents.tests ?? []).map((t) => t.id),
          ),
          ...named('route', contents.routes),
          ...plain('source', contents.sources),
          ...plain('destination', contents.destinations),
        ],
  );
  /** The samples the ticked tests read. */
  let samples = $derived([
    ...new Set(
      (contents?.tests ?? [])
        .filter((t) => !leftOut[key({ part: 'test', id: t.id })])
        .map((t) => t.sample),
    ),
  ]);
  /** The secrets and certificates a ticked source or destination names. */
  let credentials = $derived.by(() => {
    const by = new Set(rows.filter((r) => !leftOut[key(r)]).map((r) => `${PARTS[r.part]} ${r.id}`));
    return (contents?.credentials ?? []).filter((c) => c.namedBy.some((n) => by.has(n)));
  });
  let unticked = $derived(
    rows.filter((r) => leftOut[key(r)]).map((r) => ({ part: r.part, id: r.id })),
  );
</script>

<Dialog {open} title="export {name}" wide {oncancel}>
  <div class="body">
    <Field label="Include routes, sources and destinations">
      {#snippet help()}Secrets and certificates go by name only. Importing matches each name to one
        on the other leader.{/snippet}
      <Toggle bind:checked={routes} ariaLabel="Include routes, sources and destinations" />
    </Field>
    {#if contents === null}
      <p class="dim">Loading…</p>
    {:else}
      <table>
        <thead>
          <tr><th>Kind</th><th>Name</th><th>Export</th></tr>
        </thead>
        <tbody>
          {#each rows as r (key(r))}
            <tr>
              <td>{PARTS[r.part]}</td>
              <td class="mono">{r.shown}</td>
              <td>
                <input
                  type="checkbox"
                  aria-label="Export {PARTS[r.part]} {r.shown}"
                  checked={!leftOut[key(r)]}
                  onchange={(e) => (leftOut[key(r)] = !e.currentTarget.checked)}
                />
              </td>
            </tr>
          {/each}
          {#each samples as id (id)}
            <tr>
              <td>Sample</td>
              <td class="mono">{id}</td>
              <td class="dim">With its tests</td>
            </tr>
          {/each}
          {#each credentials as c (`${c.certificate}\u0000${c.name}`)}
            <tr>
              <td>{c.certificate ? 'Certificate' : 'Secret'}</td>
              <td class="mono">{c.name}</td>
              <td class="dim">Name only</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel}>Cancel</Button>
    {#if contents !== null && unticked.length === rows.length}
      <Button type="button" class="primary" disabled="Tick something to export first."
        >Download</Button
      >
    {:else}
      <a
        class="btn primary"
        href={exportUrl(pipeline, routes, unticked)}
        download
        onclick={oncancel}>Download</a
      >
    {/if}
  {/snippet}
</Dialog>

<style>
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  th {
    text-align: left;
  }
</style>
