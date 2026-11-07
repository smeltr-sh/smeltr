<!--
  Exporting one pipeline: what the file will hold, by kind and id, and the link that downloads it. The toggle
  includes the routes that run the pipeline, with their sources and destinations, and asks the leader again.
-->
<script lang="ts">
  import Button from './Button.svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';
  import Toggle from './Toggle.svelte';
  import { exportContents, exportUrl, type ExportContents } from './api';
  import { bytes } from './bytes';
  import { reported } from './errors.svelte';

  let { open, pipeline, oncancel }: { open: boolean; pipeline: string; oncancel: () => void } =
    $props();

  let routes = $state(false);
  let contents = $state<Partial<ExportContents> | null>(null);

  $effect(() => {
    if (open) void load(pipeline, routes);
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

  const KINDS: [keyof ExportContents, string][] = [
    ['pipelines', 'Pipelines'],
    ['lookupTables', 'Lookup tables'],
    ['grokPatterns', 'Grok patterns'],
    ['schemas', 'Schemas'],
    ['samples', 'Samples'],
    ['tests', 'Tests'],
    ['routes', 'Routes'],
    ['sources', 'Sources'],
    ['destinations', 'Destinations'],
    ['secrets', 'Secrets'],
    ['certificates', 'Certificates'],
  ];

  /** Each kind the export holds any of, with its items as one line. A table names its file and that file's size. */
  let rows = $derived(
    contents === null
      ? []
      : KINDS.flatMap(([key, label]) => {
          const items = (contents?.[key] ?? []).map((item) =>
            typeof item === 'string'
              ? item
              : item.file === undefined
                ? item.id
                : `${item.id} (${item.file.replace(/^lookups\//, '')}, ${bytes(item.bytes ?? 0)})`,
          );
          return items.length > 0 ? [[label, items.join(', ')] as const] : [];
        }),
  );
</script>

<Dialog {open} title="export {pipeline}" wide {oncancel}>
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
        <tbody>
          {#each rows as [label, items] (label)}
            <tr>
              <th scope="row">{label}</th>
              <td class="mono">{items}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    {/if}
  </div>
  {#snippet actions()}
    <Button type="button" onclick={oncancel}>Cancel</Button>
    <a class="btn primary" href={exportUrl(pipeline, routes)} download onclick={oncancel}
      >Download</a
    >
  {/snippet}
</Dialog>

<style>
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.9rem;
  }
  .mono {
    font-family: var(--mono);
  }
  th {
    text-align: left;
    white-space: nowrap;
    width: 1%;
    padding-right: 1.5rem;
  }
</style>
