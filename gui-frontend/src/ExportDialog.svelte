<!--
  Exporting one pipeline: what the file will hold, by kind and id, and the link that downloads it. Two toggles:
  one, off at first, includes the routes that run the pipeline, with their sources and destinations; the other, on
  at first, includes the tests and samples. Each change asks the leader again.
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
  let tests = $state(true);
  let contents = $state<Partial<ExportContents> | null>(null);

  $effect(() => {
    if (open) void load(pipeline, routes, tests);
  });

  async function load(id: string, withRoutes: boolean, withTests: boolean) {
    contents = null;
    try {
      const res = await exportContents(id, { routes: withRoutes, tests: withTests });
      if (res.ok && id === pipeline && withRoutes === routes && withTests === tests) contents = res;
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

  /** Each kind the export holds any of, with its items on one line. A table is listed with its file's name as
   * uploaded, without the content hash prefix the leader stores it under, and the file's size. */
  let rows = $derived(
    contents === null
      ? []
      : KINDS.flatMap(([key, label]) => {
          const items = (contents?.[key] ?? []).map((item) =>
            typeof item === 'string'
              ? item
              : item.file === undefined
                ? item.id
                : `${item.id} (${item.file.replace(/^lookups\/([0-9a-f]{16}-)?/, '')}, ${bytes(item.bytes ?? 0)})`,
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
    <Field label="Include tests and samples">
      <Toggle bind:checked={tests} ariaLabel="Include tests and samples" />
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
    <a class="btn primary" href={exportUrl(pipeline, { routes, tests })} download onclick={oncancel}
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
