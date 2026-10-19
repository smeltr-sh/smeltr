<!--
  Pipelines list: open, delete, or start a pipeline. `PipelineEditor` at
  `#/build/pipelines/:id` is where authoring happens.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import EditInShared from './EditInShared.svelte';
  import OnlyNotice from './OnlyNotice.svelte';
  import { Narrowing } from './narrowing.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import { ask } from './Confirm.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import { editable } from './access';
  import { sharedConfig, listPipelines, deletePipeline, type PipelineDto } from './api';

  // Carried from a sample row's "Run in a pipeline" link (`nav.ts`'s `for`
  // route). Not validated against the sample library here -- a stale link
  // just carries a name nothing opens against, same as it would look up.
  let { sample = '', only }: { sample?: string; only?: string[] } = $props();

  const readOnly = !editable();

  /** The hash this pipeline's row should link to: `sample`, if carried,
   * follows the id so the editor opens previewing against it. */
  function pipelineHash(id: string): string {
    const base = `#/build/pipelines/${encodeURIComponent(id)}`;
    return sample ? `${base}/for/${encodeURIComponent(sample)}` : base;
  }

  let newPipelineHash = $derived(
    sample ? `#/build/pipelines/new/for/${encodeURIComponent(sample)}` : '#/build/pipelines/new',
  );

  let pipelines: PipelineDto[] = $state([]);
  /** `shared/`'s own, which this group runs too. Listed here and edited where they live: a pipeline in
   * `shared/` is every group's, and this page writes one scope. */
  let sharedPipelines: PipelineDto[] = $state([]);
  let loading = $state(true);
  let loadFailed = $state(false);
  // Something else in the bundle does not validate; the list is complete.
  let bundleProblems = $state<string[]>([]);
  let deletingId = $state('');

  // One sort for both bands: each band sorts within itself, so the shared rows stay under their own heading.
  const pipelineSort = new TableSort<'name' | 'steps'>();
  const pipelineColumns = {
    name: (p: PipelineDto) => p.name,
    steps: (p: PipelineDto) => p.steps.length,
  };
  const narrowing = new Narrowing(() => only);
  const kept = (p: PipelineDto) => narrowing.keeps(p.id);
  let sortedPipelines = $derived(pipelineSort.apply(pipelines.filter(kept), pipelineColumns));
  let sortedSharedPipelines = $derived(
    pipelineSort.apply(sharedPipelines.filter(kept), pipelineColumns),
  );

  async function load() {
    loading = true;
    loadFailed = false;
    bundleProblems = [];
    try {
      const res = await listPipelines();
      pipelines = res.pipelines;
      if (!res.ok) bundleProblems = res.errors.map((e) => e.message);
      sharedPipelines = (await sharedConfig())?.pipelines ?? [];
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  async function remove(id: string, name: string) {
    const ok = await ask({
      title: 'delete pipeline',
      body: `Delete the pipeline "${name}". This cannot be undone.`,
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    deletingId = id;
    try {
      const res = await deletePipeline(id);
      if (res.ok) {
        await load();
      }
    } catch (e) {
      reported(e);
    } finally {
      deletingId = '';
    }
  }
</script>

<SectionShell>
  <PageHeader title="Pipelines">
    {#snippet help()}
      <p>
        A pipeline is a list of steps that reshapes each record; it runs on live data only once a
        route uses it.
      </p>
    {/snippet}
  </PageHeader>

  {#if sample}
    <p class="sample-banner">
      Opening a pipeline here previews it against <strong>{sample}</strong>.
    </p>
  {/if}

  <OnlyNotice {narrowing} ids={[...pipelines, ...sharedPipelines].map((p) => p.id)} />

  {#if !readOnly}
    <div class="toolbar">
      <a class="btn primary" href={newPipelineHash}>+ Add pipeline</a>
    </div>
  {/if}

  <UnreadableFiles problems={bundleProblems} onchanged={load} />

  {#if loading}
    <p class="dim">Loading…</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else if pipelines.length === 0}
    {#if sample && !readOnly}
      <p class="dim">
        No pipelines yet. <a href={newPipelineHash}>Create one</a> to run <strong>{sample}</strong>
        through it.
      </p>
    {:else}
      <p class="dim">No pipelines yet.</p>
    {/if}
  {:else}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th aria-sort={pipelineSort.aria('name')}
              ><SortButton sort={pipelineSort} key="name">Name</SortButton></th
            >
            <th aria-sort={pipelineSort.aria('steps')}
              ><SortButton sort={pipelineSort} key="steps">Steps</SortButton></th
            >
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#if sortedSharedPipelines.length > 0}
            <tr class="bandrow"><td colspan="3">from shared/ &mdash; every group runs these</td></tr
            >
            {#each sortedSharedPipelines as p (p.id)}
              <tr class="fromshared">
                <td
                  >{p.name}
                  <span class="shared-chip"><span class="shared-dot"></span>shared</span></td
                >
                <td class="dim">{p.steps.length}</td>
                <td class="row-actions">
                  <EditInShared hash="#/build/pipelines" />
                </td>
              </tr>
            {/each}
            <tr class="bandrow"><td colspan="3">this group</td></tr>
          {/if}
          {#each sortedPipelines as p (p.id)}
            <tr>
              <td><a href={pipelineHash(p.id)}>{p.name}</a></td>
              <td class="dim">{p.steps.length}</td>
              <td class="row-actions">
                {#if !readOnly}
                  <button
                    type="button"
                    class="compact"
                    onclick={() => remove(p.id, p.name)}
                    disabled={deletingId === p.id}
                  >
                    {deletingId === p.id ? 'Deleting…' : 'Delete'}
                  </button>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</SectionShell>

<style>
  /* What `shared/` gave this group: listed, because the group runs it, and not this page's to change. */
  .bandrow td {
    padding-top: 0.9rem;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
  }

  tr.fromshared td {
    color: var(--dim);
  }

  .shared-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.28rem;
    border: 1px solid
      color-mix(
        in oklch,
        oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260)) 45%,
        var(--border)
      );
    border-radius: 999px;
    padding: 0.02rem 0.45rem;
    font-size: var(--text-micro);
    color: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
    white-space: nowrap;
  }

  .shared-dot {
    width: 7px;
    height: 7px;
    border-radius: 999px;
    background: oklch(var(--scope-l) var(--scope-c) var(--shared-h, 260));
  }

  .dim {
    color: var(--dim);
  }

  .sample-banner {
    font-size: var(--text-body);
    color: var(--ink);
    margin: 1rem 0 0;
  }

  .toolbar {
    margin: 1.2rem 0;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }

  th {
    text-align: left;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    padding: 0.4rem 0.6rem;
  }

  td {
    padding: 0.5rem 0.6rem;
    border-bottom: 1px solid var(--border);
  }

  .row-actions {
    text-align: right;
  }
</style>
