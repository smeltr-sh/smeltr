<!--
  Pipelines list: open, delete, or start a pipeline. `PipelineEditor` at
  `#/build/pipelines/:id` is where authoring happens.
-->
<script lang="ts">
  import Button, { BUSY } from './Button.svelte';
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
  import { NOT_IN_DEMO, editable } from './access';
  import {
    sharedConfig,
    listPipelines,
    listTests,
    deletePipeline,
    type PipelineDto,
    type TestSummary,
  } from './api';
  import { Standings } from './standings.svelte';
  import { narrowedHash } from './connections';
  import { count } from './time';
  import TestIcon from './TestIcon.svelte';
  import TestTally from './TestTally.svelte';
  import ExportDialog from './ExportDialog.svelte';
  import ImportDialog from './ImportDialog.svelte';
  import RowMenu from './RowMenu.svelte';
  import MoveDialog, { type MoveTarget } from './MoveDialog.svelte';
  import type { Area } from './areas';

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
  /** The pipeline the export dialog is open for. */
  /** The pipeline being exported, or null. */
  let exporting = $state<{ id: string; name: string } | null>(null);
  let importing = $state(false);
  /** The pipeline being moved into another area (`MoveDialog`). */
  let moving = $state<MoveTarget | null>(null);
  let areas: Area[] = $state([]);

  // This area's tests, and where each stands once run, for the Tests column. Loaded with the pipelines, so a
  // delete or a move counts again. A failure to load them leaves the column without them.
  let tests: TestSummary[] = $state([]);
  const standings = new Standings();
  async function loadTests() {
    try {
      tests = (await listTests()).tests;
    } catch {
      return;
    }
    await standings.run(tests.map((t) => t.id));
  }

  /** Pipeline `id`'s tests: each that runs it, its own and the end-to-end ones whose records it takes. */
  const testIdsOf = (id: string) => tests.filter((t) => t.pipelines.includes(id)).map((t) => t.id);

  // One sort for both bands: each band sorts within itself, so the shared rows stay under their own heading.
  const pipelineSort = new TableSort<'name' | 'steps' | 'tests'>();
  const pipelineColumns = {
    name: (p: PipelineDto) => p.name,
    steps: (p: PipelineDto) => p.steps.length,
    // By how many fail, then by how many there are.
    tests: (p: PipelineDto) =>
      standings.count(testIdsOf(p.id), 'failing') * 1e6 + testIdsOf(p.id).length,
  };
  const narrowing = new Narrowing(() => only);
  const kept = (p: PipelineDto) => narrowing.keeps(p.id);
  let sortedPipelines = $derived(pipelineSort.apply(pipelines.filter(kept), pipelineColumns));
  let sortedSharedPipelines = $derived(
    pipelineSort.apply(sharedPipelines.filter(kept), pipelineColumns),
  );

  async function load() {
    void loadTests();
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
      body: `Delete the pipeline "${name}".`,
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
      <Button type="button" disabled={NOT_IN_DEMO} onclick={() => (importing = true)}>Import</Button
      >
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
      <table class="readings">
        <thead>
          <tr>
            <th aria-sort={pipelineSort.aria('name')}
              ><SortButton sort={pipelineSort} key="name">Name</SortButton></th
            >
            <th aria-sort={pipelineSort.aria('steps')}
              ><SortButton sort={pipelineSort} key="steps">Steps</SortButton></th
            >
            <th aria-sort={pipelineSort.aria('tests')}
              ><SortButton sort={pipelineSort} key="tests">Tests</SortButton></th
            >
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#if sortedSharedPipelines.length > 0}
            <tr class="bandrow"
              ><td class="whole" colspan="4"
                >from shared/ &mdash; every worker group can call or route to these</td
              ></tr
            >
            {#each sortedSharedPipelines as p (p.id)}
              <tr class="fromshared">
                <td
                  >{p.name}
                  <span class="shared-chip"><span class="shared-dot"></span>shared</span></td
                >
                <td class="dim" data-label="Steps">{p.steps.length}</td>
                <td></td>
                <td class="row-actions">
                  <EditInShared hash="#/build/pipelines" />
                </td>
              </tr>
            {/each}
            <tr class="bandrow"><td class="whole" colspan="4">this worker group</td></tr>
          {/if}
          {#each sortedPipelines as p (p.id)}
            {@const ids = testIdsOf(p.id)}
            <tr>
              <td><a href={pipelineHash(p.id)}>{p.name}</a></td>
              <td class="dim" data-label="Steps">{p.steps.length}</td>
              <td data-label="Tests">
                {#if ids.length === 0}
                  <span class="dim">none</span>
                {:else}
                  <a class="glyphed" href={narrowedHash('#/build/tests', ids)}
                    ><TestIcon />
                    {#if standings.of && Object.keys(standings.of).length > 0}
                      <TestTally
                        failing={standings.count(ids, 'failing')}
                        errored={standings.count(ids, 'error')}
                        passing={standings.count(ids, 'passing')}
                      />
                    {:else}{count(ids.length, 'test')}{/if}</a
                  >
                {/if}
              </td>
              <td class="row-actions">
                <RowMenu label="More actions for {p.name || p.id}">
                  <button
                    type="button"
                    class="pick"
                    onclick={() => (exporting = { id: p.id, name: p.name || p.id })}>Export</button
                  >
                  {#if !readOnly && areas.length > 1}
                    <button
                      type="button"
                      class="pick"
                      onclick={() =>
                        (moving = { part: 'pipeline', id: p.id, name: p.name || p.id })}
                      >Move to&hellip;</button
                    >
                  {/if}
                  {#if !readOnly}
                    <Button
                      class="pick remove"
                      onclick={() => remove(p.id, p.name)}
                      disabled={deletingId === p.id ? BUSY : ''}>Delete</Button
                    >
                  {/if}
                </RowMenu>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</SectionShell>
<ExportDialog
  open={exporting !== null}
  pipeline={exporting?.id ?? ''}
  name={exporting?.name ?? ''}
  oncancel={() => (exporting = null)}
/>
<MoveDialog bind:moving bind:areas dirty={false} onmoved={() => void load()} />
<ImportDialog
  open={importing}
  oncancel={() => (importing = false)}
  onimported={() => {
    importing = false;
    void load();
  }}
/>

<style>
  /* What `shared/` gave this group: listed, because the group runs it, and not this page's to change. */
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

  .sample-banner {
    font-size: var(--text-body);
    color: var(--ink);
    margin: 1rem 0 0;
  }

  .toolbar {
    margin: 1.2rem 0;
  }
</style>
