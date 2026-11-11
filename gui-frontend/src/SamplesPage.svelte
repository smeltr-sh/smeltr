<!--
  Samples: records under a name, saved beside the config they are authored
  against (`config_file::sample`). A paste and an uploaded file both land
  as one, so a preview always runs over something saved, named, and in the
  branch's history rather than over whatever was last typed into a box.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY } from './Button.svelte';
  import { reported, unreported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import { ask } from './Confirm.svelte';
  import AddSampleDialog from './AddSampleDialog.svelte';
  import SampleEditor from './SampleEditor.svelte';
  import UnreadableFiles from './UnreadableFiles.svelte';
  import SortButton from './SortButton.svelte';
  import { TableSort } from './sort.svelte';
  import { RESOURCES_TABS } from './nav';
  import { checksOf } from './objectChanges';
  import { dateTime } from './time';
  import { editable } from './access';
  import { nameOf, pipelineNamesAt, type PipelineNames } from './pipelineNames';
  import {
    listSamples,
    deleteSample,
    listTests,
    runTests,
    deleteTest,
    currentBranch,
    currentScope,
    errorText,
    capturedAt,
    type SampleSummary,
    type TestSummary,
    type TestResult,
  } from './api';

  const readOnly = !editable();

  let samples: SampleSummary[] = $state([]);
  let loading = $state(true);
  let loadFailed = $state(false);
  // A file under `samples/` that did not parse; the rest still list.
  let loadWarnings = $state<string[]>([]);

  const saving = new Busy();

  let adding = $state(false);

  // The sample open for editing, by id.
  let editing: string | null = $state(null);

  let tests: TestSummary[] = $state([]);
  let testsLoading = $state(true);
  let testsRunError = $state('');
  // Keyed by test id; absent while its run is still in flight.
  let results: Record<string, TestResult> = $state({});
  const deletingTest = new Busy();

  let pipelineNames: PipelineNames = $state({});
  pipelineNamesAt(currentBranch(), currentScope()).then((names) => (pipelineNames = names));

  const sampleSort = new TableSort<'name' | 'source' | 'records' | 'captured'>();
  let sortedSamples = $derived(
    sampleSort.apply(samples, {
      name: (s) => s.name,
      source: (s) => s.source ?? 'pasted',
      records: (s) => s.records,
      captured: (s) => capturedAt(s) ?? 0,
    }),
  );

  /** The sample name a test row shows: its id when the sample is gone. */
  function sampleNameOf(t: TestSummary): string {
    return samples.find((s) => s.id === t.sample)?.name ?? t.sample;
  }

  const testSort = new TableSort<'name' | 'pipelines' | 'sample' | 'checks'>();
  let sortedTests = $derived(
    testSort.apply(tests, {
      name: (t) => t.name,
      pipelines: (t) => t.pipelines.map((id) => nameOf(pipelineNames, id)).join(', '),
      sample: sampleNameOf,
      checks: checksOf,
    }),
  );

  async function loadTests() {
    testsLoading = true;
    try {
      const res = await listTests();
      tests = res.tests;
    } catch (e) {
      reported(e);
    } finally {
      testsLoading = false;
    }
    if (!readOnly) runAllTests();
  }
  loadTests();

  async function runAllTests() {
    testsRunError = '';
    try {
      const res = await runTests({});
      if (!res.ok) testsRunError = errorText(res.errors ?? []);
      const byId: Record<string, TestResult> = {};
      for (const r of res.results) byId[r.id] = r;
      results = byId;
    } catch (e) {
      testsRunError = unreported(e);
    }
  }

  async function removeTest(t: TestSummary) {
    const ok = await ask({
      title: 'delete test',
      body: `Delete the test "${t.name}".`,
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    await deletingTest.run(async () => {
      const res = await deleteTest(t.id);
      if (res.ok) await loadTests();
    });
  }

  async function load() {
    loading = true;
    loadFailed = false;
    loadWarnings = [];
    try {
      const res = await listSamples();
      samples = res.samples;
      if (!res.ok) loadWarnings = res.errors.map((e) => e.message);
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  /** The names of the tests run over sample `id`, which an edit to it changes the input of. */
  function testsOn(id: string): string[] {
    return tests.filter((t) => t.sample === id).map((t) => t.name);
  }

  async function remove(s: SampleSummary) {
    const ok = await ask({
      title: 'delete sample',
      body: `Delete the sample "${s.name}".`,
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    await saving.run(async () => {
      const res = await deleteSample(s.id);
      if (res.ok) await load();
    });
  }
</script>

<SectionShell>
  <PageHeader title="Samples">
    {#snippet help()}
      A sample is a saved set of records that a pipeline preview runs against.
    {/snippet}
  </PageHeader>
  <SecondaryTabs tabs={RESOURCES_TABS} active="#/build" ariaLabel="Resources" />

  {#if !readOnly}
    <div class="toolbar">
      <Button class="primary" onclick={() => (adding = true)} disabled={saving.on ? BUSY : ''}
        >+ Add sample</Button
      >
    </div>
  {/if}

  <AddSampleDialog
    open={adding}
    oncancel={() => (adding = false)}
    onsaved={() => {
      adding = false;
      void load();
    }}
  />

  <UnreadableFiles problems={loadWarnings} onchanged={load}>
    {#snippet rest(left)}
      <p class="warn">{left.join('; ')}</p>
    {/snippet}
  </UnreadableFiles>

  {#if loading}
    <p class="dim">Loading…</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else if samples.length === 0}
    <p class="dim">
      No samples yet. Paste some records or upload a file; a pipeline is previewed against one.
    </p>
  {:else}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th aria-sort={sampleSort.aria('name')}
              ><SortButton sort={sampleSort} key="name">Name</SortButton></th
            >
            <th aria-sort={sampleSort.aria('source')}
              ><SortButton sort={sampleSort} key="source">From</SortButton></th
            >
            <th aria-sort={sampleSort.aria('records')}
              ><SortButton sort={sampleSort} key="records">Records</SortButton></th
            >
            <th aria-sort={sampleSort.aria('captured')}
              ><SortButton sort={sampleSort} key="captured">Captured</SortButton></th
            >
            <th></th><th></th><th></th>
          </tr>
        </thead>
        <tbody>
          {#each sortedSamples as s (s.id)}
            <tr>
              <td><strong>{s.name}</strong></td>
              <td class="mono dim">{s.source ?? 'pasted'}</td>
              <td class="mono">{s.records}</td>
              <td class="dim"
                >{#if capturedAt(s) !== undefined}{dateTime(capturedAt(s)! / 1000)}{/if}</td
              >
              <td class="right">
                <a class="btn" href={`#/build/pipelines/for/${encodeURIComponent(s.id)}`}
                  >Run in a pipeline</a
                >
              </td>
              <td class="right">
                {#if !readOnly}
                  <Button
                    onclick={() => (editing = editing === s.id ? null : s.id)}
                    disabled={saving.on ? BUSY : ''}>Edit</Button
                  >
                {/if}
              </td>
              <td class="right">
                {#if !readOnly}
                  <Button onclick={() => remove(s)} disabled={saving.on ? BUSY : ''}>Delete</Button>
                {/if}
              </td>
            </tr>
            {#if editing === s.id}
              <tr class="open">
                <td colspan="7">
                  <SampleEditor
                    id={s.id}
                    tests={testsOn(s.id)}
                    oncancel={() => (editing = null)}
                    onsaved={() => {
                      editing = null;
                      void load();
                      void loadTests();
                    }}
                  />
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>
  {/if}

  <div class="card tests-card">
    <div class="row">
      <h2>Tests</h2>
      <span class="dim">{tests.length}</span>
    </div>
    {#if testsRunError}
      <p class="warn">{testsRunError}</p>
    {/if}
    {#if testsLoading}
      <p class="dim">Loading…</p>
    {:else if tests.length === 0}
      <p class="dim">
        No tests locked yet. <a href="#/build/pipelines">Run a pipeline</a> against a sample and lock
        what it produces.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th aria-sort={testSort.aria('name')}
                ><SortButton sort={testSort} key="name">Test</SortButton></th
              >
              <th aria-sort={testSort.aria('pipelines')}
                ><SortButton sort={testSort} key="pipelines">Pipelines</SortButton></th
              >
              <th aria-sort={testSort.aria('sample')}
                ><SortButton sort={testSort} key="sample">Sample</SortButton></th
              >
              <th aria-sort={testSort.aria('checks')}
                ><SortButton sort={testSort} key="checks">Checks</SortButton></th
              >
              <th></th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each sortedTests as t (t.id)}
              <tr>
                <td><strong>{t.name}</strong></td>
                <td class="mono dim"
                  >{t.pipelines.map((id) => nameOf(pipelineNames, id)).join(', ') || '—'}</td
                >
                <td class="mono dim">{sampleNameOf(t)}</td>
                <td class="mono">{checksOf(t)}</td>
                <td>
                  {#if !results[t.id]}
                    <span class="dim">running…</span>
                  {:else if results[t.id].error}
                    <span class="warn" title={results[t.id].error}>could not run</span>
                  {:else if results[t.id].verdict?.passed}
                    <span class="passing">Passing</span>
                  {:else}
                    <span class="failing">Failing</span>
                  {/if}
                </td>
                <td class="right">
                  {#if !readOnly}
                    <Button onclick={() => removeTest(t)} disabled={deletingTest.on ? BUSY : ''}>
                      Delete
                    </Button>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</SectionShell>

<style>
  .toolbar {
    display: flex;
    gap: 0.5rem;
    margin: 1.2rem 0;
  }
  .warn {
    color: var(--warn);
  }
  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 0.9rem;
    margin-bottom: 1.25rem;
  }
  .tests-card {
    padding: 0;
    overflow: hidden;
  }
  .tests-card table {
    margin: 0;
  }
  .table-wrap {
    overflow-x: auto;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 0.9rem;
    background: var(--bg);
    border-bottom: 1px solid var(--border);
  }
  .row h2 {
    flex-grow: 1;
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }
  .tests-card p {
    margin: 0;
    padding: 0.75rem 0.9rem;
  }
  .passing {
    color: var(--added);
    font-weight: 600;
  }
  .failing {
    color: var(--error);
    font-weight: 600;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-body);
  }
  th,
  td {
    text-align: left;
    padding: 0.4rem 0.5rem;
    border-bottom: 1px solid var(--border);
  }
  th {
    color: var(--dim);
    font-weight: 600;
    font-size: var(--text-label);
  }
  .right {
    text-align: right;
  }
  /* The sample open for editing, folded open under its row (STYLE_GUIDE.md, "Lists"). */
  tr.open {
    background: var(--panel);
    box-shadow: inset 3px 0 0 var(--accent);
  }
</style>
