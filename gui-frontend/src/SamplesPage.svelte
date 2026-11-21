<!--
  Samples: records under a name, saved beside the config they are authored
  against (`config_file::sample`). A paste and an uploaded file both land
  as one, so a preview always runs over something saved, named, and in the
  branch's history rather than over whatever was last typed into a box.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY } from './Button.svelte';
  import { reported } from './errors.svelte';
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
  import { dateTime } from './time';
  import { editable } from './access';
  import {
    listSamples,
    deleteSample,
    listTests,
    capturedAt,
    type SampleSummary,
    type TestSummary,
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

  /** The tests that run over each sample, so an edit to the sample can name the ones it changes the input of. */
  let tests: TestSummary[] = $state([]);

  const sampleSort = new TableSort<'name' | 'source' | 'records' | 'captured'>();
  let sortedSamples = $derived(
    sampleSort.apply(samples, {
      name: (s) => s.name,
      source: (s) => s.source ?? 'pasted',
      records: (s) => s.records,
      captured: (s) => capturedAt(s) ?? 0,
    }),
  );

  async function loadTests() {
    try {
      tests = (await listTests()).tests;
    } catch (e) {
      reported(e);
    }
  }
  loadTests();

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
            <th></th>
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
              <td class="row-actions">
                <a class="btn" href={`#/build/pipelines/for/${encodeURIComponent(s.id)}`}
                  >Run in a pipeline</a
                >
                {#if !readOnly}
                  <Button
                    onclick={() => (editing = editing === s.id ? null : s.id)}
                    disabled={saving.on ? BUSY : ''}>Edit</Button
                  >
                  <Button onclick={() => remove(s)} disabled={saving.on ? BUSY : ''}>Delete</Button>
                {/if}
              </td>
            </tr>
            {#if editing === s.id}
              <tr class="open">
                <td colspan="5">
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
  .table-wrap {
    overflow-x: auto;
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
  /* The sample open for editing, folded open under its row (STYLE_GUIDE.md, "Lists"). */
  tr.open {
    background: var(--panel);
    box-shadow: inset 3px 0 0 var(--accent);
  }
</style>
