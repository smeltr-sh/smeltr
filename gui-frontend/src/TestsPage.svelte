<!--
  Tests: every test in the branch chip's scope, run on the branch, each saying whether it passes. A failing one says why under
  its row. Open shows a test where it is locked (the pipeline editor or Route a sample), on its sample with the
  fields and rows it checks; new tests are locked there too.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import RowMenu from './RowMenu.svelte';
  import Button, { BUSY } from './Button.svelte';
  import MoveDialog, { type MoveTarget } from './MoveDialog.svelte';
  import type { Area } from './areas';
  import { reported, unreported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import SecondaryTabs from './SecondaryTabs.svelte';
  import SortButton from './SortButton.svelte';
  import TestFailure from './TestFailure.svelte';
  import { ask } from './Confirm.svelte';
  import { TableSort } from './sort.svelte';
  import { RESOURCES_TABS, testHash } from './nav';
  import { checksOf } from './objectChanges';
  import { editable } from './access';
  import { names } from './names.svelte';
  import {
    listSamples,
    listTests,
    runTests,
    deleteTest,
    currentBranch,
    currentScope,
    errorText,
    type SampleSummary,
    type TestSummary,
    type TestResult,
  } from './api';

  const readOnly = !editable();

  let tests: TestSummary[] = $state([]);
  let samples: SampleSummary[] = $state([]);
  let loading = $state(true);
  let runError = $state('');
  /** The result of each test's run, by test id; absent while its run is in flight. */
  let results: Record<string, TestResult> = $state({});
  const deleting = new Busy();
  /** The test being moved into another area (`MoveDialog`). */
  let moving: MoveTarget | null = $state(null);
  let areas: Area[] = $state([]);

  const nameOf = (kind: 'pipelines' | 'routes', id: string) =>
    names.of(currentBranch(), currentScope(), kind, id);

  /** What a test covers: for an end-to-end test the routes its sample takes, else its pipelines. */
  function coversOf(t: TestSummary): string {
    if (t.routes?.length) return `via ${t.routes.map((r) => nameOf('routes', r)).join(', ')}`;
    return t.pipelines.map((p) => nameOf('pipelines', p)).join(', ');
  }

  /** The name a row shows for its sample: the sample's id when the sample is gone. */
  const sampleNameOf = (t: TestSummary) => samples.find((s) => s.id === t.sample)?.name ?? t.sample;

  /** Sort rank: failing, could not run, passing, not run yet; what needs a decision comes first. */
  function standing(t: TestSummary): number {
    const r = results[t.id];
    return !r ? 3 : r.error ? 1 : r.verdict?.passed ? 2 : 0;
  }

  const sort = new TableSort<'result' | 'name' | 'covers' | 'sample' | 'checks'>();
  let sorted = $derived(
    sort.apply(
      [...tests].sort((a, b) => standing(a) - standing(b) || a.name.localeCompare(b.name)),
      {
        result: standing,
        name: (t) => t.name,
        covers: coversOf,
        sample: sampleNameOf,
        checks: checksOf,
      },
    ),
  );
  let failing = $derived(tests.filter((t) => standing(t) === 0).length);
  let passing = $derived(tests.filter((t) => standing(t) === 2).length);

  async function load() {
    loading = true;
    try {
      const [testsRes, samplesRes] = await Promise.all([listTests(), listSamples()]);
      tests = testsRes.tests;
      samples = samplesRes.samples;
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
    if (!readOnly) void run();
  }
  load();

  async function run() {
    runError = '';
    try {
      const res = await runTests({});
      if (!res.ok) runError = errorText(res.errors ?? []);
      results = Object.fromEntries(res.results.map((r) => [r.id, r]));
    } catch (e) {
      runError = unreported(e);
    }
  }

  async function remove(t: TestSummary) {
    const ok = await ask({
      title: 'delete test',
      body: `Delete the test "${t.name}".`,
      action: 'Delete',
      danger: true,
    });
    if (!ok) return;
    await deleting.run(async () => {
      const res = await deleteTest(t.id);
      if (res.ok) await load();
    });
  }
</script>

<SectionShell>
  <PageHeader title="Tests" />
  <SecondaryTabs tabs={RESOURCES_TABS} active="#/build/tests" ariaLabel="Resources" />

  {#if runError}<p class="warn">{runError}</p>{/if}

  {#if loading}
    <p class="dim">Loading…</p>
  {:else if tests.length === 0}
    <p class="dim">
      No tests locked yet. <a href="#/build/pipelines">Run a pipeline</a> against a sample and lock what
      it produces.
    </p>
  {:else}
    <p class="tally">
      {#if failing > 0}<span class="failing">{failing} failing</span>{/if}
      {#if passing > 0}<span class="passing">{passing} passing</span>{/if}
    </p>
    <div class="table-wrap">
      <table class="readings">
        <thead>
          <tr>
            <th aria-sort={sort.aria('result')}
              ><SortButton {sort} key="result">Result</SortButton></th
            >
            <th aria-sort={sort.aria('name')}><SortButton {sort} key="name">Test</SortButton></th>
            <th aria-sort={sort.aria('covers')}
              ><SortButton {sort} key="covers">Covers</SortButton></th
            >
            <th aria-sort={sort.aria('sample')}
              ><SortButton {sort} key="sample">Sample</SortButton></th
            >
            <th aria-sort={sort.aria('checks')}
              ><SortButton {sort} key="checks">Checks</SortButton></th
            >
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each sorted as t (t.id)}
            {@const r = results[t.id]}
            {@const fails = !!r?.verdict && !r.verdict.passed}
            <tr class:open={fails}>
              <td>
                {#if !r}
                  <span class="dim">Running…</span>
                {:else if r.error}
                  <span class="warn" title={r.error}>Could not run</span>
                {:else if r.verdict?.passed}
                  <span class="passing">Passing</span>
                {:else}
                  <span class="failing">Failing</span>
                {/if}
              </td>
              <td><strong>{t.name}</strong></td>
              <td class="mono dim" data-label="Covers">{coversOf(t)}</td>
              <td class="mono dim" data-label="Sample">{sampleNameOf(t)}</td>
              <td class="mono" data-label="Checks">{checksOf(t)}</td>
              <td class="row-actions">
                <a class="btn" href={testHash(t.id, t.sample, t.pipeline)}>Open</a>
                {#if !readOnly}
                  <RowMenu label="More actions for {t.name}">
                    {#if areas.length > 1}
                      <button
                        type="button"
                        class="pick"
                        onclick={() => (moving = { part: 'test', id: t.id, name: t.name })}
                        >Move to&hellip;</button
                      >
                    {/if}
                    <Button
                      class="pick remove"
                      onclick={() => remove(t)}
                      disabled={deleting.on ? BUSY : ''}>Delete</Button
                    >
                  </RowMenu>
                {/if}
              </td>
            </tr>
            {#if fails}
              <tr class="open">
                <td></td>
                <td class="whole" colspan="5">
                  <TestFailure verdict={r!.verdict!} routeName={(id) => nameOf('routes', id)} />
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
  <MoveDialog bind:moving bind:areas dirty={false} onmoved={() => void load()} />
</SectionShell>

<style>
  .warn {
    color: var(--warn);
  }
  .tally {
    display: flex;
    gap: 1rem;
    margin: 1.2rem 0 0.6rem;
  }
  .passing {
    color: var(--added);
    font-weight: 600;
  }
  .failing {
    color: var(--error);
    font-weight: 600;
  }
  tr.open:has(+ tr.open),
  tr.open:has(+ tr.open) td {
    border-bottom: 0;
  }
  /* A failing test, with why under its row (STYLE_GUIDE.md, "Lists"). */
  tr.open {
    background: var(--panel);
    box-shadow: inset 3px 0 0 var(--error);
  }
</style>
