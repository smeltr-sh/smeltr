<!--
  Build / <pipeline> / lock a test, or Build / routes / route a sample / lock
  a test: run once -- the pipeline preview, or (when `pipeline` is absent)
  the route preview -- over a saved sample and fix what it made of the
  fields and rows that were on screen (`lockChoice`) as the answer. The
  pipeline (or nothing) and the sample are given, not chosen here
  (`gui-design.md`).

  Locking again is given a failing test instead, and reads all of that from
  it, its fields and rows too. `now()` stays pinned to the instant the test
  was first locked, so the review diff holds only what really changed.
-->
<script lang="ts">
  import { Busy } from './busy.svelte';
  import Button, { BUSY, INCOMPLETE } from './Button.svelte';
  import ShowAll from './ShowAll.svelte';
  import { bareError, errors, reported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import SectionShell from './SectionShell.svelte';
  import Field from './Field.svelte';
  import PageHeader from './PageHeader.svelte';
  import SaveConflictPanel from './SaveConflict.svelte';
  import {
    getConfig,
    currentBranch,
    currentScope,
    getSample,
    getTest,
    previewPipeline,
    routePreview,
    cutDown,
    lockChoice,
    putTest,
    errorText,
    recordLine,
    type ApiError,
    type LockChoice,
    type Outcome,
    type RoutePreviewResult,
    type Sent,
    type SaveConflict,
    type TestDto,
  } from './api';
  import { count } from './time';
  import { formatDiffValue } from './diff';
  import { leafMap } from './recordDiff';
  import { announcePipelineNames } from './nav';
  import { editable } from './access';
  import { nameOf, pipelineNamesAt } from './pipelineNames';

  // `pipeline` absent is an end-to-end lock: the sample routed by its
  // source (the route preview) rather than run through one pipeline.
  // `relock` names a test to lock again; its own pipeline and sample stand
  // in for the other two.
  let props: { pipeline?: string; sample?: string; relock?: string } = $props();
  // svelte-ignore state_referenced_locally
  let pipeline = $state(props.pipeline);
  // svelte-ignore state_referenced_locally
  let sample = $state(props.sample ?? '');
  // svelte-ignore state_referenced_locally
  const relock = props.relock;

  let loading = $state(true);
  let loadFailed = $state(false);
  /** Why it failed, where the error bar does not say: a test or a sample that is not there. */
  let loadError = $state('');
  let loadWarning = $state('');

  let sampleName = $state('');
  let pipelineName = $state('');
  let sampleSource = $state('');
  let sampleRecords = $state<string[]>([]);

  // Captured once, at the moment this screen ran its one preview: what
  // `now()` read and what it produced. Locking replays exactly this,
  // rather than running the preview a second time.
  let atMicros = $state(0);
  /** Every document the run made, rows it made none on included, as the grid numbers its rows. */
  let made = $state<Made[]>([]);
  /** The fields and rows to lock: the locked test's own when locking again, else what was on screen. */
  let choice = $state<LockChoice | null>(relock ? null : lockChoice());
  let previewOk = $state(true);
  let previewErrors = $state<ApiError[]>([]);

  let name = $state('');
  const saving = new Busy();
  let conflict = $state<SaveConflict | null>(null);

  const SHOWN = 5;
  /** Whoever asked to see every sample record, or every checked row, not the first few. */
  let allRecords = $state(false);
  let allChecked = $state(false);

  /** One document a run made, whole: what `cutDown` cuts to a test's fields. */
  type Made = { row: number; document: unknown | null; sent?: Sent };

  /** A route preview as what an end-to-end test checks: per record, per route that carried it, per document,
   * with where it went, and a record that made none as one entry making none -- the leader's own
   * `run_route_actual`, so comparing the two is comparing like with like. */
  function routedMade(result: RoutePreviewResult): Made[] {
    const out: Made[] = [];
    for (const row of result.rows) {
      const before = out.length;
      for (const t of row.through) {
        const route = result.route_ids[t.route] ?? '';
        const pipeline = result.route_pipeline_ids[t.route] ?? '';
        for (const doc of t.documents) {
          const destinations = doc.destinations.map(
            (d) => result.destination_names[d] ?? String(d),
          );
          out.push({
            row: row.row,
            document: JSON.parse(doc.document),
            sent: { pipeline, route, destinations },
          });
        }
      }
      if (out.length === before) out.push({ row: row.row, document: null });
    }
    return out;
  }

  /** With nothing chosen -- the page reached by its address alone -- every field the run made, and every row. */
  let fields = $derived(
    choice?.fields ?? [
      ...new Set(made.flatMap((m) => (m.document === null ? [] : [...leafMap(m.document).keys()]))),
    ],
  );
  let rowsChecked = $derived(choice?.rows ?? [...new Set(made.map((m) => m.row))]);
  // A checked row the run never reached made nothing, as a dropped row did, and stays checked as that.
  let outcomes = $derived(
    rowsChecked.flatMap((row) => {
      const here = made.filter((m) => m.row === row);
      return here.length ? here.map((m) => cutDown(m.row, m.document, fields, m.sent)) : [{ row }];
    }),
  );

  async function load() {
    loading = true;
    loadFailed = false;
    loadError = '';
    loadWarning = '';
    try {
      let lockedAt: number | null = null;
      if (relock) {
        const testRes = await getTest(relock);
        if (!testRes.test)
          return failed(
            testRes.errors,
            `there is no test ${JSON.stringify(relock)} on this branch`,
          );
        pipeline = testRes.test.pipeline;
        sample = testRes.test.sample;
        name = testRes.test.name;
        lockedAt = testRes.test.lockedAt;
        choice = {
          fields: testRes.test.fields,
          rows: [...new Set(testRes.test.rows.map((o) => o.row))],
        };
      }
      const [configRes, sampleRes] = await Promise.all([getConfig(), getSample(sample)]);
      if (!configRes.config)
        return failed(configRes.errors, 'the config on this branch could not be read');
      pipelineName = pipeline
        ? nameOf(await pipelineNamesAt(currentBranch(), currentScope()), pipeline)
        : '';
      if (pipeline) announcePipelineNames({ [pipeline]: pipelineName });
      if (!configRes.ok) loadWarning = errorText(configRes.errors);
      if (!sampleRes.sample)
        return failed(
          sampleRes.errors,
          `there is no sample ${JSON.stringify(sample)} on this branch`,
        );
      sampleName = sampleRes.sample.name;
      sampleSource = sampleRes.sample.source ?? '';
      sampleRecords = sampleRes.sample.records.map((r) => recordLine(r));

      atMicros = lockedAt ?? Date.now() * 1000;
      // Without its schemas: the leader holds the rows to the branch's, and a standard's runs to megabytes.
      const config = JSON.stringify({ ...configRes.config, schemas: [] });
      if (pipeline) {
        const preview = await previewPipeline({ config, pipeline, sample, atMicros });
        made = preview.rows.map((r) => ({ row: r.row, document: r.json }));
        previewOk = preview.ok;
        previewErrors = preview.errors;
      } else {
        const preview = await routePreview(config, sample, { atMicros });
        // A row that asked for more memory than a preview may hold fails a
        // run the same way (`test_api::run_route_actual`); refused here
        // rather than locked and failing every time it runs.
        const refused = preview.rows.find((r) => r.refused);
        previewOk = preview.ok && !refused;
        previewErrors = preview.ok
          ? refused
            ? [bareError(`record ${refused.row}: ${refused.refused}`)]
            : []
          : preview.errors;
        made = previewOk ? routedMade(preview) : [];
      }
    } catch (e) {
      // The previews are quiet calls, which this puts in the bar too: this page has nowhere else to say it.
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
  }
  load();

  /** A read answered without what it was for. Its errors are in the bar; with none, `otherwise` says why here. */
  function failed(said: ApiError[], otherwise: string) {
    loadFailed = true;
    if (said.length === 0) loadError = otherwise;
  }

  let canLock = $derived(!loading && previewOk && name.trim() !== '' && !saving.on);

  function buildTest(): TestDto {
    const base = {
      id: relock ?? name.trim(),
      name: name.trim(),
      sample,
      lockedAt: atMicros,
      rowsIn: sampleRecords.length,
      fields,
      rows: outcomes,
    };
    return pipeline ? { ...base, pipeline } : base;
  }

  async function lockTest() {
    if (!canLock) return;
    conflict = null;
    await saving.run(async () => {
      const res = await putTest(buildTest());
      if (res.ok) {
        if (relock) history.back();
        else location.hash = '#/build';
      } else if (res.conflict) {
        conflict = res.conflict;
        errors.shown(res.errors);
      }
    });
  }

  /** Retried onto whatever branch the conflict panel just switched to. */
  async function retry(): Promise<boolean> {
    return (await putTest(buildTest())).ok;
  }

  function cancel() {
    if (relock) {
      history.back();
      return;
    }
    location.hash = pipeline
      ? `#/build/pipelines/${encodeURIComponent(pipeline)}`
      : '#/build/routes/sample';
  }

  function short(text: string, limit = 100): string {
    return text.length > limit ? `${text.slice(0, limit)}…` : text;
  }

  /** What a checked row shows for `field`: its value, or that the document lacks it. */
  function cell(o: Outcome, field: string): string {
    return o.values && field in o.values ? formatDiffValue(o.values[field]) : 'absent';
  }
</script>

<SectionShell>
  <PageHeader title={relock ? 'Lock this test again' : 'Lock this as a test'}>
    {#snippet help()}
      Whatever the expected output shows now, right or wrong, becomes the answer.
    {/snippet}
  </PageHeader>

  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if loadFailed}
    <InlineError message={loadError} />
    <!-- A re-lock has no trail back, so its way out cannot hang on a read that failed. -->
    {#if relock}<button onclick={cancel}>Cancel</button>{/if}
  {:else}
    {#if loadWarning}<p class="warn">{loadWarning}</p>{/if}

    <div class="card fields">
      <div class="grow">
        <Field required label="Name">
          <input
            bind:value={name}
            placeholder="what this test checks"
            data-guide="lock-name"
            disabled={saving.on}
          />
        </Field>
      </div>
      {#if pipeline}
        <Field label="Pipeline" group>
          <div class="readonly">{pipelineName}</div>
        </Field>
      {:else}
        <Field label="Routed by source" group>
          <div class="readonly mono">{sampleSource}</div>
        </Field>
      {/if}
      <Field label="Sample" group>
        <div class="readonly mono">{sampleName}</div>
      </Field>
    </div>

    {#if !previewOk}
      <InlineError>
        {errorText(previewErrors) ||
          (pipeline
            ? 'this pipeline did not run over this sample'
            : 'this sample did not route cleanly')}
      </InlineError>
    {/if}

    <div class="grid2">
      <div class="card">
        <div class="row">
          <h2 class="uppercase-label">Input, locked</h2>
          <span class="mono dim">{count(sampleRecords.length, 'row')}</span>
        </div>
        <div class="rows">
          {#each allRecords ? sampleRecords : sampleRecords.slice(0, SHOWN) as line}
            <div class="mono line ellipsis">{short(line)}</div>
          {/each}
          {#if !allRecords && sampleRecords.length > SHOWN}
            <div class="dim line ellipsis">
              <ShowAll
                more={sampleRecords.length - SHOWN}
                what="row"
                onshowall={() => (allRecords = true)}
              />
            </div>
          {/if}
        </div>
      </div>

      <div class="card accent">
        <div class="row">
          <h2 class="uppercase-label">Checks, locked</h2>
          <span class="mono dim">
            {count(fields.length, 'field')} on {count(rowsChecked.length, 'row')}
          </span>
        </div>
        <div class="rows checks">
          <table>
            <thead>
              <tr>
                <th class="dim">row</th>
                {#each fields as f (f)}<th class="mono">{f}</th>{/each}
              </tr>
            </thead>
            <tbody>
              {#each allChecked ? outcomes : outcomes.slice(0, SHOWN) as o}
                <tr>
                  <td class="dim mono">{o.row}</td>
                  {#if !o.values}
                    <td class="dim" colspan={Math.max(fields.length, 1)}>dropped</td>
                  {:else}
                    {#each fields as f (f)}
                      <td class="mono" class:dim={!(f in o.values)}>{short(cell(o, f), 40)}</td>
                    {/each}
                  {/if}
                </tr>
                {#if o.sent}
                  <tr class="sent">
                    <td></td>
                    <td class="mono dim" colspan={Math.max(fields.length, 1)}>
                      via {o.sent.route} &rarr; {o.sent.destinations.join(', ')}
                    </td>
                  </tr>
                {/if}
              {/each}
            </tbody>
          </table>
          {#if !allChecked && outcomes.length > SHOWN}
            <div class="dim line ellipsis">
              <ShowAll
                more={outcomes.length - SHOWN}
                what="row"
                onshowall={() => (allChecked = true)}
              />
            </div>
          {/if}
        </div>
      </div>
    </div>

    {#if conflict}
      <SaveConflictPanel {conflict} onDiscard={() => location.reload()} onRetry={retry} />
    {/if}

    <div class="card">
      <div class="row explain-row">
        <Button onclick={cancel} disabled={saving.on ? BUSY : ''}>Cancel</Button>
        {#if editable()}
          <Button
            class="primary"
            data-guide="lock"
            onclick={lockTest}
            disabled={saving.on ? BUSY : !canLock ? INCOMPLETE : ''}
          >
            {saving.on ? 'Locking…' : 'Lock the test'}
          </Button>
        {/if}
      </div>
    </div>
  {/if}
</SectionShell>

<style>
  h2 {
    margin: 0;
  }
  .warn {
    color: var(--warn);
  }

  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-bottom: 1rem;
  }
  .card.accent {
    border-color: var(--accent);
  }
  .row {
    display: flex;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.5rem 0.75rem;
    background: var(--bg);
    border-radius: var(--radius) var(--radius) 0 0;
    border-bottom: 1px solid var(--border);
  }
  .row h2 {
    flex-grow: 1;
  }

  .fields {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem 1.5rem;
    align-items: flex-end;
    padding: 0.75rem;
  }
  .readonly {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.35rem 0.5rem;
    background: var(--field);
    color: var(--ink);
    font: inherit;
    font-size: var(--text-body);
  }
  input {
    min-width: min(20rem, 100%);
  }

  .grid2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }
  /* A grid item's default minimum width is its content's, so a long
     unbroken line (a JSON row, a document id) would widen the column
     past its share instead of the `.line` ellipsis below ever triggering. */
  .grid2 > .card {
    min-width: 0;
  }
  .rows {
    padding: 0.5rem 0.75rem;
  }
  .line {
    font-size: var(--text-note);
    padding: 0.15rem 0;
  }

  .checks {
    overflow-x: auto;
  }
  .checks table {
    border-collapse: collapse;
    font-size: var(--text-note);
  }
  .checks th,
  .checks td {
    text-align: left;
    padding: 0.2rem 1.2rem 0.2rem 0;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .checks tr:has(+ .sent) td {
    border-bottom: none;
  }

  .explain-row {
    align-items: center;
    justify-content: flex-end;
  }

  @media (max-width: 800px) {
    .grid2 {
      grid-template-columns: 1fr;
    }
  }
</style>
