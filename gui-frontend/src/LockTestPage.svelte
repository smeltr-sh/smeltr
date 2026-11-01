<!--
  Build / <pipeline> / lock a test, or Build / routes / route preview / lock
  a test: run once -- the pipeline preview, or (when `pipeline` is absent)
  the route preview -- over a saved sample and fix what it produced as the
  answer. The pipeline (or nothing) and the sample are given, not chosen
  here (`gui-design.md`).

  A re-lock is given a failing test instead, and reads all of that from it.
  `now()` stays pinned to the instant the test was first locked, so the
  review diff holds only what really changed.
-->
<script lang="ts">
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
    endToEndDocument,
    putTest,
    errorText,
    recordLine,
    type ApiError,
    type EndToEndExpected,
    type RoutePreviewResult,
    type SaveConflict,
    type TestDto,
  } from './api';
  import { count } from './time';
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
  let expected = $state<unknown[]>([]);
  let previewOk = $state(true);
  let previewErrors = $state<ApiError[]>([]);

  let name = $state('');
  let saving = $state(false);
  let conflict = $state<SaveConflict | null>(null);

  const SHOWN = 5;
  /** Whoever asked to see every sample record, or every expected row, not the first few. */
  let allRecords = $state(false);
  let allExpected = $state(false);

  /** A route preview flattened into what `expected` holds for an end-to-end test: per record, per matched
   * route, per document, packed with where it went (`endToEndDocument`) -- the same shape locking and a
   * run both produce, so comparing the two is comparing like with like. */
  function flattenRouted(result: RoutePreviewResult): EndToEndExpected[] {
    const out: EndToEndExpected[] = [];
    for (const row of result.rows) {
      for (const t of row.through) {
        const route = result.route_ids[t.route] ?? '';
        const routedPipeline = result.route_pipeline_ids[t.route] ?? '';
        for (const doc of t.documents) {
          const destinations = doc.destinations.map(
            (d) => result.destination_names[d] ?? String(d),
          );
          const document = JSON.parse(doc.document);
          out.push(endToEndDocument(document, routedPipeline, route, destinations));
        }
      }
    }
    return out;
  }

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
      const config = JSON.stringify(configRes.config);
      if (pipeline) {
        const preview = await previewPipeline({ config, pipeline, sample, atMicros });
        expected = preview.rows.map((r) => r.json);
        previewOk = preview.ok;
        previewErrors = preview.errors;
      } else {
        const preview = await routePreview(config, sample, atMicros);
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
        expected = previewOk ? flattenRouted(preview) : [];
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

  let droppedCount = $derived(pipeline ? expected.filter((v) => v === null).length : 0);
  let canLock = $derived(!loading && previewOk && name.trim() !== '' && !saving);

  function buildTest(): TestDto {
    const base = {
      id: relock ?? name.trim(),
      name: name.trim(),
      sample,
      lockedAt: atMicros,
      rowsIn: sampleRecords.length,
      expected,
    };
    return pipeline ? { ...base, pipeline } : base;
  }

  async function lockTest() {
    if (!canLock) return;
    saving = true;
    conflict = null;
    try {
      const res = await putTest(buildTest());
      if (res.ok) {
        if (relock) history.back();
        else location.hash = '#/build';
      } else if (res.conflict) {
        conflict = res.conflict;
        errors.shown(res.errors);
      }
    } catch (e) {
      reported(e);
    } finally {
      saving = false;
    }
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
      : '#/build/routes/preview';
  }

  function short(text: string, limit = 100): string {
    return text.length > limit ? `${text.slice(0, limit)}…` : text;
  }

  /** `expected`'s entries, for an end-to-end test: `flattenRouted`'s own shape, read back for display. */
  function routed(v: unknown): EndToEndExpected {
    return v as EndToEndExpected;
  }
</script>

<SectionShell>
  <PageHeader title="Lock this as a test">
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
            disabled={saving}
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
          <h2>Input, locked</h2>
          <span class="mono dim">{count(sampleRecords.length, 'row')}</span>
        </div>
        <div class="rows">
          {#each allRecords ? sampleRecords : sampleRecords.slice(0, SHOWN) as line}
            <div class="mono line">{short(line)}</div>
          {/each}
          {#if !allRecords && sampleRecords.length > SHOWN}
            <div class="dim line">
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
          <h2>Expected output, locked</h2>
          <span class="mono dim">
            {#if pipeline}
              {count(expected.length, 'row')}, {droppedCount} dropped
            {:else}
              {count(expected.length, 'document')} kept
            {/if}
          </span>
        </div>
        <div class="rows">
          {#if pipeline}
            {#each allExpected ? expected : expected.slice(0, SHOWN) as doc}
              <div class="mono line" class:dim={doc === null}>
                {doc === null ? 'dropped' : short(JSON.stringify(doc))}
              </div>
            {/each}
          {:else}
            {#each allExpected ? expected : expected.slice(0, SHOWN) as entry}
              {@const e = routed(entry)}
              <div class="e2e-entry">
                <div class="mono line">{short(JSON.stringify(e.document))}</div>
                <div class="mono dim e2e-meta">
                  via {e.route} &rarr; {e.destinations.join(', ')}
                </div>
              </div>
            {/each}
          {/if}
          {#if !allExpected && expected.length > SHOWN}
            <div class="dim line">
              <ShowAll
                more={expected.length - SHOWN}
                what={pipeline ? 'row' : 'document'}
                onshowall={() => (allExpected = true)}
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
        <button onclick={cancel} disabled={saving}>Cancel</button>
        {#if editable()}
          <button class="primary" data-guide="lock" onclick={lockTest} disabled={!canLock}>
            {saving ? 'Locking…' : 'Lock the test'}
          </button>
        {/if}
      </div>
    </div>
  {/if}
</SectionShell>

<style>
  h2 {
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }
  .warn {
    color: var(--warn);
  }
  .mono {
    font-family: var(--mono);
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
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    padding: 0.15rem 0;
  }

  .e2e-entry {
    padding: 0.2rem 0;
  }
  .e2e-entry .line {
    padding: 0;
  }
  .e2e-meta {
    font-size: var(--text-micro);
    padding: 0.1rem 0 0.2rem;
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
