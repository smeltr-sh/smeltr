<!--
  The tests of every pipeline and route a change reaches, run on its branch,
  grouped by what they cover, each group saying why the change reaches it.
-->
<script lang="ts">
  import InlineError from './InlineError.svelte';
  import {
    currentBranch,
    setScope,
    scopedKey,
    type Reason,
    type ScopedImpact,
    type TestResult,
    type TestSummary,
    type Verdict,
  } from './api';
  import { diffObjects, formatDiffValue, type DiffEntry } from './diff';
  import { count } from './time';
  import { testsCovering } from './objectChanges';
  import Help from './Help.svelte';

  let {
    branch,
    touchedByScope,
    testResults,
    testSummaryByKey,
    testsLoading,
    testsError,
    pipelineName,
  }: {
    /** The branch the change is on. A re-lock writes to the branch in hand, so it is offered only there. */
    branch: string;
    /** What the change reaches, one entry per scope it reaches anything in. Kept apart from
     * `testResults` because something here with no result covering it is reached and untested. */
    touchedByScope: ScopedImpact[];
    testResults: TestResult[];
    /** Every test's summary on the branch, by `scopedKey(scope, id)`: which pipelines and routes a
     * test covers is read from here, since a result's own `pipeline` is absent for an end-to-end
     * test. */
    testSummaryByKey: Map<string, TestSummary>;
    testsLoading: boolean;
    testsError: string;
    /** What a reader calls pipeline `id` of `scope`. */
    pipelineName: (scope: string, id: string) => string;
  } = $props();

  /** Every route `t` covers, per its summary -- empty for a pipeline test. */
  function routesOf(t: TestResult): string[] {
    return testSummaryByKey.get(scopedKey(t.scope, t.id))?.routes ?? [];
  }

  /** Every test `route` covers within `scope`, by `routesOf` -- the route counterpart of `testsCovering`. */
  function resultsForRoute(scope: string, route: string): TestResult[] {
    return testResults.filter((t) => t.scope === scope && routesOf(t).includes(route));
  }

  /** A short, actionable phrase naming the object the reason points at -- the table, the source, the
   * destination, or the pipeline called. Three of these name an object that never appears in the diff on
   * this same screen, so the phrase is the only thing connecting the two. A switch with no default: an
   * unhandled `Reason` variant is a compile error here, never a blank line on screen. */
  function reasonPhrase(reason: Reason, scope: string): string {
    switch (reason.type) {
      case 'edited':
        return 'Edited directly.';
      case 'readsTable':
        return `Reads lookup table ${reason.table}, which changed.`;
      case 'namesGrokPattern':
        return `Uses grok pattern ${reason.pattern}, which changed.`;
      case 'calls':
        return `Calls pipeline ${pipelineName(scope, reason.pipeline)}, which is reached.`;
      case 'sourceChanged':
        return `Reads from source ${reason.source}, which changed.`;
      case 'destinationChanged':
        return `Delivers to destination ${reason.destination}, which changed.`;
      default:
        return reason satisfies never;
    }
  }

  type ReachedGroup = {
    kind: 'pipeline' | 'route' | 'changed';
    scope: string;
    id: string;
    reasons: Reason[];
    tests: TestResult[];
  };

  /** Pipelines then routes, each with the tests that cover it, across every scope the change reaches --
   * one list, so a reached route renders alongside a reached pipeline, tagged by `kind` so it is never
   * mistaken for one, and by `scope` so two groups' same-named pipeline or route are never mistaken for
   * each other either. */
  let reachedGroups = $derived.by((): ReachedGroup[] => {
    const reached = touchedByScope.flatMap((si) => [
      ...si.pipelines.map((r) => ({
        kind: 'pipeline' as const,
        scope: si.scope,
        id: r.id,
        reasons: r.reasons,
        tests: testsCovering(testResults, testSummaryByKey, si.scope, r.id),
      })),
      ...si.routes.map((r) => ({
        kind: 'route' as const,
        scope: si.scope,
        id: r.id,
        reasons: r.reasons,
        tests: resultsForRoute(si.scope, r.id),
      })),
    ]);
    // A test whose sample or lock changed runs though the change reaches nothing it covers.
    const covered = new Set(reached.flatMap((g) => g.tests));
    const changed = [...new Set(testResults.map((t) => t.scope))].map((scope) => ({
      kind: 'changed' as const,
      scope,
      id: '',
      reasons: [],
      tests: testResults.filter((t) => t.scope === scope && !covered.has(t)),
    }));
    return [...reached, ...changed.filter((g) => g.tests.length > 0)];
  });

  /** Every pipeline, and every route, this change reaches, summed across every scope -- what the Tests
   * card header counts. */
  let touchedPipelineCount = $derived(touchedByScope.reduce((n, si) => n + si.pipelines.length, 0));
  let touchedRouteCount = $derived(touchedByScope.reduce((n, si) => n + si.routes.length, 0));
  /** Only the kinds the change reaches: a change to a sample alone touches none. */
  let touchedText = $derived(
    [
      touchedPipelineCount > 0 ? count(touchedPipelineCount, 'pipeline') : null,
      touchedRouteCount > 0 ? count(touchedRouteCount, 'route') : null,
    ]
      .filter((part) => part !== null)
      .join(', '),
  );

  /** What a failing verdict's first difference is, laid out for the card:
   * a headline sentence, and either the field-level diff (both sides are
   * documents) or a plain note plus whichever side is present -- a drop,
   * or a run that produced more or fewer rows than the test locks. */
  type FailInfo = {
    headline: string;
    note: string | null;
    entries: DiffEntry[];
    expectedLine: string | null;
    actualLine: string | null;
  };

  /** Lock a failing test again, in the scope that holds it. */
  function relock(t: TestResult) {
    setScope(t.scope, `#/build/tests/relock/${encodeURIComponent(t.id)}`);
  }

  /** "held 3 records when locked, now 4": the sample itself changed under the test. */
  function rowsInText(v: Verdict): string | null {
    if (!v.rowsIn) return null;
    return `The sample held ${count(v.rowsIn.locked, 'record')} when locked, now ${v.rowsIn.now}.`;
  }

  function failInfo(v: Verdict): FailInfo | null {
    const d = v.firstDifference;
    if (!d) return null;
    const headline =
      v.expectedRows === v.actualRows
        ? `Row ${d.at + 1} of ${v.expectedRows} differs from what the test locks.`
        : `Row ${d.at + 1} differs from what the test locks — it locked ${count(v.expectedRows, 'row')}, this run produced ${count(v.actualRows, 'row')}.`;

    const bothPresent =
      !d.expectedDropped && !d.actualDropped && d.expected !== undefined && d.actual !== undefined;
    if (bothPresent) {
      return {
        headline,
        note: null,
        entries: diffObjects(d.expected, d.actual).filter((e) => e.kind !== 'unchanged'),
        expectedLine: null,
        actualLine: null,
      };
    }
    const note = d.expectedDropped
      ? 'The test locked this row as dropped; this run kept it.'
      : d.actualDropped
        ? 'The test locked a document here; this run dropped it.'
        : d.expected === undefined
          ? 'The test locks nothing here; this run produced a document.'
          : 'The test locks a document here; this run produced nothing.';
    return {
      headline,
      note,
      entries: [],
      expectedLine: d.expected !== undefined ? formatDiffValue(d.expected) : null,
      actualLine: d.actual !== undefined ? formatDiffValue(d.actual) : null,
    };
  }
</script>

<div class="card">
  <div class="row card-head">
    <h2>
      Tests<Help label="Tests"
        >The tests of every pipeline and route this change touches, run on this branch.</Help
      >
    </h2>
    <span class="dim count">
      {#if testsLoading}
        running&hellip;
      {:else if touchedText}
        {touchedText} touched
      {/if}
    </span>
  </div>
  <div class="card-body">
    {#if testsError}
      <InlineError message={testsError} />
    {:else if testsLoading}
      <p class="dim">Running the branch&rsquo;s tests&hellip;</p>
    {:else if reachedGroups.length === 0}
      <p class="dim">This change touches no pipeline or route.</p>
    {:else}
      {#each reachedGroups as group (`${group.kind}:${group.scope}:${group.id}`)}
        {@const failing = group.tests.filter((t) => t.verdict && !t.verdict.passed)}
        {@const passing = group.tests.filter((t) => t.verdict?.passed)}
        {@const errored = group.tests.filter((t) => t.error)}
        <div class="pipeline-block">
          <div class="pipeline-head">
            <span class="kind-tag">{group.kind === 'changed' ? 'test' : group.kind}</span>
            {#if group.kind === 'pipeline'}
              <span class="pipeline-id">{pipelineName(group.scope, group.id)}</span>
            {:else if group.kind === 'changed'}
              <span class="pipeline-id">Sample or lock changed</span>
            {:else}
              <span class="mono pipeline-id">{group.id}</span>
            {/if}
            <span class="dim mono scope-tag">{group.scope}</span>
            {#if group.tests.length === 0}
              <span class="dim">no tests</span>
            {:else}
              {#if passing.length > 0}
                <span class="added">{count(passing.length, 'test')} passing</span>
              {/if}
              {#if failing.length > 0}
                <span class="removed">{count(failing.length, 'test')} failing</span>
              {/if}
              {#if passing.length === 0 && failing.length === 0}
                <span class="dim">{count(errored.length, 'test')} could not run</span>
              {/if}
            {/if}
          </div>
          {#if group.reasons.length > 0}
            <ul class="reasons">
              {#each group.reasons as reason}
                <li>{reasonPhrase(reason, group.scope)}</li>
              {/each}
            </ul>
          {/if}
          {#each group.tests as t (t.id)}
            {@const routes = routesOf(t)}
            <div class="test-line">
              <span>{t.name}</span>
              {#if routes.length > 0}
                <span class="dim mono route-tag">via {routes.join(', ')}</span>
              {/if}
              {#if t.error}
                <span class="dim could-not-run"
                  >could not run<Help label="could not run">{t.error}</Help></span
                >
              {:else if t.verdict?.passed}
                <span class="added">Passing</span>
              {:else}
                <span class="removed">Failing</span>
                {#if branch === currentBranch()}
                  <button type="button" onclick={() => relock(t)}>Re-lock</button>
                {/if}
              {/if}
            </div>
            {#if t.verdict && !t.verdict.passed}
              {@const info = failInfo(t.verdict)}
              {@const rowsIn = rowsInText(t.verdict)}
              {#if rowsIn || info}
                <div class="fail-card">
                  {#if rowsIn}<p class="fail-headline">{rowsIn}</p>{/if}
                  {#if info}<p class="fail-headline">{info.headline}</p>{/if}
                  {#if info?.note}
                    <p>{info.note}</p>
                    {#if info.expectedLine !== null}
                      <div class="mono diff-line removed">
                        expected&emsp;{info.expectedLine}
                      </div>
                    {/if}
                    {#if info.actualLine !== null}
                      <div class="mono diff-line added">
                        actual&emsp;&emsp;{info.actualLine}
                      </div>
                    {/if}
                  {:else if info}
                    {#each info.entries as entry}
                      <div class="mono diff-line">
                        <span class="diff-key">{entry.key}:</span>
                        {#if entry.kind === 'changed'}
                          <span class="removed">{formatDiffValue(entry.before)}</span>
                          <span class="dim">&rarr;</span>
                          <span class="added">{formatDiffValue(entry.after)}</span>
                        {:else if entry.kind === 'removed'}
                          <span class="removed">{formatDiffValue(entry.before)}</span>
                        {:else}
                          <span class="added">{formatDiffValue(entry.after)}</span>
                        {/if}
                      </div>
                    {/each}
                  {/if}
                </div>
              {/if}
            {/if}
          {/each}
        </div>
      {/each}
    {/if}
  </div>
</div>

<style>
  h2 {
    margin: 0;
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }
  .dim {
    color: var(--dim);
  }
  .mono {
    font-family: var(--mono);
  }
  .card {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-bottom: 1.25rem;
    overflow-x: auto;
  }
  .card-head {
    background: var(--bg);
    border-radius: var(--radius) var(--radius) 0 0;
  }
  .card-body {
    padding: 0.7rem 0.9rem;
  }
  .row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.55rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .row:last-child {
    border-bottom: 0;
  }
  .card-head h2 {
    flex-grow: 1;
  }
  .count {
    font-size: var(--text-micro);
    white-space: nowrap;
  }
  .pipeline-block {
    padding: 0.6rem 0.9rem;
    border-bottom: 1px solid var(--border);
  }
  .pipeline-block:last-child {
    border-bottom: 0;
  }
  /* A narrow card wraps whole phrases onto the next line, never inside one. */
  .pipeline-head {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    font-weight: 600;
    margin-bottom: 0.3rem;
  }
  .pipeline-head > :not(.pipeline-id) {
    white-space: nowrap;
  }
  /* The name stays beside its tag and wraps in its own box; what follows it moves below. */
  .pipeline-head .pipeline-id {
    flex: 1 1 12rem;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .pipeline-head .scope-tag {
    flex-shrink: 0;
    font-size: var(--text-micro);
    font-weight: 400;
  }
  .kind-tag {
    flex-shrink: 0;
    padding: 0.05rem 0.35rem;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    font-size: var(--text-micro);
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--dim);
  }
  .reasons {
    margin: 0 0 0.5rem 0.9rem;
    padding-left: 1.1rem;
    font-size: var(--text-note);
    color: var(--ink);
  }
  .reasons li {
    padding: 0.05rem 0;
  }
  .test-line {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.6rem;
    padding: 0.15rem 0 0.15rem 0.9rem;
    font-size: var(--text-label);
  }
  .test-line span:first-child {
    flex-grow: 1;
  }
  /* The name takes what is left; a verdict and its Re-lock never wrap. */
  .test-line > :not(:first-child) {
    flex: none;
  }
  .route-tag {
    font-size: var(--text-micro);
  }

  .fail-card {
    margin: 0.35rem 0 0.6rem 0.9rem;
    padding: 0.55rem 0.7rem;
    border: 1px solid var(--error);
    border-radius: var(--radius-card);
    background: color-mix(in oklch, var(--error) 6%, transparent);
  }
  .fail-headline {
    margin: 0 0 0.35rem;
    font-size: var(--text-label);
  }
  .diff-line {
    font-size: var(--text-note);
    padding: 0.1rem 0;
    overflow-wrap: anywhere;
  }
  .diff-key {
    color: var(--dim);
    margin-right: 0.4rem;
  }
  .added {
    color: var(--added);
  }
  .removed {
    color: var(--removed);
  }
</style>
