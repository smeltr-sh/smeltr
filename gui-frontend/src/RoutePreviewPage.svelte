<!--
  Route preview: "where it goes". A screen of its own, not a panel inside the
  routes editor -- it runs the routes as saved on this branch, and draws
  one sample's trip through them: the sample, each matched pipeline, and
  each destination, as nodes joined by edges sized by how many records
  traveled them.

  The counts, the documents and the per-record table are `RoutePreview`'s,
  handed this page's own run: one picker over one run, whatever is drawn
  from it. What this page adds is the diagram.
-->
<script lang="ts">
  import { reported, unreported } from './errors.svelte';
  import InlineError from './InlineError.svelte';
  import RoutePreview from './RoutePreview.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import {
    getConfig,
    currentBranch,
    listSamples,
    routePreview,
    errorText,
    type FileConfigDto,
    type RoutePreviewResult,
    type SampleSummary,
  } from './api';
  import { count } from './time';
  import { editable } from './access';

  let config = $state<FileConfigDto | null>(null);
  let loading = $state(true);
  let loadFailed = $state(false);

  let samples = $state<SampleSummary[]>([]);
  let selected = $state('');
  let result = $state<RoutePreviewResult | null>(null);
  let running = $state(false);
  let runError = $state('');

  async function load() {
    loading = true;
    loadFailed = false;
    try {
      const [cfg, listed] = await Promise.all([getConfig(), listSamples()]);
      if (cfg.config) config = cfg.config;
      else loadFailed = true;
      // Only a sample that names a source can be routed: routing selects on
      // one, so a pasted sample with none matches nothing.
      samples = listed.samples.filter((s) => s.source);
      if (!selected && samples.length > 0) selected = samples[0].id;
    } catch (e) {
      loadFailed = true;
      reported(e);
    } finally {
      loading = false;
    }
    // Arriving here is the ask. Nothing is delivered and no destination is
    // opened, so there is nothing to hold back for a second click.
    if (config && selected) await run();
  }
  load();

  async function run() {
    if (!config || !selected) return;
    running = true;
    runError = '';
    try {
      const res = await routePreview(JSON.stringify(config), selected);
      if (!res.ok) {
        runError = errorText(res.errors);
        result = null;
      } else {
        result = res;
      }
    } catch (e) {
      runError = unreported(e);
      result = null;
    } finally {
      running = false;
    }
  }

  let sampleName = $derived(samples.find((s) => s.id === selected)?.name ?? selected);

  // Locking a test needs a run that actually produced this output: the
  // routes as saved on this branch, over a named sample.
  let canLockTest = $derived(result !== null && result.ok && selected !== '');

  function goLockTest() {
    location.hash = `#/build/tests/lock-e2e/${encodeURIComponent(selected)}`;
  }

  // -- diagram geometry -------------------------------------------------
  //
  // Three fixed columns -- sample, matched pipelines, destinations -- each
  // node 176px wide with a 104px gap, the storyboard's own numbers. Only
  // the height grows with the data; a route that dropped rows and a record
  // no route matched each get one shared sink node rather than a node per
  // pipeline, so the middle and right columns stay the routes and the
  // destinations.
  const NODE_W = 176;
  const NODE_H = 64;
  const GAP = 104;
  const ROW_H = 92;
  const PAD = 20;
  const COL1_X = PAD;
  const COL2_X = COL1_X + NODE_W + GAP;
  const COL3_X = COL2_X + NODE_W + GAP;

  type DiagramNode = {
    key: string;
    x: number;
    y: number;
    label: string;
    sub: string;
    kind: string;
    warn?: boolean;
  };

  /** Records that matched route `i`, and how many of those the pipeline went
   * on to drop (matched but produced nothing). */
  let routeStats = $derived.by(() => {
    const by = new Map<number, { matched: number; dropped: number }>();
    if (!result) return by;
    for (const row of result.rows) {
      for (const i of row.matched) {
        const s = by.get(i) ?? { matched: 0, dropped: 0 };
        s.matched++;
        if (!row.through.some((t) => t.route === i && t.documents.length > 0)) s.dropped++;
        by.set(i, s);
      }
    }
    return by;
  });

  /** Documents route `i` sent to destination `d`, keyed `"i:d"`. */
  let routeDestCounts = $derived.by(() => {
    const by = new Map<string, number>();
    if (!result) return by;
    for (const row of result.rows) {
      for (const t of row.through) {
        for (const doc of t.documents) {
          for (const d of doc.destinations) {
            const key = `${t.route}:${d}`;
            by.set(key, (by.get(key) ?? 0) + 1);
          }
        }
      }
    }
    return by;
  });

  let unroutedCount = $derived(
    result ? result.rows.filter((r) => r.matched.length === 0).length : 0,
  );

  let pipelineNodes = $derived(
    [...routeStats.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([route, s], i) => ({
        key: `route-${route}`,
        route,
        x: COL2_X,
        y: PAD + i * ROW_H,
        label: result?.route_pipelines[route] ?? `route ${route}`,
        sub: `route ${route + 1} matched ${count(s.matched, 'record')}`,
        kind: 'pipeline',
      })),
  );

  let unroutedNode = $derived<(DiagramNode & { route?: number }) | null>(
    unroutedCount > 0
      ? {
          key: 'unrouted',
          x: COL2_X,
          y: PAD + pipelineNodes.length * ROW_H,
          label: 'unrouted',
          sub: count(unroutedCount, 'record'),
          kind: 'no route matched',
          warn: true,
        }
      : null,
  );

  let destTotals = $derived.by(() => {
    const by = new Map<number, number>();
    if (!result) return by;
    for (const row of result.rows) {
      for (const t of row.through) {
        for (const doc of t.documents) {
          for (const d of doc.destinations) by.set(d, (by.get(d) ?? 0) + 1);
        }
      }
    }
    return by;
  });

  let destNodes = $derived(
    [...destTotals.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([dest, n], i) => ({
        key: `dest-${dest}`,
        dest,
        x: COL3_X,
        y: PAD + i * ROW_H,
        label: result?.destination_names[dest] ?? `destination ${dest}`,
        sub: `${n} would be sent`,
        kind: 'destination',
      })),
  );

  let droppedTotal = $derived([...routeStats.values()].reduce((sum, s) => sum + s.dropped, 0));

  let droppedNode = $derived<DiagramNode | null>(
    droppedTotal > 0
      ? {
          key: 'dropped',
          x: COL3_X,
          y: PAD + destNodes.length * ROW_H,
          label: 'dropped by the pipeline',
          sub: count(droppedTotal, 'record'),
          kind: '',
          warn: true,
        }
      : null,
  );

  let col2Rows = $derived(pipelineNodes.length + (unroutedNode ? 1 : 0));
  let col3Rows = $derived(destNodes.length + (droppedNode ? 1 : 0));
  let diagramRows = $derived(Math.max(col2Rows, col3Rows, 1));
  let diagramHeight = $derived(PAD * 2 + (diagramRows - 1) * ROW_H + NODE_H);
  let diagramWidth = $derived(COL3_X + NODE_W + PAD);

  let sampleNode = $derived<DiagramNode | null>(
    result
      ? {
          key: 'sample',
          x: COL1_X,
          y: (diagramHeight - NODE_H) / 2,
          label: sampleName,
          sub: count(result.rows.length, 'record'),
          kind: 'sample',
        }
      : null,
  );

  let allNodes = $derived<DiagramNode[]>([
    ...(sampleNode ? [sampleNode] : []),
    ...pipelineNodes,
    ...(unroutedNode ? [unroutedNode] : []),
    ...destNodes,
    ...(droppedNode ? [droppedNode] : []),
  ]);

  /** log-scaled and clamped, so one record and ten thousand both read as a line. */
  function widthFor(n: number): number {
    return n <= 0 ? 0 : Math.max(1, Math.min(8, 1 + Math.log10(n + 1) * 3));
  }

  function curve(from: DiagramNode, to: DiagramNode): string {
    const x1 = from.x + NODE_W;
    const y1 = from.y + NODE_H / 2;
    const x2 = to.x;
    const y2 = to.y + NODE_H / 2;
    const mid = (x1 + x2) / 2;
    return `M${x1} ${y1} C${mid} ${y1} ${mid} ${y2} ${x2} ${y2}`;
  }

  type Edge = { key: string; d: string; width: number; error: boolean };

  let edges = $derived.by<Edge[]>(() => {
    if (!sampleNode) return [];
    const es: Edge[] = [];
    for (const p of pipelineNodes) {
      const n = routeStats.get(p.route)?.matched ?? 0;
      if (n > 0)
        es.push({ key: `s-${p.key}`, d: curve(sampleNode, p), width: widthFor(n), error: false });
    }
    if (unroutedNode) {
      es.push({
        key: 's-unrouted',
        d: curve(sampleNode, unroutedNode),
        width: widthFor(unroutedCount),
        error: true,
      });
    }
    for (const p of pipelineNodes) {
      for (const d of destNodes) {
        const n = routeDestCounts.get(`${p.route}:${d.dest}`) ?? 0;
        if (n > 0)
          es.push({ key: `${p.key}-${d.key}`, d: curve(p, d), width: widthFor(n), error: false });
      }
      if (droppedNode) {
        const n = routeStats.get(p.route)?.dropped ?? 0;
        if (n > 0)
          es.push({
            key: `${p.key}-dropped`,
            d: curve(p, droppedNode),
            width: widthFor(n),
            error: true,
          });
      }
    }
    return es;
  });
</script>

<SectionShell>
  <PageHeader title="Where these records would go">
    {#snippet help()}
      Runs a sample through the routes saved on this branch and stops before delivery: nothing is
      sent and no destination is opened.
    {/snippet}
  </PageHeader>

  {#if loading}
    <p class="dim">Loading&hellip;</p>
  {:else if loadFailed}
    <!-- The error bar says why. -->
  {:else}
    <div class="controls">
      <span class="saved-context">Saved on <code>{currentBranch()}</code></span>
      <label for="rp-sample"><span class="field-name">Sample</span></label>
      <select id="rp-sample" bind:value={selected}>
        {#each samples as s (s.id)}
          <option value={s.id}>{s.name} ({count(s.records, 'record')})</option>
        {/each}
      </select>
      <button class="primary" onclick={run} disabled={running || !selected || !config}>
        {running ? 'Running…' : 'Run'}
      </button>
    </div>

    {#if samples.length === 0}
      <p>
        No sample names a source. Routing selects on a source, so a sample has to say which one its
        records stand for &mdash; set that on the <a class="link" href="#/build">Samples</a> page.
      </p>
    {/if}
    <InlineError message={runError} />

    <div class="output-actions">
      <span class="grow"></span>
      {#if editable()}
        <button type="button" onclick={goLockTest} disabled={!canLockTest}>
          Lock this as a test
        </button>
      {/if}
    </div>

    <div class="diagram-scroll">
      {#if !result}
        {#if !runError}
          <p class="dim empty">
            Choose a sample and run the preview to see where its records would go.
          </p>
        {/if}
      {:else if result.rows.length === 0}
        <p class="dim empty">This sample has no records.</p>
      {:else}
        <div class="diagram" style="width: {diagramWidth}px; height: {diagramHeight}px">
          <svg class="edges" width={diagramWidth} height={diagramHeight}>
            {#each edges as e (e.key)}
              <path
                d={e.d}
                fill="none"
                stroke={e.error ? 'var(--error)' : 'var(--accent)'}
                stroke-opacity={e.error ? 0.85 : 0.5}
                stroke-width={e.width}
                stroke-linecap="round"
              />
            {/each}
          </svg>
          {#each allNodes as n (n.key)}
            <div class="node" class:warn={n.warn} style="left: {n.x}px; top: {n.y}px">
              <div class="node-label">{n.label}</div>
              <div class="node-sub mono">{n.sub}</div>
              {#if n.kind}<div class="node-kind">{n.kind}</div>{/if}
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/if}

  <RoutePreview {config} {result} />
</SectionShell>

<style>
  .grow {
    flex-grow: 1;
  }

  .dim {
    color: var(--dim);
  }

  .mono {
    font-family: var(--mono);
  }

  .saved-context {
    display: inline-flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.15rem 0.45rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--dim);
    font-size: var(--text-note);
  }

  .saved-context code {
    font-family: var(--mono);
  }

  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }

  .output-actions {
    display: flex;
    align-items: center;
    margin-bottom: 0.5rem;
  }

  .controls label {
    font-size: var(--text-label);
    color: var(--dim);
  }

  /* The label register: the field's name. */
  .field-name {
    display: flex;
    align-items: center;
    font-weight: 600;
  }

  select {
    flex-grow: 1;
    max-width: 24rem;
  }

  .diagram-scroll {
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    margin-bottom: 0.5rem;
  }

  .diagram {
    position: relative;
  }

  .empty {
    padding: 1.5rem;
    margin: 0;
  }

  .edges {
    position: absolute;
    inset: 0;
    pointer-events: none;
  }

  .node {
    position: absolute;
    width: 176px;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.45rem 0.6rem;
  }

  .node.warn {
    border-color: var(--error);
  }

  .node-label {
    font-weight: 600;
    font-size: var(--text-label);
  }

  .node.warn .node-label {
    color: var(--error);
  }

  .node-sub {
    color: var(--dim);
    font-size: var(--text-micro);
  }

  .node-kind {
    color: var(--dim);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-top: 0.15rem;
  }
</style>
