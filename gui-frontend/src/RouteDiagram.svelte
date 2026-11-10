<!--
  One sample's trip through the routes: the sample, each matched pipeline, and each destination, as nodes joined by
  edges sized by how many records traveled them.
-->
<script lang="ts">
  import { count } from './time';
  import type { RoutePreviewResult } from './api';
  import type { RouteCounts } from './routeCounts';

  let {
    result,
    counts,
    sampleName,
  }: {
    /** A run with at least one record. */
    result: RoutePreviewResult;
    /** `result`'s counts, as the tiles beside it show them. */
    counts: RouteCounts;
    sampleName: string;
  } = $props();

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

  /** Records that matched route `i`, and how many of those were sent nowhere. */
  let routeStats = $derived.by(() => {
    const nowhere = new Set(counts.dropped);
    const by = new Map<number, { matched: number; dropped: number }>();
    for (const row of result.rows) {
      for (const i of row.matched) {
        const s = by.get(i) ?? { matched: 0, dropped: 0 };
        s.matched++;
        if (nowhere.has(row)) s.dropped++;
        by.set(i, s);
      }
    }
    return by;
  });

  /** Documents route `i` sent to destination `d`, keyed `"i:d"`. */
  let routeDestCounts = $derived.by(() => {
    const by = new Map<string, number>();
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

  let pipelineNodes = $derived(
    [...routeStats.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([route, s], i) => ({
        key: `route-${route}`,
        route,
        x: COL2_X,
        y: PAD + i * ROW_H,
        label: result.route_names[route] ?? '',
        sub: `matched ${count(s.matched, 'record')}`,
        kind: 'pipeline',
      })),
  );

  let unroutedNode = $derived<(DiagramNode & { route?: number }) | null>(
    counts.unrouted > 0
      ? {
          key: 'unrouted',
          x: COL2_X,
          y: PAD + pipelineNodes.length * ROW_H,
          label: 'unrouted',
          sub: count(counts.unrouted, 'record'),
          kind: 'no route matched',
          warn: true,
        }
      : null,
  );

  let destNodes = $derived(
    counts.reached.map((d, i) => ({
      key: `dest-${d.id}`,
      dest: d.id,
      x: COL3_X,
      y: PAD + i * ROW_H,
      label: d.name,
      sub: `${d.documents} would be sent`,
      kind: 'destination',
    })),
  );

  let droppedNode = $derived<DiagramNode | null>(
    counts.dropped.length > 0
      ? {
          key: 'dropped',
          x: COL3_X,
          y: PAD + destNodes.length * ROW_H,
          label: 'dropped by the pipeline',
          sub: count(counts.dropped.length, 'record'),
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

  let sampleNode = $derived<DiagramNode>({
    key: 'sample',
    x: COL1_X,
    y: (diagramHeight - NODE_H) / 2,
    label: sampleName,
    sub: count(result.rows.length, 'record'),
    kind: 'sample',
  });

  let allNodes = $derived<DiagramNode[]>([
    sampleNode,
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
        width: widthFor(counts.unrouted),
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

<div class="diagram-scroll">
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
        {#if n.kind}<div class="node-kind uppercase-label">{n.kind}</div>{/if}
      </div>
    {/each}
  </div>
</div>

<style>
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
    margin-top: 0.15rem;
  }
</style>
