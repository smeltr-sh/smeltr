import { describe, expect, it } from 'vitest';
import { mapHash } from './connections';
import type {
  NodeReadingDto,
  DeliverEdge,
  ExceptionEdge,
  MapGroupError,
  MapNode,
  RouteEdge,
  UnmatchedEdge,
} from './api';
import {
  ARROW_RUN,
  COL_GAP,
  LOOP_W,
  MIN_NODE_W,
  NODE_H,
  NODE_W,
  PORT,
  cardWidth,
  edgePaths,
  edgeDimmed,
  edgeKey,
  gaps,
  layout,
  linkForError,
  mapEdges,
  nodeKey,
  parseQuery,
  presentGroups,
  search,
  routeKey,
  selectedPath,
  sharedCopies,
  stackNodes,
  stackReading,
  cardKey,
  statsHash,
  chartRange,
  DEFAULT_CHART_RANGE,
  survivors,
  usedNodes,
  visibleEdges,
  visibleNodes,
  type MapEdge,
  type KnownIds,
} from './map';
import { pathPoints } from './pathPoints';

/** Every node, route and deliver here defaults to group `g1` unless a test says otherwise, so
 * the single-group tests read exactly as they did before a node carried a group at all. */
function node(
  authorId: string,
  kind: MapNode['kind'],
  warning = false,
  name = authorId,
  group = 'g1',
  shared = false,
): MapNode {
  return { group, authorId, name, kind, shared, warning };
}

function route(source: string, pipeline: string, group = 'g1'): RouteEdge {
  return { group, route: `${source}->${pipeline}`, source, pipeline };
}

/** A delivery rule of route `route`, when a test follows a route end to end; otherwise one of
 * its own. */
function deliver(pipeline: string, destination: string, group = 'g1', route?: string): DeliverEdge {
  return { group, route: route ?? `${pipeline}->${destination}`, pipeline, destination };
}

function fallback(source: string, destination: string, group = 'g1'): UnmatchedEdge {
  return { group, source, destination };
}

function refused(from: string, destination: string, group = 'g1'): ExceptionEdge {
  return { group, fromKind: 'destination', from, destination, kinds: ['Refused'] };
}

/** The lines for one fleet, each list defaulting to empty. */
function edges(
  routes: RouteEdge[],
  delivers: DeliverEdge[] = [],
  unmatched: UnmatchedEdge[] = [],
  exceptions: ExceptionEdge[] = [],
): MapEdge[] {
  return mapEdges({ routes, delivers, unmatched, poison: [], exceptions });
}

describe('nodeKey', () => {
  it('is the same for the same group and author id, different otherwise', () => {
    expect(nodeKey('g1', 'source', 'orders-west')).toBe(nodeKey('g1', 'source', 'orders-west'));
    expect(nodeKey('g1', 'source', 'orders-west')).not.toBe(nodeKey('g2', 'source', 'orders-west'));
    expect(nodeKey('g1', 'source', 'a')).not.toBe(nodeKey('g1', 'pipeline', 'b'));
  });

  it('tells a source and a destination of the same id apart, since the two namespaces are separate', () => {
    expect(nodeKey('g1', 'source', 'dlq')).not.toBe(nodeKey('g1', 'destination', 'dlq'));
  });
});

describe('statsHash', () => {
  it('names the group, kind and id a stats view opens for, so `nav.ts` can read them back', () => {
    expect(statsHash('west', 'source', 'orders-west')).toBe(
      '#/map?group=west&kind=source&id=orders-west',
    );
  });

  it('names a panel tab, and leaves the overview implied', () => {
    expect(statsHash('west', 'destination', 'es', 'buffers')).toBe(
      '#/map?group=west&kind=destination&id=es&panel=buffers',
    );
    expect(statsHash('west', 'destination', 'es', 'overview')).toBe(
      '#/map?group=west&kind=destination&id=es',
    );
  });

  it('names a chart range only when given one', () => {
    expect(statsHash('west', 'source', 's', undefined, '7d')).toBe(
      '#/map?group=west&kind=source&id=s&range=7d',
    );
    expect(statsHash('west', 'source', 's')).toBe('#/map?group=west&kind=source&id=s');
  });

  it('encodes a piece that would otherwise break the query', () => {
    expect(statsHash('west', 'pipeline', 'orders & west')).toBe(
      '#/map?group=west&kind=pipeline&id=orders+%26+west',
    );
  });
});

describe('chartRange', () => {
  it('the range a link names, or the default for one it does not', () => {
    expect(chartRange('6h').minutes).toBe(360);
    expect(chartRange(undefined).range).toBe(DEFAULT_CHART_RANGE);
    expect(chartRange('5y').range).toBe(DEFAULT_CHART_RANGE);
  });
});

describe('presentGroups', () => {
  it('is every group with a node or a failure, sorted, deduplicated', () => {
    const nodes = [node('a', 'source', false, 'a', 'g2'), node('b', 'source', false, 'b', 'g1')];
    const errors: MapGroupError[] = [
      { group: 'g3', errors: ['broken'] },
      { group: 'g1', errors: ['also broken'] },
    ];
    expect(presentGroups(nodes, errors)).toEqual(['g1', 'g2', 'g3']);
  });

  it('is empty for an empty fleet', () => {
    expect(presentGroups([], [])).toEqual([]);
  });
});

describe('gaps', () => {
  const hec = node('hec', 'source');
  const seen = (keys: [string, string | undefined, boolean][], group = 'g1') => ({
    groupId: group,
    source: 'hec',
    rows: keys.length,
    keysDropped: 0,
    firstSeenMs: 0,
    lastSeenMs: 0,
    keys: keys.map(([key, condition, recent]) => ({
      key,
      rows: 1,
      firstSeenMs: 0,
      lastSeenMs: 0,
      condition,
      recent,
    })),
  });
  const threat = '__source.sourcetype == "pan:threat"';
  const traffic = '__source.sourcetype == "pan:traffic"';

  it('lists the streams the leader marks recent, whatever the routes say', () => {
    const g = gaps(
      [hec],
      [
        seen([
          ['pan:traffic', traffic, false],
          ['pan:threat', threat, true],
        ]),
      ],
    );
    expect(g.get(nodeKey('g1', 'source', 'hec'))).toEqual({
      streams: [{ key: 'pan:threat', rows: 1, condition: threat }],
      unnamed: false,
    });
  });

  it('has none for a source nothing recent came to untaken', () => {
    expect(gaps([hec], [seen([['pan:threat', threat, false]])]).size).toBe(0);
  });

  it("reads each group's streams apart, and says when rows came under no stream", () => {
    const g2 = node('hec', 'source', false, 'hec', 'g2');
    const g = gaps([hec, g2], [seen([['', undefined, true]], 'g2')]);
    expect([...g.entries()]).toEqual([
      [nodeKey('g2', 'source', 'hec'), { streams: [], unnamed: true }],
    ]);
  });
});

describe('usedNodes', () => {
  it("keeps a group's own nodes whether or not anything touches them", () => {
    const nodes = [node('lonely', 'source')];
    expect(usedNodes(nodes, [])).toEqual(nodes);
  });

  it('keeps a shared node only in a group where a line touches it', () => {
    const nodes = [
      node('fleet-metrics', 'source', false, 'fleet-metrics', 'g1', true),
      node('fleet-metrics', 'source', false, 'fleet-metrics', 'g2', true),
      node('normalize', 'pipeline', false, 'normalize', 'g1'),
    ];
    const used = usedNodes(nodes, edges([route('fleet-metrics', 'normalize', 'g1')]));
    expect(used.map((n) => `${n.group}/${n.authorId}`)).toEqual([
      'g1/fleet-metrics',
      'g1/normalize',
    ]);
  });

  it('reads warnings as the caller reads them', () => {
    const nodes = [node('fleet-metrics', 'source', true, 'fleet-metrics', 'g2', true)];
    expect(usedNodes(nodes, [], () => false)).toEqual([]);
  });

  it('keeps a shared node a warning names, line or not', () => {
    const nodes = [node('fleet-metrics', 'source', true, 'fleet-metrics', 'g2', true)];
    expect(usedNodes(nodes, [])).toEqual(nodes);
  });
});

describe('sharedCopies', () => {
  const nodes = [
    node('es-audit', 'destination', false, 'es-audit', 'g1', true),
    node('es-audit', 'destination', false, 'es-audit', 'g2', true),
    node('es-audit', 'source', false, 'es-audit', 'g2', true),
    node('es-prod', 'destination', false, 'es-prod', 'g1'),
  ];

  it('is every copy of one declaration, across groups and of its kind only', () => {
    expect(sharedCopies(nodes, nodes[0])).toEqual(
      new Set([nodeKey('g1', 'destination', 'es-audit'), nodeKey('g2', 'destination', 'es-audit')]),
    );
  });

  it('is empty for a node shared/ does not declare', () => {
    expect(sharedCopies(nodes, nodes[3]).size).toBe(0);
  });
});

describe('layout', () => {
  it('puts each kind in its own column, left to right', () => {
    const { positions } = layout([
      node('es-prod', 'destination'),
      node('orders-west', 'source'),
      node('orders-normalize', 'pipeline'),
    ]);
    const xOf = (id: string) => positions.find((p) => p.node.authorId === id)?.x;
    expect(xOf('orders-west')).toBeLessThan(xOf('orders-normalize')!);
    expect(xOf('orders-normalize')).toBeLessThan(xOf('es-prod')!);
  });

  it('orders a column by name, ties broken by id', () => {
    const { positions } = layout([
      node('b', 'source', false, 'Zebra'),
      node('a', 'source', false, 'Apple'),
    ]);
    expect(positions.map((p) => p.node.authorId)).toEqual(['a', 'b']);
  });

  it('a smaller node list is a smaller layout, not the same one with gaps', () => {
    const full = layout([node('a', 'source'), node('b', 'source'), node('c', 'source')]);
    const filtered = layout([node('a', 'source'), node('c', 'source')]);
    expect(filtered.positions.map((p) => p.y)).toEqual([full.positions[0].y, full.positions[1].y]);
    expect(filtered.height).toBeLessThan(full.height);
  });

  it('names all three columns, whether or not each holds anything', () => {
    const { columns } = layout([node('a', 'source')]);
    expect(columns.map((c) => c.kind)).toEqual(['source', 'pipeline', 'destination']);
  });

  it('a tie in name and id is finally broken by group, so two groups never collide in row order', () => {
    const { positions } = layout([
      node('orders-bad', 'source', false, 'orders-bad', 'g2'),
      node('orders-bad', 'source', false, 'orders-bad', 'g1'),
    ]);
    expect(positions.map((p) => p.node.group)).toEqual(['g1', 'g2']);
  });

  it('narrows its cards to the room it has, down to a floor', () => {
    expect(cardWidth(10_000)).toBe(NODE_W);
    expect(cardWidth(0)).toBe(MIN_NODE_W);
    const room = 900;
    const w = cardWidth(room);
    expect(w).toBeGreaterThan(MIN_NODE_W);
    expect(w).toBeLessThan(NODE_W);
    const fitted = layout([node('a', 'source'), node('b', 'destination')], w);
    expect(fitted.width).toBeLessThanOrEqual(room);
    expect(fitted.positions[1].x).toBe(fitted.columns[2].x);
    expect(fitted.columns[1].x - fitted.columns[0].x).toBe(w + COL_GAP);
  });
});

describe('edgePaths', () => {
  const at = (id: string, kind: MapNode['kind'], col: number, row: number) => ({
    node: node(id, kind),
    x: col * (NODE_W + COL_GAP),
    y: row * 100,
  });
  const points = (d: string) => pathPoints(d)[0];
  /** The straight run a line ends on. */
  const lastRun = (d: string) => {
    const p = points(d);
    const [a, b] = [p.at(-2)!, p.at(-1)!];
    return { horizontal: a[1] === b[1], length: Math.abs(b[0] - a[0]) };
  };

  it('draws one line between two cards in a row straight across', () => {
    const [d] = edgePaths([{ from: at('a', 'source', 0, 0), to: at('b', 'pipeline', 1, 0) }]);
    expect(points(d)).toEqual([
      [NODE_W, NODE_H / 2],
      [NODE_W + COL_GAP, NODE_H / 2],
    ]);
  });

  it("leaves a narrower card from that card's own right edge", () => {
    const w = MIN_NODE_W;
    const from = { node: node('a', 'source'), x: 0, y: 0 };
    const to = { node: node('b', 'pipeline'), x: w + COL_GAP, y: 0 };
    const [d] = edgePaths([{ from, to }], w);
    expect(points(d)).toEqual([
      [w, NODE_H / 2],
      [w + COL_GAP, NODE_H / 2],
    ]);
  });

  it('turns square in the gap and ends on a straight run as long as the arrowhead', () => {
    const a = at('a', 'source', 0, 0);
    const b = at('b', 'pipeline', 1, 2);
    const [d] = edgePaths([{ from: a, to: b }]);
    const p = points(d);
    expect(p[0]).toEqual([a.x + NODE_W, a.y + NODE_H / 2]);
    expect(p.at(-1)![0]).toBe(b.x);
    expect(Math.abs(p.at(-1)![1] - (b.y + NODE_H / 2))).toBeLessThanOrEqual(PORT / 2);
    expect(p.every(([x]) => x >= a.x + NODE_W && x <= b.x)).toBe(true);
    expect(lastRun(d).horizontal).toBe(true);
    expect(lastRun(d).length).toBeGreaterThanOrEqual(ARROW_RUN);
  });

  it('gives lines meeting one card each its own end and its own lane', () => {
    const to = at('t', 'pipeline', 1, 1);
    const ds = edgePaths([
      { from: at('a', 'source', 0, 0), to },
      { from: at('b', 'source', 0, 1), to },
      { from: at('c', 'source', 0, 3), to },
    ]);
    const ends = ds.map((d) => points(d).at(-1)![1]);
    expect(new Set(ends).size).toBe(3);
    expect(ends.every((y) => y > to.y && y < to.y + NODE_H)).toBe(true);
    // A turning line's lane is the x of its vertical run.
    const lanes = ds
      .map((d) => points(d))
      .filter((p) => p.length > 2)
      .map((p) => p[2][0]);
    expect(new Set(lanes).size).toBe(lanes.length);
    for (const d of ds) expect(lastRun(d).length).toBeGreaterThanOrEqual(ARROW_RUN);
  });

  it('runs a line alone on its side straight into the port it meets', () => {
    const to = at('es', 'destination', 2, 0);
    const ds = edgePaths([
      { from: at('a', 'pipeline', 1, 0), to },
      { from: at('b', 'pipeline', 1, 1), to },
    ]);
    const p = points(ds[0]);
    expect(p).toHaveLength(2);
    expect(p[0][1]).toBe(p[1][1]);
  });

  it('never runs a line leaving a card along one arriving at the card beside it', () => {
    // Two pipelines in one row each send two lines: one row up, one row down. The line leaving
    // `b` upward and the one arriving at `y` from `a` share the gap on that row.
    const ds = edgePaths([
      { from: at('a', 'pipeline', 1, 0), to: at('x', 'destination', 2, 0) },
      { from: at('a', 'pipeline', 1, 0), to: at('y', 'destination', 2, 2) },
      { from: at('b', 'pipeline', 1, 2), to: at('w', 'destination', 2, 1) },
      { from: at('b', 'pipeline', 1, 2), to: at('y', 'destination', 2, 2) },
    ]);
    const starts = ds.map((d) => points(d)[0][1]);
    const ends = ds.map((d) => points(d).at(-1)![1]);
    starts.forEach((y, i) => ends.forEach((e, j) => i !== j && expect(e).not.toBe(y)));
  });

  it('keeps two lines that need not cross from crossing', () => {
    const ds = edgePaths([
      { from: at('a', 'source', 0, 0), to: at('x', 'pipeline', 1, 2) },
      { from: at('b', 'source', 0, 1), to: at('y', 'pipeline', 1, 3) },
    ]);
    expect(crossings(points(ds[0]), points(ds[1]))).toBe(0);
  });

  it('crosses the pipeline column between two rows, clear of every other line', () => {
    const s0 = at('s0', 'source', 0, 0);
    const p0 = at('p0', 'pipeline', 1, 0);
    const d0 = at('d0', 'destination', 2, 0);
    const ds = edgePaths([
      { from: s0, to: p0 },
      { from: at('s1', 'source', 0, 1), to: p0 },
      { from: s0, to: d0 },
      { from: p0, to: d0 },
    ]);
    const p = points(ds[2]);
    const across = p.findIndex(([x], i) => x < p0.x && p[i + 1]?.[0] > p0.x + NODE_W);
    const corridor = p[across][1];
    expect(p[across + 1][1]).toBe(corridor);
    expect(corridor).toBeGreaterThan(p0.y + NODE_H);
    expect(corridor).toBeLessThan(p0.y + 100);
    const runs = (d: string) =>
      points(d)
        .slice(1)
        .map((q, i) => [points(d)[i], q] as const);
    for (const [i, d] of ds.entries())
      for (const [j, e] of ds.entries())
        if (i !== j)
          for (const [a, b] of runs(d))
            for (const [c, f] of runs(e))
              if (a[1] === b[1] && c[1] === f[1] && a[1] === c[1])
                expect(
                  Math.min(Math.max(a[0], b[0]), Math.max(c[0], f[0])) -
                    Math.max(Math.min(a[0], b[0]), Math.min(c[0], f[0])),
                ).toBeLessThanOrEqual(0);
  });

  it('bends a line between two cards in one column out past their right edges', () => {
    const a = { node: node('es-prod', 'destination'), x: 600, y: 0 };
    const b = { node: node('quarantine', 'destination'), x: 600, y: 84 };
    const [d] = edgePaths([{ from: a, to: b }]);
    const p = points(d);
    expect(p[0]).toEqual([a.x + NODE_W, a.y + NODE_H / 2]);
    expect(p.at(-1)![0]).toBe(b.x + NODE_W);
    expect(Math.abs(p.at(-1)![1] - (b.y + NODE_H / 2))).toBeLessThanOrEqual(PORT / 2);
    expect(Math.max(...p.map(([x]) => x))).toBeLessThanOrEqual(a.x + NODE_W + LOOP_W);
    expect(lastRun(d).length).toBeGreaterThanOrEqual(ARROW_RUN);
  });
});

/** How many times two square lines cross, read from their corner points. */
function crossings(a: [number, number][], b: [number, number][]): number {
  const segs = (p: [number, number][]) => p.slice(1).map((q, i) => [p[i], q] as const);
  let n = 0;
  for (const [p, q] of segs(a))
    for (const [r, s] of segs(b)) {
      const [h, v] =
        p[1] === q[1] && r[0] === s[0]
          ? [
              [p, q],
              [r, s],
            ]
          : [
              [r, s],
              [p, q],
            ];
      if (h[0][1] !== h[1][1] || v[0][0] !== v[1][0]) continue;
      const x = v[0][0];
      const y = h[0][1];
      const within = (t: number, u: number, w: number) => t > Math.min(u, w) && t < Math.max(u, w);
      if (within(x, h[0][0], h[1][0]) && within(y, v[0][1], v[1][1])) n++;
    }
  return n;
}

describe('mapEdges', () => {
  it('collapses two routes between the same source and pipeline into one line that names both', () => {
    const lines = edges([
      route('orders-west', 'normalize'),
      { group: 'g1', route: 'r2', source: 'orders-west', pipeline: 'normalize' },
    ]);
    expect(lines).toEqual([
      {
        kind: 'route',
        from: nodeKey('g1', 'source', 'orders-west'),
        to: nodeKey('g1', 'pipeline', 'normalize'),
        routes: [routeKey('g1', 'orders-west->normalize'), routeKey('g1', 'r2')],
      },
    ]);
  });

  it("never merges two groups' lines, even between the same-named pair", () => {
    expect(edges([route('a', 'b', 'g1'), route('a', 'b', 'g2')])).toHaveLength(2);
  });

  it('keeps an unmatched and a poison line to one destination apart', () => {
    const lines = mapEdges({
      routes: [],
      delivers: [],
      unmatched: [fallback('a', 'dlq')],
      poison: [{ group: 'g1', source: 'a', destination: 'dlq', throughput: 1 }],
      exceptions: [],
    });
    expect(lines.map((e) => e.kind).sort()).toEqual(['poison', 'unmatched']);
  });

  it('draws an exception line from the node that answers it', () => {
    const [line] = edges([], [], [], [refused('es-prod', 'quarantine')]);
    expect(line).toEqual({
      kind: 'exception',
      from: nodeKey('g1', 'destination', 'es-prod'),
      to: nodeKey('g1', 'destination', 'quarantine'),
      routes: [],
    });
  });
});

describe('selectedPath', () => {
  // Routes, each end to end: orders-west → normalize → es-prod, clicks → normalize → es-prod,
  // clicks → sessionize → es-audit. es-prod ⇢ quarantine (refused); orders-west ⇢ leftovers
  // (unmatched).
  const lines = edges(
    [
      route('orders-west', 'normalize'),
      route('clicks', 'sessionize'),
      route('clicks', 'normalize'),
    ],
    [
      deliver('normalize', 'es-prod', 'g1', 'orders-west->normalize'),
      deliver('normalize', 'es-prod', 'g1', 'clicks->normalize'),
      deliver('sessionize', 'es-audit', 'g1', 'clicks->sessionize'),
    ],
    [fallback('orders-west', 'leftovers')],
    [refused('es-prod', 'quarantine')],
  );
  const k = (kind: MapNode['kind'], id: string) => nodeKey('g1', kind, id);
  const ids = (s: Set<string>) => [...s].map((key) => (JSON.parse(key) as string[])[2]).sort();
  const onPath = (p: { edges: Set<string> }, from: string, to: string) =>
    [...p.edges].some((e) => e.includes(`"${from}"`) && e.includes(`"${to}"`));

  it('from a source, is its routes end to end, and the fallbacks it can take', () => {
    const p = selectedPath(lines, k('source', 'orders-west'), 'source');
    expect(ids(p.nodes)).toEqual([
      'es-prod',
      'leftovers',
      'normalize',
      'orders-west',
      'quarantine',
    ]);
  });

  it('from a destination, is the routes delivering to it, and no fallback their sources take', () => {
    const p = selectedPath(lines, k('destination', 'es-prod'), 'destination');
    expect(ids(p.nodes)).toEqual(['clicks', 'es-prod', 'normalize', 'orders-west', 'quarantine']);
    expect([...p.edges].some((e) => e.startsWith('unmatched'))).toBe(false);
  });

  it('from a pipeline, is every route through it', () => {
    const p = selectedPath(lines, k('pipeline', 'normalize'), 'pipeline');
    expect(ids(p.nodes)).toEqual(['clicks', 'es-prod', 'normalize', 'orders-west', 'quarantine']);
  });

  it('a fallback destination has for its path whatever falls into it', () => {
    const p = selectedPath(lines, k('destination', 'leftovers'), 'destination');
    expect(ids(p.nodes)).toEqual(['leftovers', 'orders-west']);
    expect(p.edges).toEqual(
      new Set([
        edgeKey({
          kind: 'unmatched',
          from: k('source', 'orders-west'),
          to: k('destination', 'leftovers'),
        }),
      ]),
    );
  });

  it('a line with one end off the path is not on it', () => {
    const p = selectedPath(lines, k('source', 'orders-west'), 'source');
    expect(onPath(p, 'clicks', 'normalize')).toBe(false);
    expect(onPath(p, 'orders-west', 'normalize')).toBe(true);
  });

  it("a shared pipeline joins a source only to its own route's destinations", () => {
    const shared = edges(
      [route('a', 'p'), route('b', 'p')],
      [deliver('p', 'x', 'g1', 'a->p'), deliver('p', 'y', 'g1', 'b->p')],
    );
    const p = selectedPath(shared, k('source', 'a'), 'source');
    expect(ids(p.nodes)).toEqual(['a', 'p', 'x']);
    expect(onPath(p, 'p', 'y')).toBe(false);
    const q = selectedPath(shared, k('destination', 'y'), 'destination');
    expect(ids(q.nodes)).toEqual(['b', 'p', 'y']);
  });

  it('from a destination, is its routes up to it, not where else they deliver', () => {
    const fanOut = edges(
      [route('a', 'p')],
      [deliver('p', 'x', 'g1', 'a->p'), deliver('p', 'y', 'g1', 'a->p')],
    );
    const p = selectedPath(fanOut, k('destination', 'x'), 'destination');
    expect(ids(p.nodes)).toEqual(['a', 'p', 'x']);
    expect(ids(selectedPath(fanOut, k('source', 'a'), 'source').nodes)).toEqual([
      'a',
      'p',
      'x',
      'y',
    ]);
  });

  it("a destination's refusal brings the destination it refuses to, not that one's own routes", () => {
    const fleet = edges(
      [route('orders-west', 'normalize'), route('audit', 'keep')],
      [
        deliver('normalize', 'es-prod', 'g1', 'orders-west->normalize'),
        deliver('keep', 'quarantine', 'g1', 'audit->keep'),
      ],
      [],
      [refused('es-prod', 'quarantine')],
    );
    const p = selectedPath(fleet, k('destination', 'es-prod'), 'destination');
    expect(ids(p.nodes)).toEqual(['es-prod', 'normalize', 'orders-west', 'quarantine']);
    expect(onPath(p, 'keep', 'quarantine')).toBe(false);
  });

  it('a refusal chain carries on past the first destination, both ways', () => {
    const chain = edges(
      [route('orders', 'p')],
      [deliver('p', 'a', 'g1', 'orders->p')],
      [],
      [refused('a', 'b'), refused('b', 'c')],
    );
    expect(ids(selectedPath(chain, k('source', 'orders'), 'source').nodes)).toEqual([
      'a',
      'b',
      'c',
      'orders',
      'p',
    ]);
    const intoC = selectedPath(chain, k('destination', 'c'), 'destination');
    expect(ids(intoC.nodes)).toEqual(['a', 'b', 'c', 'orders', 'p']);
    expect(onPath(intoC, 'a', 'b')).toBe(true);
  });

  it("a pipeline's exception answer is on a path only for its own route", () => {
    const fleet = mapEdges({
      routes: [route('a', 'p'), route('b', 'p')],
      delivers: [deliver('p', 'x', 'g1', 'a->p'), deliver('p', 'y', 'g1', 'b->p')],
      unmatched: [],
      poison: [],
      exceptions: [
        {
          group: 'g1',
          fromKind: 'pipeline',
          from: 'p',
          route: 'b->p',
          destination: 'dlq',
          kinds: ['Error'],
        },
      ],
    });
    expect(ids(selectedPath(fleet, k('source', 'a'), 'source').nodes)).toEqual(['a', 'p', 'x']);
    expect(ids(selectedPath(fleet, k('source', 'b'), 'source').nodes)).toEqual([
      'b',
      'dlq',
      'p',
      'y',
    ]);
    // What falls into the DLQ comes from route b, back to its source.
    const into = selectedPath(fleet, k('destination', 'dlq'), 'destination');
    expect(ids(into.nodes)).toEqual(['b', 'dlq', 'p']);
  });
});

describe('edgeDimmed', () => {
  it('full strength only when both ends are matches', () => {
    const matched = new Set(['a', 'b']);
    expect(edgeDimmed(matched, { from: 'a', to: 'b' })).toBe(false);
    expect(edgeDimmed(matched, { from: 'a', to: 'c' })).toBe(true);
    expect(edgeDimmed(matched, { from: 'c', to: 'd' })).toBe(true);
  });
});

describe('parseQuery', () => {
  it('splits on whitespace and lowercases', () => {
    expect(parseQuery(' Orders  kind:Source ')).toEqual([
      { field: null, value: 'orders' },
      { field: 'kind', value: 'source' },
    ]);
  });

  it('keeps a quoted value whole, and an id its case', () => {
    expect(parseQuery('id:"My \\"big\\" one" "Two Words" kind:"Source"')).toEqual([
      { field: 'id', value: 'My "big" one' },
      { field: null, value: 'two words' },
      { field: 'kind', value: 'source' },
    ]);
  });
});

describe('search', () => {
  const nodes = [
    node('orders-west', 'source'),
    node('clicks-west', 'source'),
    node('orders-normalize', 'pipeline'),
    node('es-prod', 'destination', true),
    node('es-audit', 'destination'),
  ];
  const lines = edges(
    [route('orders-west', 'orders-normalize'), route('clicks-west', 'orders-normalize')],
    [deliver('orders-normalize', 'es-prod'), deliver('orders-normalize', 'es-audit')],
  );
  const callers: Record<string, string[]> = { 'enrich-geo': ['orders-normalize'] };
  const key = (kind: MapNode['kind'], id: string) => nodeKey('g1', kind, id);

  it('a matched source keeps its DLQ, so the edge between them still draws', () => {
    const r = search(
      [...nodes, node('leftovers', 'destination')],
      [...lines, ...edges([], [], [fallback('orders-west', 'leftovers')])],
      'orders-west',
      callers,
    );
    expect(r.dimmed.has(key('destination', 'leftovers'))).toBe(true);
  });

  it('a term on a reading matches what reads that way now', () => {
    const held = (n: MapNode) =>
      n.authorId === 'es-prod'
        ? {
            group: 'g1',
            kind: n.kind,
            id: n.authorId,
            queue: { busiest: 1.1, nearCapacity: 1, fresh: 1, stale: 0, workers: 1 },
            warnings: [],
          }
        : undefined;
    const r = search(nodes, lines, 'pressure:held', callers, held);
    expect(r.matched).toEqual(new Set([key('destination', 'es-prod')]));
    expect(search(nodes, lines, 'kind:dest es', callers).matched.size).toBe(2);
  });

  it('a value no term of a known field has says so, rather than matching nothing', () => {
    const r = search(nodes, lines, 'pressure:soon', callers);
    expect(r.unsupported.map((u) => u.field)).toEqual(['pressure:soon']);
  });

  it('`shared` keeps what is declared in shared/', () => {
    const fleet = [
      ...nodes,
      node('alerts', 'destination', false, 'alerts', 'g1', true),
      node('shared-archive', 'destination'),
    ];
    expect(search(fleet, lines, 'shared', callers).matched).toEqual(
      new Set([key('destination', 'alerts')]),
    );
  });

  it('a term still being typed filters nothing and raises nothing', () => {
    const r = search(nodes, lines, 'pressure:he', callers);
    expect(r.unsupported).toEqual([]);
    expect(r.matched.size).toBe(nodes.length);
  });

  it('an empty query matches everything and dims nothing', () => {
    const r = search(nodes, lines, '', callers);
    expect(r.active).toBe(false);
    expect(r.matched.size).toBe(nodes.length);
    expect(r.dimmed.size).toBe(0);
  });

  it('a bare term matches by name or id, case-insensitively', () => {
    const r = search(nodes, lines, 'Orders-West', callers);
    expect(r.matched).toEqual(new Set([key('source', 'orders-west')]));
  });

  it("a match's direct neighbors survive dimmed, not hidden", () => {
    const r = search(nodes, lines, 'orders-west', callers);
    expect(r.dimmed).toEqual(new Set([key('pipeline', 'orders-normalize')]));
    expect(survivors(r)).toEqual(
      new Set([key('source', 'orders-west'), key('pipeline', 'orders-normalize')]),
    );
  });

  it('everything connected to no match is gone entirely', () => {
    const r = search(nodes, lines, 'orders-west', callers);
    expect(survivors(r).has(key('source', 'clicks-west'))).toBe(false);
    expect(survivors(r).has(key('destination', 'es-prod'))).toBe(false);
    expect(survivors(r).has(key('destination', 'es-audit'))).toBe(false);
  });

  it('kind: narrows to one node kind', () => {
    const r = search(nodes, lines, 'kind:destination', callers);
    expect(r.matched).toEqual(
      new Set([key('destination', 'es-prod'), key('destination', 'es-audit')]),
    );
  });

  it('calls:enrich-geo finds every direct caller', () => {
    const r = search(nodes, lines, 'calls:enrich-geo', callers);
    expect(r.matched).toEqual(new Set([key('pipeline', 'orders-normalize')]));
  });

  it('two terms AND together', () => {
    const r = search(nodes, lines, 'kind:source west', callers);
    expect(r.matched).toEqual(
      new Set([key('source', 'orders-west'), key('source', 'clicks-west')]),
    );
    const narrowed = search(nodes, lines, 'kind:source orders', callers);
    expect(narrowed.matched).toEqual(new Set([key('source', 'orders-west')]));
  });

  it('a field nothing can match says so instead of silently finding nothing', () => {
    const r = search(nodes, lines, 'tag:pii', callers);
    expect(r.unsupported).toEqual([
      { field: 'tag', note: "object tags aren't built in this codebase yet" },
    ]);
    expect(r.matched.size).toBe(0);
  });

  it('an unsupported field does not swallow the rest of an otherwise usable query', () => {
    const r = search(nodes, lines, 'kind:source group:west', callers);
    expect(r.unsupported).toHaveLength(1);
    expect(r.matched).toEqual(
      new Set([key('source', 'orders-west'), key('source', 'clicks-west')]),
    );
  });

  it('an unrecognized field is flagged the same way, not matched as if it were a name', () => {
    const r = search(nodes, lines, 'nonsense:foo', callers);
    expect(r.unsupported).toEqual([
      { field: 'nonsense', note: 'the map does not index `nonsense` yet' },
    ]);
  });

  it('calls: matches a callee id in any group, since pipeline-callers answers for one editor scope', () => {
    const twoGroups = [
      node('orders-normalize', 'pipeline', false, 'orders-normalize', 'g1'),
      node('orders-normalize', 'pipeline', false, 'orders-normalize', 'g2'),
    ];
    const r = search(twoGroups, [], 'calls:enrich-geo', callers);
    expect(r.matched).toEqual(
      new Set([
        nodeKey('g1', 'pipeline', 'orders-normalize'),
        nodeKey('g2', 'pipeline', 'orders-normalize'),
      ]),
    );
  });
});

describe('search by id', () => {
  it('id: matches the whole id in every group, and not an id that holds it', () => {
    const nodes = [
      node('normalize', 'pipeline', false, 'normalize', 'g1'),
      node('normalize', 'pipeline', false, 'normalize', 'g2'),
      node('normalize-v2', 'pipeline', false, 'normalize-v2', 'g1'),
    ];
    const r = search(nodes, [], 'kind:pipeline id:normalize', {});
    expect(r.unsupported).toEqual([]);
    expect(r.matched).toEqual(
      new Set([nodeKey('g1', 'pipeline', 'normalize'), nodeKey('g2', 'pipeline', 'normalize')]),
    );
  });

  it('id: finds the id the Connections line quotes, whatever it holds, and in its own case', () => {
    const id = 'Edge "A" 2';
    const nodes = [
      node(id, 'source', false, id, 'g1'),
      node('edge "a" 2', 'source', false, 'x', 'g2'),
    ];
    const q = new URLSearchParams(mapHash('source', id, ['g1', 'g2'])!.split('?')[1]).get('q')!;
    expect(search(nodes, [], q, {}).matched).toEqual(new Set([nodeKey('g1', 'source', id)]));
  });
});

describe('search across groups', () => {
  it("a bare term matches both groups' same-named node, each its own key", () => {
    const nodes = [
      node('orders-bad', 'source', false, 'orders-bad', 'g1'),
      node('orders-bad', 'source', false, 'orders-bad', 'g2'),
    ];
    const r = search(nodes, [], 'orders-bad', {});
    expect(r.matched).toEqual(
      new Set([nodeKey('g1', 'source', 'orders-bad'), nodeKey('g2', 'source', 'orders-bad')]),
    );
  });

  it("a match's neighbor survives dimmed in its own group only", () => {
    const nodes = [
      node('orders-west', 'source', false, 'orders-west', 'g1'),
      node('normalize', 'pipeline', false, 'normalize', 'g1'),
      node('orders-west', 'source', false, 'orders-west', 'g2'),
      node('normalize', 'pipeline', false, 'normalize', 'g2'),
    ];
    const lines = edges([
      route('orders-west', 'normalize', 'g1'),
      route('orders-west', 'normalize', 'g2'),
    ]);
    const r = search(nodes, lines, 'orders-west', {});
    expect(r.matched).toEqual(
      new Set([nodeKey('g1', 'source', 'orders-west'), nodeKey('g2', 'source', 'orders-west')]),
    );
    expect(r.dimmed).toEqual(
      new Set([nodeKey('g1', 'pipeline', 'normalize'), nodeKey('g2', 'pipeline', 'normalize')]),
    );
  });
});

describe('visibleNodes / visibleEdges', () => {
  it('keeps only survivors, and only lines with both ends surviving', () => {
    const nodes = [node('a', 'source'), node('b', 'pipeline'), node('c', 'destination')];
    const lines = edges([route('a', 'b')], [deliver('b', 'c')]);
    const ids = new Set([nodeKey('g1', 'source', 'a'), nodeKey('g1', 'pipeline', 'b')]);
    expect(visibleNodes(nodes, ids).map((n) => n.authorId)).toEqual(['a', 'b']);
    expect(visibleEdges(lines, ids).map((e) => e.kind)).toEqual(['route']);
  });

  it("does not let one group's survivor id make another group's line visible", () => {
    const lines = edges([route('a', 'b', 'g2')]);
    const ids = new Set([nodeKey('g1', 'source', 'a'), nodeKey('g1', 'pipeline', 'b')]);
    expect(visibleEdges(lines, ids)).toEqual([]);
  });
});

describe('linkForError', () => {
  const known: KnownIds = {
    pipelines: [
      { id: 'pipeline-n0rm4l1z', name: 'orders normalize' },
      { id: 'pipeline-0rd3r5x1', name: 'orders' },
    ],
    sources: ['orders-west'],
    destinations: ['es-prod'],
    routes: [{ id: 'r1', name: 'orders-west \u2192 orders' }],
  };

  it('an unreadable pipeline file links straight to that pipeline, known or not', () => {
    expect(linkForError('pipelines/bad-one.json: invalid JSON: EOF', known)).toEqual({
      label: 'pipeline bad-one',
      hash: '#/build/pipelines/bad-one',
    });
  });

  it('an unreadable file of a known pipeline is named by its name', () => {
    expect(linkForError('pipelines/pipeline-0rd3r5x1.json: invalid JSON: EOF', known)).toEqual({
      label: 'pipeline orders',
      hash: '#/build/pipelines/pipeline-0rd3r5x1',
    });
  });

  it('names a pipeline the message names by id by its name', () => {
    expect(linkForError('route "r1": pipeline "pipeline-n0rm4l1z" does not exist', known)).toEqual({
      label: 'pipeline orders normalize',
      hash: '#/build/pipelines/pipeline-n0rm4l1z',
    });
  });

  it('names a pipeline the message names by name, as the compiler writes it', () => {
    expect(linkForError('pipeline "orders normalize": step "s1": bad regex', known)).toEqual({
      label: 'pipeline orders normalize',
      hash: '#/build/pipelines/pipeline-n0rm4l1z',
    });
  });

  it('matches a quoted name whole, not a shorter name it starts with', () => {
    expect(linkForError('pipeline "orders": step "s1": bad regex', known)).toEqual({
      label: 'pipeline orders',
      hash: '#/build/pipelines/pipeline-0rd3r5x1',
    });
  });

  it('prefers a pipeline name over a source id inside that name', () => {
    expect(
      linkForError('pipeline "orders-west cleanup": step "s1": bad regex', {
        ...known,
        pipelines: [...known.pipelines, { id: 'pipeline-w3st0001', name: 'orders-west cleanup' }],
      }),
    ).toEqual({
      label: 'pipeline orders-west cleanup',
      hash: '#/build/pipelines/pipeline-w3st0001',
    });
  });

  it('matches a name with a quote in it the way the compiler escapes it', () => {
    expect(
      linkForError('pipeline "the \\"big\\" one": step "s1": bad regex', {
        ...known,
        pipelines: [{ id: 'pipeline-b1g00001', name: 'the "big" one' }],
      }),
    ).toEqual({
      label: 'pipeline the "big" one',
      hash: '#/build/pipelines/pipeline-b1g00001',
    });
  });

  it('links to no pipeline when two share the name the message gives', () => {
    expect(
      linkForError('pipeline "orders": step "s1": bad regex', {
        ...known,
        sources: ['orders'],
        pipelines: [...known.pipelines, { id: 'pipeline-0rd3r5x2', name: 'orders' }],
      }),
    ).toBeNull();
  });

  it('prefers the longest known id a message contains', () => {
    expect(
      linkForError('source "orders-west" is unreachable', {
        ...known,
        sources: ['orders', 'orders-west'],
      }),
    ).toEqual({
      label: 'source orders-west',
      hash: '#/build/sources/orders-west',
    });
  });

  it('links a source or a destination by the same rule', () => {
    expect(linkForError('source "orders-west" is unreachable', known)).toEqual({
      label: 'source orders-west',
      hash: '#/build/sources/orders-west',
    });
    expect(linkForError('destination "es-prod" refused the bundle', known)).toEqual({
      label: 'destination es-prod',
      hash: '#/build/destinations/es-prod',
    });
  });

  it('a route still lands on the routes editor, named by its name', () => {
    const route = { label: 'route orders-west \u2192 orders', hash: '#/build/routes' };
    expect(linkForError('route "r1" names no pipeline', known)).toEqual(route);
    // Its name holds its source's id, which it outweighs.
    expect(
      linkForError('route orders-west \u2192 orders answers Unmatched with tag', known),
    ).toEqual(route);
  });

  it('names nothing it cannot tie to an object', () => {
    expect(linkForError('sources.json: invalid JSON: EOF', known)).toBeNull();
  });
});

describe('stackNodes', () => {
  const title = (count: number, kind: string) => `${count} ${kind}`;
  const kafka = (id: string, group = 'g1'): MapNode => ({
    ...node(id, 'source', false, id, group),
    system: 'kafka',
  });
  const s3 = (id: string): MapNode => ({ ...node(id, 'destination'), system: 's3' });
  const none = () => false;

  it('stacks sources of one system into the pipelines they share, one line for the stack', () => {
    const nodes = [kafka('a'), kafka('b'), kafka('c'), node('p', 'pipeline')];
    const lines = edges([route('a', 'p'), route('b', 'p'), route('c', 'p')]);
    const { cards, edges: drawn } = stackNodes(nodes, lines, none, title);
    const stack = cards.find((c) => c.members)!;
    expect(stack.members!.map((m) => m.authorId)).toEqual(['a', 'b', 'c']);
    expect(stack.name).toBe('3 source');
    expect(cards.map((c) => c.authorId)).toEqual(['p', stack.authorId]);
    expect(drawn).toHaveLength(1);
    expect(drawn[0].from).toBe(cardKey(stack));
    expect(drawn[0].routes).toHaveLength(3);
  });

  it('stacks destinations one pipeline delivers to', () => {
    const nodes = [node('p', 'pipeline'), s3('x'), s3('y'), s3('z')];
    const lines = edges([], [deliver('p', 'x'), deliver('p', 'y'), deliver('p', 'z')]);
    const { cards } = stackNodes(nodes, lines, none, title);
    expect(cards.filter((c) => c.members)).toHaveLength(1);
  });

  it('leaves fewer than three alone', () => {
    const nodes = [kafka('a'), kafka('b'), node('p', 'pipeline')];
    const { cards } = stackNodes(nodes, edges([route('a', 'p'), route('b', 'p')]), none, title);
    expect(cards.some((c) => c.members)).toBe(false);
  });

  it('keeps apart a different system, different pipelines, another group, and what `apart` takes', () => {
    const nodes = [
      kafka('a'),
      kafka('b'),
      kafka('c'),
      kafka('d'),
      { ...node('h', 'source'), system: 'splunkHec' } as MapNode,
      kafka('q'),
      kafka('e', 'g2'),
      node('p', 'pipeline'),
      node('p2', 'pipeline'),
    ];
    const lines = edges([
      route('a', 'p'),
      route('b', 'p'),
      route('c', 'p'),
      route('d', 'p'),
      route('h', 'p'),
      route('q', 'p'),
      route('q', 'p2'),
      route('e', 'p', 'g2'),
    ]);
    const { cards } = stackNodes(nodes, lines, (k) => k === nodeKey('g1', 'source', 'd'), title);
    const stack = cards.find((c) => c.members)!;
    expect(stack.members!.map((m) => m.authorId)).toEqual(['a', 'b', 'c']);
    expect(
      cards
        .filter((c) => !c.members)
        .map((c) => c.authorId)
        .sort(),
    ).toEqual(['d', 'e', 'h', 'p', 'p2', 'q']);
  });

  it('never stacks a shared object or one on no route', () => {
    const shared = (id: string) => ({ ...kafka(id), shared: true });
    const nodes = [
      shared('a'),
      shared('b'),
      shared('c'),
      kafka('u1'),
      kafka('u2'),
      kafka('u3'),
      node('p', 'pipeline'),
    ];
    const lines = edges(
      [route('a', 'p'), route('b', 'p'), route('c', 'p')],
      [],
      [fallback('u1', 'dlq'), fallback('u2', 'dlq'), fallback('u3', 'dlq')],
    );
    expect(stackNodes(nodes, lines, none, title).cards.some((c) => c.members)).toBe(false);
  });

  it("moves a member's fallback lines to its stack", () => {
    const nodes = [
      kafka('a'),
      kafka('b'),
      kafka('c'),
      node('p', 'pipeline'),
      node('dlq', 'destination'),
    ];
    const lines = edges(
      [route('a', 'p'), route('b', 'p'), route('c', 'p')],
      [],
      [fallback('a', 'dlq'), fallback('b', 'dlq')],
    );
    const { cards, edges: drawn } = stackNodes(nodes, lines, none, title);
    const stack = cards.find((c) => c.members)!;
    expect(drawn.filter((e) => e.kind === 'unmatched')).toEqual([
      {
        kind: 'unmatched',
        from: cardKey(stack),
        to: nodeKey('g1', 'destination', 'dlq'),
        routes: [],
      },
    ]);
  });

  it("draws a selected stack's path through what its members reach", () => {
    const nodes = [kafka('a'), kafka('b'), kafka('c'), node('p', 'pipeline'), s3('x')];
    const lines = edges(
      [route('a', 'p'), route('b', 'p'), route('c', 'p')],
      [deliver('p', 'x', 'g1', 'a->p')],
    );
    const { cards, edges: drawn } = stackNodes(nodes, lines, none, title);
    const stack = cards.find((c) => c.members)!;
    const path = selectedPath(drawn, cardKey(stack), 'source');
    expect(path.nodes.has(nodeKey('g1', 'destination', 'x'))).toBe(true);
  });

  it("keys a stack apart from any object's key", () => {
    const stack = { ...kafka('a'), members: [kafka('a')] };
    expect(cardKey(stack)).not.toBe(nodeKey('g1', 'source', 'a'));
    expect(cardKey(kafka('a'))).toBe(nodeKey('g1', 'source', 'a'));
  });
});

describe('stackReading', () => {
  const reading = (id: string, r: Partial<NodeReadingDto>): NodeReadingDto => ({
    group: 'g1',
    kind: 'source',
    id,
    warnings: [],
    ...r,
  });

  it('sums rates and takes the most lag and the fullest buffer', () => {
    const members = ['a', 'b', 'c'].map((id) => node(id, 'source'));
    const stack = { ...members[0], authorId: 's', members };
    const readings: Record<string, NodeReadingDto> = {
      a: reading('a', {
        rate: 10,
        lag: 5,
        queue: { busiest: 0.2, nearCapacity: 0, fresh: 1, stale: 0, workers: 1 },
      }),
      b: reading('b', { rate: 5, lag: 50 }),
      c: reading('c', { queue: { busiest: 0.6, nearCapacity: 0, fresh: 1, stale: 0, workers: 1 } }),
    };
    const r = stackReading(stack, (n) => readings[n.authorId])!;
    expect(r.rate).toBe(15);
    expect(r.lag).toBe(50);
    expect(r.queue?.busiest).toBe(0.6);
    expect(r.warnings).toEqual([]);
  });

  it('is absent when no member reports', () => {
    const members = ['a', 'b', 'c'].map((id) => node(id, 'source'));
    expect(stackReading({ ...members[0], members }, () => undefined)).toBeUndefined();
  });
});
