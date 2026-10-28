// The topology map's geometry, search and paint (design doc "The map"): pure functions
// `MapPage.svelte` wires to a fetch, a search box, a group filter and a click. `layout` lays out
// one group's nodes at a time -- `MapPage.svelte` calls it once per band -- in three fixed
// columns -- source, pipeline, destination -- so, unlike `RoutePreviewPage`'s one-sample diagram,
// a node's column is its kind and there is no per-run layout to discover.

import type {
  DeliverEdge,
  ExceptionEdge,
  MapGroupError,
  MapNode,
  MapNodeKind,
  NodeReadingDto,
  PoisonEdge,
  RouteEdge,
  UnmatchedEdge,
  UnmatchedStreamsDto,
} from './api';
import { TERM_FIELDS, termFor, termsFor } from './mapTerms';

// -- identity -------------------------------------------------------------
//
// An author id is unique only within one group plus `shared/`, and only among objects of its own
// kind -- `config_file::load` numbers sources, destinations and pipelines in three separate
// namespaces, so one bundle may hold a source and a destination both called `dlq`. A node's real
// key is all three, and two nodes sharing any two of them must never collide in a `Set` or a
// `Map`.

export function nodeKey(group: string, kind: MapNodeKind, authorId: string): string {
  return JSON.stringify([group, kind, authorId]);
}

/** The group a `nodeKey` names. */
export function groupOfKey(key: string): string {
  return (JSON.parse(key) as [string, MapNodeKind, string])[0];
}

/** The map hash with one node's stats open. Carries the click in the query, so opening it is a
 * link and the back button closes it instead of leaving the map (`nav.ts` reads it back into
 * `Route`'s `stats` field). */
export function statsHash(
  group: string,
  kind: MapNodeKind,
  id: string,
  panel?: string,
  range?: string,
): string {
  const q = new URLSearchParams({ group, kind, id });
  if (panel && panel !== 'overview') q.set('panel', panel);
  if (range) q.set('range', range);
  return `#/map?${q}`;
}

/** How far back a panel's charts reach, by the name the link carries. */
export const CHART_RANGES: { range: string; label: string; minutes: number }[] = [
  { range: '10m', label: '10 min', minutes: 10 },
  { range: '1h', label: '1 hour', minutes: 60 },
  { range: '6h', label: '6 hours', minutes: 6 * 60 },
  { range: '24h', label: '24 hours', minutes: 24 * 60 },
  { range: '7d', label: '7 days', minutes: 7 * 24 * 60 },
  { range: '30d', label: '30 days', minutes: 30 * 24 * 60 },
];

/** Ten minutes is short enough that it is about now rather than about today, which is what someone
 * who just clicked a node is asking. */
export const DEFAULT_CHART_RANGE = '10m';

/** The range a link names, or the default for one it does not. */
export function chartRange(range: string | undefined): (typeof CHART_RANGES)[number] {
  return (
    CHART_RANGES.find((r) => r.range === range) ??
    CHART_RANGES.find((r) => r.range === DEFAULT_CHART_RANGE)!
  );
}

/** The map hash with a stack selected: its group, kind and the stack's `authorId`. */
export function stackHash(group: string, kind: MapNodeKind, id: string): string {
  return `#/map?${new URLSearchParams({ group, kind, id, stack: '1' })}`;
}

// -- grouping (design doc "One fleet, many groups") ------------------------
//
// The map's own filter, unrelated to the scope an editor is working in (`MapPage.svelte`):
// narrows which groups are drawn, defaulting to all of them.

/** Every group the fleet holds an opinion about -- drew a node, or failed to compile -- sorted
 * for a stable filter list and a stable band order. */
export function presentGroups(nodes: MapNode[], groupErrors: MapGroupError[]): string[] {
  const ids = new Set<string>();
  for (const n of nodes) ids.add(n.group);
  for (const e of groupErrors) ids.add(e.group);
  return [...ids].sort((a, b) => a.localeCompare(b));
}

// -- shared/ (design doc "One fleet, many groups": "shared logic written once") -------------
//
// A node marked `shared` is declared once in `shared/` and merged into every group -- running there
// as its own thing, with its own counts and warning (`src/leader/map.rs`'s module doc). The map
// draws it in each group that uses it, and lights every copy when one is selected.

/** The nodes a group actually uses: its own, and a shared one only where a line touches it or
 * `warned` says a warning names it -- `shared/` merges every declaration into every group, so the
 * rest would repeat it where nothing runs through it. */
export function usedNodes(
  nodes: MapNode[],
  edges: MapEdge[],
  warned: (n: MapNode) => boolean = (n) => n.warning,
): MapNode[] {
  const touched = new Set<string>();
  for (const e of edges) {
    touched.add(e.from);
    touched.add(e.to);
  }
  return nodes.filter(
    (n) => !n.shared || warned(n) || touched.has(nodeKey(n.group, n.kind, n.authorId)),
  );
}

// -- gaps -----------------------------------------------------------------
//
// What arrives that no route takes: a source with no route at all, and each stream of a routed
// source that none of its routes' conditions select (`GET /api/streams`). The leader remembers a
// stream after a route takes it, so a stream is a gap only while no route takes it now.

/** One stream of a source no route takes, and how many of its rows arrived untaken. `condition` selects it, where
 * the group's deployed bundle declares the source. */
export type StreamGap = { key: string; rows: number; condition?: string };

/** What a source received within the hour that no route took, as its `streamUnrouted` warning says it: the streams,
 * and whether rows came under no stream. */
export type SourceGap = { streams: StreamGap[]; unnamed: boolean };

/** Each source node with a gap, by `nodeKey`: the streams the leader marks `recent`, which are the ones its
 * `streamUnrouted` warning names. */
export function gaps(nodes: MapNode[], unmatched: UnmatchedStreamsDto[]): Map<string, SourceGap> {
  const out = new Map<string, SourceGap>();
  for (const n of nodes) {
    if (n.kind !== 'source') continue;
    const recent = unmatched
      .filter((u) => u.groupId === n.group && u.source === n.authorId)
      .flatMap((u) => u.keys)
      .filter((k) => k.recent);
    if (recent.length === 0) continue;
    out.set(nodeKey(n.group, n.kind, n.authorId), {
      streams: recent
        .filter((k) => k.key !== '')
        .map((k) => ({
          key: k.key,
          rows: k.rows,
          ...(k.condition ? { condition: k.condition } : {}),
        })),
      unnamed: recent.some((k) => k.key === ''),
    });
  }
  return out;
}

/** Every copy of a shared declaration among `nodes`, in any group: two shared nodes of one kind
 * and author id are one declaration. `nodeKey`s. Empty for a node `shared/` does not declare. */
export function sharedCopies(nodes: MapNode[], of: MapNode): Set<string> {
  if (!of.shared) return new Set();
  return new Set(
    nodes
      .filter((n) => n.shared && n.kind === of.kind && n.authorId === of.authorId)
      .map((n) => nodeKey(n.group, n.kind, n.authorId)),
  );
}

// -- layout -------------------------------------------------------------

/** A card's width when the group has room for it. */
export const NODE_W = 280;
/** The narrowest a card gets: its name cuts short with "…", but its rate and its note still fit
 * side by side. Below this a group scrolls sideways. */
export const MIN_NODE_W = 180;
/** Room for a card's three lines and a destination's buffer bar. */
export const NODE_H = 86;
/** Room for the lanes lines turn on between two columns (`edgePaths`). */
export const COL_GAP = 72;
export const ROW_H = 100;
export const PAD = 18;
/** The head of a column, above the first row: a kind is a fact about the column, so a band names
 * it once there rather than on every card in it. */
export const HEAD_H = 24;
/** Between a column's head and its first card. */
export const HEAD_GAP = 14;
/** Room right of the last column for a line that leaves a destination for another one: both ends
 * sit in the same column, so it bends out past their right edges and back. */
export const LOOP_W = 40;

const COLUMNS: MapNodeKind[] = ['source', 'pipeline', 'destination'];

export type Positioned = { node: MapCard; x: number; y: number };

/** A column's heading, drawn whether or not the column holds anything: the three are fixed, so a
 * heading that came and went would take the reading of a node's place with it. `count` is the
 * objects it draws, a stack's members among them. */
export type Column = { kind: MapNodeKind; x: number; count: number };

export type Layout = {
  positions: Positioned[];
  columns: Column[];
  /** Every card's width; `edgePaths` needs it to find a card's right edge. */
  nodeW: number;
  width: number;
  height: number;
};

/** The card width that fits three columns, their gaps and the loop room into `room` pixels. */
export function cardWidth(room: number): number {
  const fit = Math.floor(
    (room - PAD * 2 - (COLUMNS.length - 1) * COL_GAP - LOOP_W) / COLUMNS.length,
  );
  return Math.max(MIN_NODE_W, Math.min(NODE_W, fit));
}

/** Each kind in its own column, ordered by name so the same nodes always draw the same
 * picture. Rows pack from the top of each column, so handing this a smaller node list --
 * `survivors` below, filtered by a search -- draws a smaller graph, not the same graph with
 * gaps in it (design doc: "the rest closes up"). */
export function layout(nodes: MapCard[], nodeW = NODE_W): Layout {
  const byKind: Record<MapNodeKind, MapCard[]> = { source: [], pipeline: [], destination: [] };
  for (const n of nodes) byKind[n.kind].push(n);
  for (const k of COLUMNS) {
    byKind[k].sort(
      (a, b) =>
        a.name.localeCompare(b.name) ||
        a.authorId.localeCompare(b.authorId) ||
        a.group.localeCompare(b.group),
    );
  }

  const positions: Positioned[] = [];
  const columns: Column[] = [];
  COLUMNS.forEach((kind, col) => {
    const x = PAD + col * (nodeW + COL_GAP);
    columns.push({ kind, x, count: objectCount(byKind[kind]) });
    byKind[kind].forEach((node, row) => {
      positions.push({ node, x, y: PAD + HEAD_H + HEAD_GAP + row * ROW_H });
    });
  });

  const rows = Math.max(1, ...COLUMNS.map((k) => byKind[k].length));
  return {
    positions,
    columns,
    nodeW,
    width: PAD * 2 + COLUMNS.length * nodeW + (COLUMNS.length - 1) * COL_GAP + LOOP_W,
    height: PAD * 2 + HEAD_H + HEAD_GAP + (rows - 1) * ROW_H + NODE_H,
  };
}

/** Lines run square: out of a card, along a lane in a gap, into the next card. A side gives each
 * line ending on it a port and a gap gives each line turning in it a lane, squeezed closer when
 * many share one. */
export const LANE = 8;
export const PORT = 12;
/** A line ends on a straight run at least this long: the arrowhead sits on it, so a line never
 * turns under its own head. */
export const ARROW_RUN = 14;
const BEND = 6;
/** How far a card's ports keep from its top and bottom corners. */
const PORT_INSET = 18;

type Side = 'left' | 'right';
type End = { line: number; start: boolean; node: Positioned; side: Side; other: Positioned };
/** One vertical run of a line, from `ya` to `yb`, in the gap `gap` names. */
type Turn = { line: number; at: number; ya: number; yb: number };

const centerY = (p: Positioned) => p.y + NODE_H / 2;
const posKey = (p: Positioned) => nodeKey(p.node.group, p.node.kind, p.node.authorId);

/** `n` values about `mid`, `step` apart, all moved `shift` steps along. */
function spread(n: number, mid: number, step: number, shift = 0): number[] {
  return Array.from({ length: n }, (_, i) => mid + (i - (n - 1) / 2 + shift) * step);
}

/** The path for each line, in the order given. Every line runs left to right from `from`'s right
 * edge to `to`'s left edge but a destination's refusals, which go to another destination in the
 * same column: that one leaves and re-enters on the right, turning in `LOOP_W`. A line skipping
 * the pipeline column crosses it between two rows of cards. Lines are routed together, since each
 * one's ports and lanes depend on the others drawn beside it. */
export function edgePaths(lines: { from: Positioned; to: Positioned }[], nodeW = NODE_W): string[] {
  const loop = (l: number) => lines[l].to.x <= lines[l].from.x;
  const skip = (l: number) => lines[l].to.x - lines[l].from.x > nodeW + COL_GAP;

  // Ports: a side's ends ordered by where their other end sits, so lines leave and arrive in the
  // order they run and do not cross at the card.
  const sides = new Map<string, End[]>();
  lines.forEach(({ from, to }, line) => {
    const ends: End[] = [
      { line, start: true, node: from, side: 'right', other: to },
      { line, start: false, node: to, side: loop(line) ? 'right' : 'left', other: from },
    ];
    for (const e of ends) {
      const k = `${posKey(e.node)} ${e.side}`;
      sides.set(k, [...(sides.get(k) ?? []), e]);
    }
  });
  // One step for every side, so the whole and half steps below hold across cards.
  const most = Math.max(1, ...[...sides.values()].map((ends) => ends.length));
  const step = Math.min(PORT, (NODE_H - 2 * PORT_INSET) / most);
  const y1: number[] = [];
  const y2: number[] = [];
  for (const ends of sides.values()) {
    ends.sort(
      (a, b) => centerY(a.other) - centerY(b.other) || a.other.x - b.other.x || a.line - b.line,
    );
    // Lines leave on whole steps and arrive on half steps, so a line leaving a card does not run
    // along one arriving at the card beside it.
    const shift = (ends.length % 2 === 0) === ends[0].start ? 0.5 : 0;
    const ys = spread(ends.length, centerY(ends[0].node), step, shift);
    ends.forEach((e, i) => ((e.start ? y1 : y2)[e.line] = ys[i]));
  }
  // A line alone on a side slides to meet its other end, so it runs straight rather than jog;
  // alone on both sides of one row, it runs through the middle.
  const lone = new Map<string, End>();
  for (const ends of sides.values())
    if (ends.length === 1 && !skip(ends[0].line))
      lone.set(`${ends[0].line} ${ends[0].start}`, ends[0]);
  for (const e of lone.values()) {
    const [mine, theirs] = e.start ? [y1, y2] : [y2, y1];
    if (lone.has(`${e.line} ${!e.start}`) && e.node.y === e.other.y) {
      mine[e.line] = theirs[e.line] = centerY(e.node);
      continue;
    }
    const y = theirs[e.line];
    if (y >= e.node.y + PORT_INSET && y <= e.node.y + NODE_H - PORT_INSET) mine[e.line] = y;
  }

  // Corridors: a line skipping a column crosses it in the space between two rows, below its own
  // card or above it toward `to`. No port is there, so it runs along no other line.
  const corridor: number[] = [];
  const corridors = new Map<number, number[]>();
  const between = (ROW_H - NODE_H) / 2;
  lines.forEach(({ from, to }, line) => {
    if (!skip(line)) return;
    const y = centerY(to) < centerY(from) ? from.y - between : from.y + NODE_H + between;
    corridors.set(y, [...(corridors.get(y) ?? []), line]);
  });
  for (const [y, members] of corridors) {
    members.sort((a, b) => y1[a] - y1[b] || y2[a] - y2[b] || a - b);
    const ys = spread(members.length, y, Math.min(LANE, (ROW_H - NODE_H - 4) / members.length));
    members.forEach((l, i) => (corridor[l] = ys[i]));
  }

  // Lanes: a gap's vertical runs, each on its own lane. A run going down takes a lane further
  // right the higher it starts, one going up the lower it starts: two lines that need not cross
  // then don't. A gap is named by the x of its left edge.
  const gaps = new Map<number, Turn[]>();
  const turn = (gap: number, t: Turn) => gaps.set(gap, [...(gaps.get(gap) ?? []), t]);
  lines.forEach(({ from }, line) => {
    const x = from.x + nodeW;
    if (skip(line)) {
      turn(x, { line, at: 0, ya: y1[line], yb: corridor[line] });
      turn(x + COL_GAP + nodeW, { line, at: 1, ya: corridor[line], yb: y2[line] });
    } else if (y1[line] !== y2[line]) turn(x, { line, at: 0, ya: y1[line], yb: y2[line] });
  });
  const lane = new Map<string, number>();
  for (const [gap, turns] of gaps) {
    const down = turns.filter((t) => t.yb > t.ya).sort((a, b) => b.ya - a.ya || a.line - b.line);
    const up = turns.filter((t) => t.yb < t.ya).sort((a, b) => a.ya - b.ya || a.line - b.line);
    const [lo, hi] = loop(turns[0].line)
      ? [gap + ARROW_RUN + BEND, gap + LOOP_W - 2]
      : [gap + BEND + 2, gap + COL_GAP - ARROW_RUN - BEND];
    const xs = spread(turns.length, (lo + hi) / 2, Math.min(LANE, (hi - lo) / turns.length));
    [...down, ...up].forEach((t, i) => lane.set(`${t.line} ${t.at}`, xs[i]));
  }

  return lines.map(({ from, to }, line) => {
    const x1 = from.x + nodeW;
    const x2 = loop(line) ? to.x + nodeW : to.x;
    const a = lane.get(`${line} 0`);
    const b = lane.get(`${line} 1`);
    if (a === undefined)
      return elbow([
        [x1, y1[line]],
        [x2, y2[line]],
      ]);
    if (b === undefined)
      return elbow([
        [x1, y1[line]],
        [a, y1[line]],
        [a, y2[line]],
        [x2, y2[line]],
      ]);
    return elbow([
      [x1, y1[line]],
      [a, y1[line]],
      [a, corridor[line]],
      [b, corridor[line]],
      [b, y2[line]],
      [x2, y2[line]],
    ]);
  });
}

/** A polyline through `points` with each corner rounded off. */
export function elbow(points: [number, number][]): string {
  const len = (a: [number, number], b: [number, number]) => Math.hypot(b[0] - a[0], b[1] - a[1]);
  const toward = (a: [number, number], b: [number, number], d: number): [number, number] => {
    const l = len(a, b);
    return [a[0] + ((b[0] - a[0]) * d) / l, a[1] + ((b[1] - a[1]) * d) / l];
  };
  const f = (p: [number, number]) => `${round(p[0])} ${round(p[1])}`;
  let d = `M${f(points[0])}`;
  for (let i = 1; i < points.length - 1; i++) {
    const [a, c, b] = [points[i - 1], points[i], points[i + 1]];
    const r = Math.min(BEND, len(a, c) / 2, len(c, b) / 2);
    // Control points 0.45 of the radius from the corner make the cubic follow a quarter circle.
    d += ` L${f(toward(c, a, r))} C${f(toward(c, a, r * 0.45))} ${f(toward(c, b, r * 0.45))} ${f(toward(c, b, r))}`;
  }
  return `${d} L${f(points[points.length - 1])}`;
}

const round = (v: number) => Math.round(v * 10) / 10;

// -- lines (design doc "The map") -------------------------------------------
//
// A line is a connection and nothing else: what flows is read on the cards. Route and delivery
// lines always draw; a fallback line -- a row nothing claimed, a batch nothing could process, an
// exception answer -- draws only on a selected path that can take it, since every source can fall
// through and a fan of them converging on one DLQ says nothing.

export type EdgeKind = 'route' | 'deliver' | 'unmatched' | 'poison' | 'exception';

/** One drawn line between two nodes, `from`/`to` as `nodeKey`s. Several routes (or delivery
 * rules) between one pair are one line, and `routes` names each of them by `routeKey`; a line no
 * route owns -- a source's fallback, a destination's refusal -- has none. A line never crosses a
 * group: both ends carry the same `group`. */
export type MapEdge = { kind: EdgeKind; from: string; to: string; routes: string[] };

export function edgeKey(e: Pick<MapEdge, 'kind' | 'from' | 'to'>): string {
  return `${e.kind} ${e.from} ${e.to}`;
}

/** A route's identity: its id is unique only within its group. */
export function routeKey(group: string, route: string): string {
  return JSON.stringify([group, route]);
}

export function isFallback(kind: EdgeKind): boolean {
  return kind !== 'route' && kind !== 'deliver';
}

export type MapEdges = {
  routes: RouteEdge[];
  delivers: DeliverEdge[];
  unmatched: UnmatchedEdge[];
  poison: PoisonEdge[];
  exceptions: ExceptionEdge[];
};

/** Every line the map can draw, one per kind and pair. Kept apart by kind, so a deployment naming
 * one destination for two fallbacks still draws two lines. */
export function mapEdges(m: MapEdges): MapEdge[] {
  const out = new Map<string, MapEdge>();
  const add = (kind: EdgeKind, from: string, to: string, group: string, route?: string) => {
    const key = edgeKey({ kind, from, to });
    const e = out.get(key) ?? { kind, from, to, routes: [] };
    if (route !== undefined && !e.routes.includes(routeKey(group, route)))
      e.routes.push(routeKey(group, route));
    out.set(key, e);
  };
  for (const r of m.routes)
    add(
      'route',
      nodeKey(r.group, 'source', r.source),
      nodeKey(r.group, 'pipeline', r.pipeline),
      r.group,
      r.route,
    );
  for (const d of m.delivers)
    add(
      'deliver',
      nodeKey(d.group, 'pipeline', d.pipeline),
      nodeKey(d.group, 'destination', d.destination),
      d.group,
      d.route,
    );
  for (const u of m.unmatched)
    add(
      'unmatched',
      nodeKey(u.group, 'source', u.source),
      nodeKey(u.group, 'destination', u.destination),
      u.group,
    );
  for (const p of m.poison)
    add(
      'poison',
      nodeKey(p.group, 'source', p.source),
      nodeKey(p.group, 'destination', p.destination),
      p.group,
    );
  for (const x of m.exceptions)
    add(
      'exception',
      nodeKey(x.group, x.fromKind, x.from),
      nodeKey(x.group, 'destination', x.destination),
      x.group,
      x.route,
    );
  return [...out.values()];
}

/** Full strength only between two matches; a line touching a dimmed neighbor is dimmed too,
 * so it reads as leading out to context rather than as another result. */
export function edgeDimmed(matched: Set<string>, edge: { from: string; to: string }): boolean {
  return !(matched.has(edge.from) && matched.has(edge.to));
}

// -- the selected path -------------------------------------------------------

/** What a selection lights: `nodes` and `edges` (`edgeKey`s) on its path. */
export type SelectedPath = { nodes: Set<string>; edges: Set<string> };

/** The routes that run through `node`: the routes out of a source, through a pipeline, or
 * delivering to a destination. */
export function routesThrough(edges: MapEdge[], node: string): Set<string> {
  const out = new Set<string>();
  for (const e of edges)
    if (!isFallback(e.kind) && (e.from === node || e.to === node))
      for (const r of e.routes) out.add(r);
  return out;
}

/** The route and delivery lines of `routes` that `keep` takes, and every node they touch. */
function routeLines(
  edges: MapEdge[],
  routes: Set<string>,
  into: SelectedPath,
  keep: (e: MapEdge) => boolean,
) {
  for (const e of edges) {
    if (isFallback(e.kind) || !e.routes.some((r) => routes.has(r)) || !keep(e)) continue;
    into.edges.add(edgeKey(e));
    into.nodes.add(e.from);
    into.nodes.add(e.to);
  }
}

/** The path through `selected` (a `nodeKey` of kind `kind`): the routes that run through it, end
 * to end from a source or a pipeline and up to it from a destination -- so a pipeline two routes
 * share joins a source only to its own route's destinations.
 * A fallback line joins from the downstream side, for what the selection can send there: a
 * source's fallbacks, a pipeline's exception answers on these routes, a reached destination's
 * refusals. A destination a fallback reaches has for its path whatever falls into it, back to the
 * routes it falls from. */
export function selectedPath(edges: MapEdge[], selected: string, kind: MapNodeKind): SelectedPath {
  const routes = routesThrough(edges, selected);
  const path: SelectedPath = { nodes: new Set([selected]), edges: new Set() };
  // A destination's path is what reaches it: its routes' other deliveries go somewhere else.
  routeLines(edges, routes, path, (e) =>
    kind === 'destination' ? e.kind !== 'deliver' || e.to === selected : true,
  );

  // Downstream of the selection: the source sends along the whole path, a pipeline to its
  // destinations, a destination only itself.
  const down = new Set([selected]);
  if (kind === 'source') for (const n of path.nodes) down.add(n);
  if (kind === 'pipeline') for (const n of path.nodes) if (destinationKey(n)) down.add(n);

  const take = (e: MapEdge) => {
    path.edges.add(edgeKey(e));
    path.nodes.add(e.from);
    path.nodes.add(e.to);
  };
  const fallbacks = edges.filter((e) => isFallback(e.kind));

  // What the selection sends down a fallback, and on down the chain: a destination a fallback
  // reaches refuses on in turn.
  const sending = [...down];
  while (sending.length > 0) {
    const at = sending.pop()!;
    for (const e of fallbacks) {
      if (e.from !== at || !(e.routes.length === 0 || e.routes.some((r) => routes.has(r))))
        continue;
      take(e);
      if (!down.has(e.to)) {
        down.add(e.to);
        sending.push(e.to);
      }
    }
  }

  // What falls into the selection, back up the chain to the routes behind a pipeline's answer or
  // a destination's refusal, without the rest of where those routes go.
  const reached = new Set([selected]);
  const falling = [selected];
  while (falling.length > 0) {
    const at = falling.pop()!;
    for (const e of fallbacks) {
      if (e.to !== at) continue;
      take(e);
      if (sourceKey(e.from)) continue;
      const behind = e.routes.length > 0 ? new Set(e.routes) : routesThrough(edges, e.from);
      for (const l of edges) {
        if (isFallback(l.kind) || !l.routes.some((r) => behind.has(r))) continue;
        if (l.kind === 'route' || l.to === e.from) take(l);
      }
      if (destinationKey(e.from) && !reached.has(e.from)) {
        reached.add(e.from);
        falling.push(e.from);
      }
    }
  }
  return path;
}

function kindOfKey(key: string): MapNodeKind {
  return (JSON.parse(key) as string[])[1] as MapNodeKind;
}

function sourceKey(key: string): boolean {
  return kindOfKey(key) === 'source';
}

function destinationKey(key: string): boolean {
  return kindOfKey(key) === 'destination';
}

// -- stacks (design doc "The map": Stacks) ----------------------------------
//
// Sources or destinations of one system that connect to the same pipelines are one card, so a
// fleet of hundreds draws a card per kind of thing rather than per thing. A member the reader
// needs to see -- warned, selected, matched -- stands outside its stack, under the same rules that
// open a folded group.

/** The fewest members a stack has: two cards read fine side by side. */
export const STACK_MIN = 3;

/** One card on the map: an object, or a stack of them (`members`). A stack's `authorId` names
 * its system and the pipelines its members connect to, and its `name` is its card's title. */
export type MapCard = MapNode & { members?: MapNode[] };

/** A card's key: an object's `nodeKey`, or a stack's own, which no object's can equal. Both keep
 * the group first and the kind second, so `groupOfKey` reads either. */
export function cardKey(c: MapCard): string {
  return c.members
    ? JSON.stringify([c.group, c.kind, c.authorId, 'stack'])
    : nodeKey(c.group, c.kind, c.authorId);
}

/** How many objects `cards` draw: a stack counts its members. */
export function objectCount(cards: MapCard[]): number {
  return cards.reduce((n, c) => n + (c.members?.length ?? 1), 0);
}

export type Stacked = {
  cards: MapCard[];
  /** `edges` with each member's end moved to its stack, one line per kind and pair. */
  edges: MapEdge[];
};

/** Stacks `nodes`: a source or destination joins the others of its group, kind and system that
 * connect to exactly the same pipelines, unless `apart` takes its `nodeKey`. A pipeline, a shared
 * object, and an object on no route or delivery stay cards of their own. `title` names a stack of
 * `count` members of `kind`. */
export function stackNodes(
  nodes: MapNode[],
  edges: MapEdge[],
  apart: (key: string) => boolean,
  title: (count: number, kind: MapNodeKind) => string,
): Stacked {
  const pipelines = new Map<string, Set<string>>();
  for (const e of edges) {
    if (isFallback(e.kind)) continue;
    const [end, pipeline] = e.kind === 'route' ? [e.from, e.to] : [e.to, e.from];
    if (!pipelines.has(end)) pipelines.set(end, new Set());
    pipelines.get(end)!.add((JSON.parse(pipeline) as string[])[2]);
  }
  const buckets = new Map<string, { id: string; members: MapNode[] }>();
  for (const n of nodes) {
    const key = nodeKey(n.group, n.kind, n.authorId);
    const through = pipelines.get(key);
    if (n.kind === 'pipeline' || n.shared || !through || apart(key)) continue;
    const id = JSON.stringify([n.system ?? '', ...[...through].sort()]);
    const bucket = JSON.stringify([n.group, n.kind, id]);
    if (!buckets.has(bucket)) buckets.set(bucket, { id, members: [] });
    buckets.get(bucket)!.members.push(n);
  }
  const into = new Map<string, string>();
  const stacks: MapCard[] = [];
  for (const { id, members } of buckets.values()) {
    if (members.length < STACK_MIN) continue;
    members.sort((a, b) => a.name.localeCompare(b.name) || a.authorId.localeCompare(b.authorId));
    const { group, kind, system } = members[0];
    const card: MapCard = {
      group,
      kind,
      system,
      authorId: id,
      name: title(members.length, kind),
      shared: false,
      warning: false,
      members,
    };
    for (const m of members) into.set(nodeKey(m.group, m.kind, m.authorId), cardKey(card));
    stacks.push(card);
  }
  const cards: MapCard[] = nodes.filter((n) => !into.has(nodeKey(n.group, n.kind, n.authorId)));
  const lines = new Map<string, MapEdge>();
  for (const e of edges) {
    const moved = { ...e, from: into.get(e.from) ?? e.from, to: into.get(e.to) ?? e.to };
    const had = lines.get(edgeKey(moved));
    if (had) had.routes = [...new Set([...had.routes, ...moved.routes])];
    else lines.set(edgeKey(moved), moved);
  }
  return { cards: cards.concat(stacks), edges: [...lines.values()] };
}

/** What a stack reads: its members' rates summed, and the worst of everything else -- the most
 * lag, the fullest buffer. Absent where no member reports it. A member with a warning stands
 * apart, so a stack carries none. */
export function stackReading(
  stack: MapCard,
  readingOf: (n: MapNode) => NodeReadingDto | undefined,
): NodeReadingDto | undefined {
  const rs = (stack.members ?? []).map(readingOf).filter((r) => r !== undefined);
  if (rs.length === 0) return undefined;
  const most = (xs: (number | undefined)[]) => {
    const ns = xs.filter((x) => x !== undefined);
    return ns.length > 0 ? Math.max(...ns) : undefined;
  };
  const rates = rs.map((r) => r.rate).filter((r) => r !== undefined);
  const busiest = rs.map((r) => r.queue).filter((q) => q !== undefined);
  return {
    group: stack.group,
    kind: stack.kind,
    id: stack.authorId,
    rate: rates.length > 0 ? rates.reduce((a, b) => a + b, 0) : undefined,
    lastSeenMillis: most(rs.map((r) => r.lastSeenMillis)),
    lag: most(rs.map((r) => r.lag)),
    unmatched: rs.some((r) => r.unmatched !== undefined)
      ? rs.reduce((a, r) => a + (r.unmatched ?? 0), 0)
      : undefined,
    queue:
      busiest.length > 0
        ? busiest.reduce((a, q) => ((q.busiest ?? -1) > (a.busiest ?? -1) ? q : a))
        : undefined,
    warnings: [],
  };
}

// -- search -----------------------------------------------------------------
//
// A query is space-separated terms, ANDed: every term has to match a node for it to survive.
// A bare term matches a name or id; `field:value` narrows to one of the fields below. A value in
// double quotes holds spaces, with `\"` and `\\` for a quote and a backslash. A field
// the design names that nothing here can answer -- object tags, a worker group, a source's
// topics -- says so, rather than quietly matching nothing (design doc: "A query for a field
// nothing can match must say that").

export type SearchToken = { field: string | null; value: string };

export function parseQuery(query: string): SearchToken[] {
  const tokens: SearchToken[] = [];
  for (const m of query.matchAll(/(?:([^\s:"]+):)?(?:"((?:[^"\\]|\\.)*)"?|(\S*))/g)) {
    if (m[0].length === 0) continue;
    const field = m[1]?.toLowerCase() ?? null;
    const raw = m[2] !== undefined ? m[2].replace(/\\(.)/g, '$1') : m[3];
    // `id:` keeps its case: it is exact (`matchSupported`).
    tokens.push({ field, value: field === 'id' ? raw : raw.toLowerCase() });
  }
  return tokens;
}

const KNOWN_FIELDS = new Set(['calls', 'id', ...TERM_FIELDS]);

/** Why a field the design names cannot match here -- a fact about this codebase, not about
 * any one query, so the same sentence applies everywhere it is shown. */
const UNSUPPORTED_FIELDS: Record<string, string> = {
  tag: "object tags aren't built in this codebase yet",
  tags: "object tags aren't built in this codebase yet",
  group: 'search does not filter by group yet -- use the group filter above the map',
  groups: 'search does not filter by group yet -- use the group filter above the map',
  topic: "a source's topics aren't carried onto the map yet",
  topics: "a source's topics aren't carried onto the map yet",
};

export type UnsupportedField = { field: string; note: string };

/** Every callee a pipeline's `Call` steps name, mapped back to its direct callers
 * (`config_file::Links`, served by `/api/areas/:area/pipeline-callers`) -- a call is never drawn, so
 * `calls:` has nowhere else to read from. */
export type CallersIndex = Record<string, string[]>;

export type SearchResult = {
  /** False for an empty query: nothing is filtered and nothing is dimmed. */
  active: boolean;
  /** `nodeKey`s. */
  matched: Set<string>;
  /** Direct neighbors of a match, one edge away -- dimmed rather than hidden, since hiding
   * one would leave a pipeline with an input or an output from nowhere. `nodeKey`s. */
  dimmed: Set<string>;
  unsupported: UnsupportedField[];
};

function matchSupported(token: SearchToken, nodes: MapNode[], callers: CallersIndex): Set<string> {
  if (token.field === null) {
    return new Set(
      nodes
        .filter(
          (n) =>
            n.name.toLowerCase().includes(token.value) ||
            n.authorId.toLowerCase().includes(token.value),
        )
        .map((n) => nodeKey(n.group, n.kind, n.authorId)),
    );
  }
  // `id:` is exact, where a bare term is not: a link that picks out one object's nodes in every
  // group (`connections.ts`) must not pick out another whose id holds it.
  if (token.field === 'id') {
    return new Set(
      nodes
        .filter((n) => n.authorId === token.value)
        .map((n) => nodeKey(n.group, n.kind, n.authorId)),
    );
  }
  // The last member of KNOWN_FIELDS. `callers` comes from `/api/areas/:area/pipeline-callers`, which
  // answers for one editor scope rather than the whole fleet (`config_file::Links` reads a
  // single bundle) -- so a callee id here is not group-qualified, and a match is every pipeline
  // node of that author id in any group, not one group's alone.
  const calleeIds = new Set<string>();
  for (const [callee, direct] of Object.entries(callers)) {
    if (callee.toLowerCase().includes(token.value)) for (const c of direct) calleeIds.add(c);
  }
  return new Set(
    nodes
      .filter((n) => n.kind === 'pipeline' && calleeIds.has(n.authorId))
      .map((n) => nodeKey(n.group, n.kind, n.authorId)),
  );
}

function intersect(a: Set<string>, b: Set<string>): Set<string> {
  return new Set([...a].filter((x) => b.has(x)));
}

function neighborsOf(ids: Set<string>, edges: MapEdge[]): Set<string> {
  const out = new Set<string>();
  for (const e of edges) {
    if (ids.has(e.from)) out.add(e.to);
    if (ids.has(e.to)) out.add(e.from);
  }
  return out;
}

/** `edges` is every line, fallbacks included: a DLQ is a neighbor of the source that feeds it,
 * the same as a pipeline is, so a search matching one keeps the other. A term on a reading
 * (`pressure:held`, `warning:lagclimbing`) reads `readingOf`; with no readings every node reads as
 * having none. */
export function search(
  nodes: MapNode[],
  edges: MapEdge[],
  query: string,
  callers: CallersIndex,
  readingOf: (n: MapNode) => NodeReadingDto | undefined = () => undefined,
): SearchResult {
  const tokens = parseQuery(query);
  if (tokens.length === 0) {
    return {
      active: false,
      matched: new Set(nodes.map((n) => nodeKey(n.group, n.kind, n.authorId))),
      dimmed: new Set(),
      unsupported: [],
    };
  }

  const terms = termsFor(nodes, readingOf);
  const unsupported: UnsupportedField[] = [];
  let matched: Set<string> | null = null;
  for (const token of tokens) {
    const term =
      token.field === null
        ? token.value === 'shared'
          ? termFor('shared', terms)
          : null
        : token.field === 'calls' || token.field === 'id'
          ? null
          : termFor(`${token.field}:${token.value}`, terms);
    if (term) {
      const hit = new Set(
        nodes
          .filter((n) => term.test(n, readingOf(n)))
          .map((n) => nodeKey(n.group, n.kind, n.authorId)),
      );
      matched = matched === null ? hit : intersect(matched, hit);
      continue;
    }
    if (token.field !== null && TERM_FIELDS.has(token.field)) {
      // Half typed: no filter yet, and nothing to complain about.
      const typed = `${token.field}:${token.value}`;
      if (terms.some((t) => t.term.startsWith(typed))) continue;
      unsupported.push({
        field: `${token.field}:${token.value}`,
        note: 'no term by that name -- open the box for the ones there are',
      });
      continue;
    }
    if (token.field !== null && !KNOWN_FIELDS.has(token.field)) {
      unsupported.push({
        field: token.field,
        note: UNSUPPORTED_FIELDS[token.field] ?? `the map does not index \`${token.field}\` yet`,
      });
      continue;
    }
    const hit = matchSupported(token, nodes, callers);
    matched = matched === null ? hit : intersect(matched, hit);
  }

  if (matched === null && unsupported.length === 0) {
    // Every token is a term still being typed: the map stays as it is.
    return {
      active: false,
      matched: new Set(nodes.map((n) => nodeKey(n.group, n.kind, n.authorId))),
      dimmed: new Set(),
      unsupported,
    };
  }
  const matchedIds = matched ?? new Set<string>();
  const neighbors = neighborsOf(matchedIds, edges);
  const dimmed = new Set([...neighbors].filter((id) => !matchedIds.has(id)));
  return { active: true, matched: matchedIds, dimmed, unsupported };
}

/** Matches and their direct neighbors: what a filtered map still draws. `nodeKey`s. */
export function survivors(result: SearchResult): Set<string> {
  return new Set([...result.matched, ...result.dimmed]);
}

/** `ids` are `nodeKey`s. */
export function visibleNodes(nodes: MapNode[], ids: Set<string>): MapNode[] {
  return nodes.filter((n) => ids.has(nodeKey(n.group, n.kind, n.authorId)));
}

export function visibleEdges(edges: MapEdge[], ids: Set<string>): MapEdge[] {
  return edges.filter((e) => ids.has(e.from) && ids.has(e.to));
}

// -- when the fleet cannot be read at all -----------------------------------
//
// `MapResponse.errors` (as opposed to one group's own `groupErrors`) is a problem that kept every
// group from being read -- the repository, the branch, or `shared/` itself -- and names the
// object it is about the way every `config_file` load error already does (`src/leader/map.rs`),
// so it is linked to its editor by finding a known name or id inside the free text rather than asking the
// backend for a second, structured form of the same message.

export type ProblemLink = { label: string; hash: string };

export type KnownIds = {
  /** A pipeline is matched against the message by its name where the message quotes one, else
   * by its id; its name is what the link says. */
  pipelines: { id: string; name: string }[];
  sources: string[];
  destinations: string[];
  routes: string[];
};

/** `config_file::bundle::load_lenient` names an unreadable pipeline file `pipelines/<id>.json`
 * -- true even when the id is not in `KnownIds`, since a file that failed to parse never made
 * it into the loaded pipeline list. */
const PIPELINE_FILE = /pipelines\/([^/]+)\.json/;

/** `name` is absent for a pipeline the bundle could not load, which has only its id to go by. */
function pipelineLink(id: string, name?: string): ProblemLink {
  return {
    label: `pipeline ${name || id}`,
    hash: `#/build/pipelines/${encodeURIComponent(id)}`,
  };
}
function sourceLink(id: string): ProblemLink {
  return { label: `source ${id}`, hash: `#/build/sources/${encodeURIComponent(id)}` };
}
function destinationLink(id: string): ProblemLink {
  return { label: `destination ${id}`, hash: `#/build/destinations/${encodeURIComponent(id)}` };
}
// A route has no editor of its own -- `RoutesPage` edits the whole list at once -- so every
// route id links to the same place, the editor that fixes it.
function routeLink(id: string): ProblemLink {
  return { label: `route ${id}`, hash: '#/build/routes' };
}

/** How `config_file::load` names a pipeline whose steps fail to compile: `pipeline "<name>"`,
 * the name in Rust's `{:?}` form, which escapes a quote and a backslash the way JSON does. */
function pipelineByName(name: string): string {
  return `pipeline ${JSON.stringify(name)}`;
}

/** The object one error names, if any: the file it could not even parse, else the pipeline it
 * names by name, else the longest known id its text contains, so a short id (`"orders"`) cannot
 * steal a match that belongs to a longer one that contains it (`"orders-detail"`). A name is
 * tried before any id because an id may occur inside a name. A name two pipelines share ties
 * the message to neither, so it links to none. `null` when nothing in the message can be tied
 * to one object -- shown as plain text, honestly, rather than a guess dressed as a link. */
export function linkForError(message: string, known: KnownIds): ProblemLink | null {
  const badFile = PIPELINE_FILE.exec(message);
  if (badFile) {
    const id = decodeURIComponent(badFile[1]);
    return pipelineLink(id, known.pipelines.find((p) => p.id === id)?.name);
  }

  const named = known.pipelines
    .filter((p) => p.name.length > 0 && message.includes(pipelineByName(p.name)))
    .sort((a, b) => b.name.length - a.name.length);
  if (named.length > 0) {
    const sameName = named.filter((p) => p.name === named[0].name);
    return sameName.length === 1 ? pipelineLink(sameName[0].id, sameName[0].name) : null;
  }

  const candidates = [
    ...known.pipelines.map((p) => ({ id: p.id, link: (id: string) => pipelineLink(id, p.name) })),
    ...known.sources.map((id) => ({ id, link: sourceLink })),
    ...known.destinations.map((id) => ({ id, link: destinationLink })),
    ...known.routes.map((id) => ({ id, link: routeLink })),
  ];
  const hit = candidates
    .filter((c) => c.id.length > 0 && message.includes(c.id))
    .sort((a, b) => b.id.length - a.id.length)[0];
  return hit ? hit.link(hit.id) : null;
}
