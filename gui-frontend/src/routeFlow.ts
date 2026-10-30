// The flow card's model (`RouteFlow.svelte`): every route from one source, drawn as one graph. Each route is a
// branch: the source, the route's condition, its pipeline, then its delivery rules out to destination pills.
//
// A rule with no `when` is drawn as plain lines from the pipeline to its destinations; a rule with one is a
// condition box on the way. A route's plain lines are the union of its plain rules, so the card edits the first
// and cuts from all of them.
//
// The card holds one thing the bundle cannot: a destination brought onto it that no rule sends to yet. It
// lives on the card only, so the card can warn about it, and a reload forgets it.

import type { DeliverRuleDto, FileConfigDto, PipelineDto, RouteDto } from './api';
import { elbow } from './map';

export type Point = { x: number; y: number };

/** The routes of one card: every route that reads `source`, in the order the page holds them. */
export type Card = { source: string; routes: RouteDto[] };

/** The page's routes as cards, one per source, in the order each source first appears. */
export function cards(routes: RouteDto[]): Card[] {
  const out: Card[] = [];
  for (const r of routes) {
    const card = out.find((c) => c.source === r.source);
    if (card) card.routes.push(r);
    else out.push({ source: r.source, routes: [r] });
  }
  return out;
}

type Routed = Pick<RouteDto, 'source' | 'pipeline'>;

/** `routes[at]` as every screen and message names it: its source, then its pipeline, and `(n)` when another route
 * runs from that source to that pipeline too, `n` counting those routes in the order they run. A route's id is
 * minted, so it is never the name. The server names routes the same way (`route_name` in `src/config_file/mod.rs`). */
export function routeName(
  routes: readonly Routed[],
  at: number,
  pipelineName: (id: string) => string,
): string {
  const r = routes[at];
  const twin = (o: Routed) => o.source === r.source && o.pipeline === r.pipeline;
  const name = `${r.source} \u2192 ${pipelineName(r.pipeline)}`;
  const twins = routes.filter(twin).length;
  return twins === 1 ? name : `${name} (${routes.slice(0, at).filter(twin).length + 1})`;
}

/** Each of `routes`, all that run, in the order they run, with its `routeName`. */
export function namedRoutes<R extends Routed>(
  routes: readonly R[],
  pipelineName: (id: string) => string,
): (R & { name: string })[] {
  return routes.map((r, at) => ({ ...r, name: routeName(routes, at, pipelineName) }));
}

/** What a pipeline is called: its name, or its id where it has none. */
export function pipelineNamer(
  pipelines: readonly Pick<PipelineDto, 'id' | 'name'>[],
): (id: string) => string {
  const names = new Map(pipelines.map((p) => [p.id, p.name || p.id]));
  return (id) => names.get(id) ?? id;
}

/** The routes a bundle runs, `shared`'s first, as a group runs them, each with its `routeName`. */
export function bundleRoutes(
  config: Pick<FileConfigDto, 'routes' | 'pipelines'>,
  shared?: Pick<FileConfigDto, 'routes' | 'pipelines'> | null,
): (RouteDto & { name: string })[] {
  return namedRoutes(
    [...(shared?.routes ?? []), ...(config.routes ?? [])],
    pipelineNamer([...(shared?.pipelines ?? []), ...(config.pipelines ?? [])]),
  );
}

/** Route `id` of `config` by its `routeName`, counted after `shared`'s routes; undefined where `config` has no such
 * route. */
export function routeNameIn(
  config: Pick<FileConfigDto, 'routes' | 'pipelines'>,
  id: string,
  shared?: Pick<FileConfigDto, 'routes' | 'pipelines'> | null,
): string | undefined {
  if (!(config.routes ?? []).some((r) => r.id === id)) return undefined;
  return bundleRoutes(config, shared).find((r) => r.id === id)?.name;
}

/** Every destination on the card, once each: those a rule sends to, in first-use order, then the ones brought onto
 * the card and not yet wired. */
export function pills(routes: RouteDto[], extras: string[]): string[] {
  const sent = routes.flatMap((r) => (r.deliver ?? []).flatMap((rule) => rule.to));
  return [...new Set([...sent, ...extras])];
}

/** One pill: a destination at place `k` under the pipeline of route `at`, fed by that route's rule `via` (a box)
 * or by its plain lines (`'p'`); or, at -1, one brought onto the card that nothing sends to yet. */
export type Spot = { at: number; k: number; via: number | 'p'; dest: string };

/** The card's pills. Each line ends on a pill of its own, so the card only fans out: a destination two pipelines,
 * or two of one pipeline's boxes, send to has a pill under each. A route's pills follow its rules' order: rules are
 * independent, so that order says nothing about delivery and is free to be the card's, kept with the routes. A
 * box's destinations sit together. Last come the destinations nothing sends to. */
export function spots(routes: RouteDto[], extras: string[], boxed = gated): Spot[] {
  const out: Spot[] = routes.flatMap((r, at) =>
    flat(r, boxed).map(({ unit, dest }, k) => ({
      at,
      k,
      via: 'box' in unit ? (r.deliver ?? []).indexOf(unit.box) : ('p' as const),
      dest,
    })),
  );
  const sent = new Set(out.map((s) => s.dest));
  const idle = [...new Set(extras)].filter((d) => !sent.has(d));
  return [...out, ...idle.map((dest, k) => ({ at: -1, k, via: 'p' as const, dest }))];
}

/** Whether a rule has a condition of its own, so it is drawn as a box. A blank `when` takes every row. */
export function gated(rule: DeliverRuleDto): boolean {
  return (rule.when ?? '').trim() !== '';
}

/** The destinations a route sends its pipeline's whole output to: its plain rules', once each. `boxed` says which
 * rules are drawn as boxes instead, which a rule whose condition is still being written is. */
export function plain(route: RouteDto, boxed: (rule: DeliverRuleDto) => boolean = gated): string[] {
  return [...new Set((route.deliver ?? []).filter((r) => !boxed(r)).flatMap((r) => r.to))];
}

/** Whether the card is drawn as one line: one route, at most one destination, and nothing on the card that no
 * rule sends to. `boxes` counts the rules drawn as boxes, which a rule being written is before its `when` is. */
export function isLine(routes: RouteDto[], extras: string[], boxes: DeliverRuleDto[]): boolean {
  if (routes.length !== 1) return false;
  const boxed = (r: DeliverRuleDto) => gated(r) || boxes.includes(r);
  const lines = plain(routes[0], boxed).length + boxes.reduce((n, b) => n + b.to.length, 0);
  return (
    lines <= 1 &&
    boxes.length <= 1 &&
    boxes.every((b) => b.to.length > 0) &&
    extras.every((d) => !unsent(routes, d))
  );
}

/** Wire `rule` to `dest`, or cut it. `on` undefined flips it. A rule's `to` is a set, so wiring twice is
 * one edge. */
export function toggle(rule: DeliverRuleDto, dest: string, on?: boolean): void {
  const i = rule.to.indexOf(dest);
  const want = on ?? i < 0;
  if (want && i < 0) rule.to.push(dest);
  if (!want && i >= 0) rule.to.splice(i, 1);
}

/** The route's first plain rule, made when it has none: where a plain line is wired. */
function plainRule(route: RouteDto): DeliverRuleDto {
  const deliver = (route.deliver ??= []);
  const found = deliver.find((r) => !gated(r));
  if (found) return found;
  deliver.push({ to: [] });
  // Read back: in a reactive list the element is the list's own wrapper of what was pushed.
  return deliver[deliver.length - 1];
}

/** Wire the route's pipeline straight to `dest`, or cut that line. A cut takes it from every plain rule. */
export function setPlain(route: RouteDto, dest: string, on: boolean): void {
  if (on) toggle(plainRule(route), dest, true);
  else for (const r of route.deliver ?? []) if (!gated(r)) toggle(r, dest, false);
}

/** What a route's destinations are placed by, in its order: a box, whose destinations sit together, or one plain
 * line. */
type Unit = { box: DeliverRuleDto } | { dest: string };

function units(route: RouteDto, boxed: (rule: DeliverRuleDto) => boolean): Unit[] {
  const out: Unit[] = [];
  // A destination has one plain line however many plain rules name it; a box's lines to it are the box's own.
  const seen = new Set<string>();
  for (const rule of route.deliver ?? []) {
    if (boxed(rule)) out.push({ box: rule });
    else
      for (const d of rule.to) {
        if (seen.has(d)) continue;
        seen.add(d);
        out.push({ dest: d });
      }
  }
  return out;
}

/** Rewrites the route's rules in `list`'s order: each box as it is, each run of plain lines as one plain rule. A
 * destination in `plains` that a box placed first still gets its plain line, in a plain rule at the end. Returns
 * where each unit's rule landed. */
function rebuild(route: RouteDto, list: Unit[], plains: Set<string>): number[] {
  const next: DeliverRuleDto[] = [];
  const at: number[] = [];
  let run: DeliverRuleDto | null = null;
  for (const u of list) {
    if ('box' in u) {
      next.push(u.box);
      run = null;
    } else {
      if (!run) next.push((run = { to: [] }));
      run.to.push(u.dest);
    }
    at.push(next.length - 1);
  }
  const placed = new Set(list.flatMap((u) => ('dest' in u ? [u.dest] : [])));
  const rest = [...plains].filter((d) => !placed.has(d));
  if (rest.length) next.push({ to: rest });
  const deliver = (route.deliver ??= []);
  // A route that had a rule keeps one: an empty plain rule is how a route says it sends nowhere yet.
  if (next.length === 0 && deliver.length > 0) next.push({ to: [] });
  deliver.splice(0, deliver.length, ...next);
  return at;
}

/** Puts a condition on the line from the route's pipeline to `dest`: the line leaves the plain rules for a rule
 * of its own, in the line's place, whose `when` is still to be written. That rule, which the card draws as a box
 * until it is. */
export function gate(route: RouteDto, dest: string, boxed = gated): DeliverRuleDto {
  const list = units(route, boxed);
  const plains = new Set(plain(route, boxed));
  plains.delete(dest);
  let i = list.findIndex((u) => 'dest' in u && u.dest === dest);
  if (i < 0) i = list.push({ dest }) - 1;
  list[i] = { box: { to: [dest] } };
  // Read back: in a reactive list the element is the list's own wrapper of what was put there.
  return route.deliver![rebuild(route, list, plains)[i]];
}

/** Takes a box's condition off: its lines become plain lines in its place, and the rule goes. */
export function ungate(route: RouteDto, rule: DeliverRuleDto, boxed = gated): void {
  const list = units(route, boxed);
  const i = list.findIndex((u) => 'box' in u && u.box === rule);
  if (i < 0) return;
  const lines = new Set(list.flatMap((u) => ('dest' in u ? [u.dest] : [])));
  const plains = new Set(plain(route, boxed));
  list.splice(i, 1, ...rule.to.filter((d) => !lines.has(d)).map((d) => ({ dest: d })));
  rebuild(route, list, plains);
}

/** The route's pills in its rules' order, each with the unit that feeds it. */
function flat(route: RouteDto, boxed: (rule: DeliverRuleDto) => boolean) {
  const list = units(route, boxed);
  return list.flatMap((unit, ui) =>
    ('box' in unit ? unit.box.to : [unit.dest]).map((dest) => ({ unit, ui, dest })),
  );
}

/** Whether the route has a pill `by` places from its pill `k`: where a move can take it. */
export function canMove(route: RouteDto, k: number, by: number, boxed = gated): boolean {
  return flat(route, boxed)[k + by] !== undefined;
}

/** Moves the route's pill `k` one place left (`by` -1) or right, by rewriting the order of its rules. Inside a box
 * it trades places with the box's next destination; past the box's edge, the whole box moves. */
export function moveDest(route: RouteDto, k: number, by: number, boxed = gated): void {
  const pills = flat(route, boxed);
  const [from, to] = [pills[k], pills[k + by]];
  if (!from || !to) return;
  const list = units(route, boxed);
  const [a, b] = [from.ui, to.ui];
  const u = list[a];
  if (a === b && 'box' in u) {
    const [i, j] = [u.box.to.indexOf(from.dest), u.box.to.indexOf(to.dest)];
    [u.box.to[i], u.box.to[j]] = [u.box.to[j], u.box.to[i]];
    return;
  }
  const plains = new Set(plain(route, boxed));
  [list[a], list[b]] = [list[b], list[a]];
  rebuild(route, list, plains);
}

/** A plain rule that sends nowhere says nothing once the route has another rule. */
function dropEmptyPlain(route: RouteDto): void {
  const deliver = route.deliver ?? [];
  for (let i = deliver.length - 1; i >= 0; i--) {
    if (deliver.length > 1 && !gated(deliver[i]) && deliver[i].to.length === 0)
      deliver.splice(i, 1);
  }
}

/** Take `dest` off the card: every edge into it goes with it. */
export function removeDest(routes: RouteDto[], dest: string): void {
  for (const route of routes) for (const r of route.deliver ?? []) toggle(r, dest, false);
}

/** Configured destinations not on the card: what `+ destination` offers. Once each: a bundle read leniently can
 * declare an id twice, in one scope or across a group and `shared/`. */
export function offCard(configured: string[], onCard: string[]): string[] {
  return [...new Set(configured)].filter((d) => !onCard.includes(d));
}

/** Whether a pill has no edge into it, so the card warns under it. */
export function unsent(routes: RouteDto[], dest: string): boolean {
  return !routes.some((route) => (route.deliver ?? []).some((r) => r.to.includes(dest)));
}

/** A rule's name in a list: its condition, or what a blank one takes. */
export function ruleName(rule: DeliverRuleDto): string {
  return (rule.when ?? '').trim() || 'every row';
}

/** How far a vertical card's line runs upright out of and into its boxes. Short, so lines that meet at one socket
 * stay apart until close to it, and the controls on them do not land on each other. */
function bend(a: Point, b: Point): number {
  return Math.min(28, Math.max(16, (b.y - a.y) / 2));
}

/** The edge from `a` to `b`, leaving and entering along the card's flow: level on a line, upright on a vertical
 * card. A straight run between two centers is a straight line. */
export function curve(a: Point, b: Point, down = false): string {
  if (down) {
    const k = bend(a, b);
    return `M${a.x} ${a.y} C${a.x} ${a.y + k}, ${b.x} ${b.y - k}, ${b.x} ${b.y}`;
  }
  const k = Math.max(30, (b.x - a.x) / 2);
  return `M${a.x} ${a.y} C${a.x + k} ${a.y}, ${b.x - k} ${b.y}, ${b.x} ${b.y}`;
}

/** An edge's corners from `a` to `b`: it leaves and enters along the card's flow, level on a line and upright on a
 * vertical card, and crosses over at `mid`, the x (line) or height (vertical card) of its middle run. */
export function corners(a: Point, b: Point, down = false, mid?: number): Point[] {
  if (down) {
    const m = mid ?? (a.y + b.y) / 2;
    return tidy([a, { x: a.x, y: m }, { x: b.x, y: m }, b]);
  }
  const m = mid ?? (a.x + b.x) / 2;
  return tidy([a, { x: m, y: a.y }, { x: m, y: b.y }, b]);
}

/** `points` without repeats or the middle of three in a row, so every corner left turns. */
export function tidy(points: Point[]): Point[] {
  const out: Point[] = [];
  for (const p of points) {
    const prev = out[out.length - 1];
    if (prev && Math.abs(prev.x - p.x) < 0.5 && Math.abs(prev.y - p.y) < 0.5) continue;
    const before = out[out.length - 2];
    if (
      before &&
      ((before.x === prev.x && prev.x === p.x) || (before.y === prev.y && prev.y === p.y))
    )
      out.pop();
    out.push(p);
  }
  return out;
}

/** The SVG path through `points`, its corners rounded as the map's are. */
export function path(points: Point[]): string {
  return elbow(points.map((p) => [p.x, p.y]));
}

function length(points: Point[]): number {
  let n = 0;
  for (let i = 1; i < points.length; i++)
    n += Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y);
  return n;
}

/** The point `d` along `points` from its start, or from its end when `d` is negative; clamped to its ends. A
 * fraction of its length is `at(points, t * length)`, which `halfway` gives for the middle. */
export function at(points: Point[], d: number): Point {
  let left = d < 0 ? length(points) + d : d;
  for (let i = 1; i < points.length; i++) {
    const [a, b] = [points[i - 1], points[i]];
    const l = Math.hypot(b.x - a.x, b.y - a.y);
    if (left <= l && l > 0)
      return { x: a.x + ((b.x - a.x) * left) / l, y: a.y + ((b.y - a.y) * left) / l };
    left -= l;
  }
  return points[points.length - 1];
}

/** The point `t` of the way along `points`. */
export function halfway(points: Point[], t = 0.5): Point {
  return at(points, length(points) * t);
}

/** Where a condition box sits over its destinations: over the middle one, or between the middle two. */
export function centerOver(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Centers for boxes `width` wide that want to sit at `want`, pushed apart until `gap` separates each from the
 * next. Boxes that touch move as one, centered on what they want together, then the whole row is pulled inside
 * `lo`..`hi` where it fits. In `want`'s order. */
export function pushApart(
  want: number[],
  width: number,
  gap: number,
  lo: number,
  hi: number,
): number[] {
  const step = width + gap;
  const order = want.map((_, i) => i).sort((a, b) => want[a] - want[b] || a - b);
  // Each block is a run of boxes side by side: its members, in order, and its first box's center.
  const blocks: { members: number[]; x: number }[] = [];
  for (const i of order) {
    blocks.push({ members: [i], x: want[i] });
    for (;;) {
      const b = blocks[blocks.length - 1];
      const a = blocks[blocks.length - 2];
      if (!a || a.x + a.members.length * step <= b.x) break;
      const members = [...a.members, ...b.members];
      const mean = members.reduce((n, j, k) => n + want[j] - k * step, 0) / members.length;
      blocks.splice(-2, 2, { members, x: mean });
    }
  }
  const out: number[] = [];
  for (const b of blocks) b.members.forEach((j, k) => (out[j] = b.x + k * step));
  if (out.length === 0) return out;
  const first = Math.min(...out) - width / 2;
  const last = Math.max(...out) + width / 2;
  const shift = last - first > hi - lo ? lo - first : Math.max(lo - first, Math.min(0, hi - last));
  return out.map((x) => x + shift);
}
