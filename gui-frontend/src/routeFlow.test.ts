import { describe, expect, it } from 'vitest';
import type { DeliverRuleDto, RouteDto } from './api';
import {
  at,
  canMove,
  cards,
  centerOver,
  corners,
  gate,
  halfway,
  isLine,
  moveDest,
  offCard,
  ruleName,
  pills,
  routeName,
  routeNameIn,
  plain,
  pushApart,
  removeDest,
  setPlain,
  spots,
  toggle,
  ungate,
  unsent,
} from './routeFlow';

function route(deliver: DeliverRuleDto[], source = 's', id = 'r'): RouteDto {
  return { id, source, condition: null, pipeline: 'p', deliver };
}
const boxes = (r: RouteDto) => (r.deliver ?? []).filter((rule) => (rule.when ?? '').trim() !== '');

describe('the flow card model', () => {
  it('makes one card per source, in the order each source first appears', () => {
    const a = route([], 'x', 'a');
    const b = route([], 'y', 'b');
    const c = route([], 'x', 'c');
    expect(cards([a, b, c])).toEqual([
      { source: 'x', routes: [a, c] },
      { source: 'y', routes: [b] },
    ]);
  });

  it("lists every destination the card's rules send to once, then the ones not yet wired", () => {
    const r = route([{ to: ['a', 'b'] }, { when: 'x', to: ['b', 'c'] }]);
    const q = route([{ to: ['e'] }]);
    expect(pills([r, q], ['d', 'a'])).toEqual(['a', 'b', 'c', 'e', 'd']);
  });

  it('keeps a rule a set: wiring twice is one edge, and a cut takes it off', () => {
    const rule = { to: ['a'] };
    toggle(rule, 'a', true);
    toggle(rule, 'b');
    expect(rule.to).toEqual(['a', 'b']);
    toggle(rule, 'a');
    toggle(rule, 'c', false);
    expect(rule.to).toEqual(['b']);
  });

  it('takes every edge into a destination with it when it leaves the card', () => {
    const r = route([{ to: ['a', 'b'] }, { to: ['a'] }]);
    const q = route([{ to: ['a'] }]);
    removeDest([r, q], 'a');
    expect(r.deliver).toEqual([{ to: ['b'] }, { to: [] }]);
    expect(unsent([r, q], 'a')).toBe(true);
    expect(unsent([r, q], 'b')).toBe(false);
  });

  it("draws a route's plain rules as one set of lines, wired on the first and cut from all", () => {
    const r = route([{ to: ['a'] }, { when: 'x', to: ['b'] }, { to: ['a', 'c'] }]);
    expect(plain(r)).toEqual(['a', 'c']);
    setPlain(r, 'd', true);
    expect(r.deliver?.[0].to).toEqual(['a', 'd']);
    setPlain(r, 'a', false);
    expect(plain(r)).toEqual(['d', 'c']);
    const bare = route([{ when: 'x', to: [] }]);
    setPlain(bare, 'a', true);
    expect(bare.deliver).toEqual([{ when: 'x', to: [] }, { to: ['a'] }]);
  });

  it('puts a condition on a line as a rule of its own, and takes it off back to a plain line', () => {
    const r = route([{ to: ['a', 'b'] }]);
    const rule = gate(r, 'a');
    // In the line's place, so the card's order holds.
    expect(r.deliver).toEqual([{ to: ['a'] }, { to: ['b'] }]);
    rule.when = 'x';
    expect(plain(r)).toEqual(['b']);
    ungate(r, rule);
    expect(r.deliver).toEqual([{ to: ['a', 'b'] }]);
    // A plain rule left sending nowhere goes with the gate.
    const one = route([{ to: ['a'] }]);
    gate(one, 'a');
    expect(one.deliver).toEqual([{ to: ['a'] }]);
  });

  it('draws one route to at most one destination as a line, and anything past that top to bottom', () => {
    const line = (rs: RouteDto[], extras: string[] = []) => isLine(rs, extras, rs.flatMap(boxes));
    expect(line([route([])])).toBe(true);
    expect(line([route([{ to: [] }])])).toBe(true);
    expect(line([route([{ to: ['a'] }])])).toBe(true);
    // The conditions sit on the line.
    expect(line([{ ...route([{ when: 'x', to: ['a'] }]), condition: 'y' }])).toBe(true);
    // Brought on and wired is still one destination.
    expect(line([route([{ to: ['a'] }])], ['a'])).toBe(true);
    expect(line([route([{ to: ['a', 'b'] }])])).toBe(false);
    expect(line([route([{ to: ['a'] }, { when: 'x', to: ['a'] }])])).toBe(false);
    expect(line([route([{ to: ['a'] }]), route([{ to: ['a'] }], 's', 'q')])).toBe(false);
    // A destination no rule sends to is on the card only, so the card turns to show it.
    expect(line([route([{ to: [] }])], ['a'])).toBe(false);
    // So is a condition that sends nowhere.
    expect(line([route([{ when: 'x', to: [] }])])).toBe(false);
    // A condition being written is a box before its `when` is: its line is the box's, not a plain one too.
    const writing = route([{ to: ['a'] }]);
    expect(isLine([writing], [], writing.deliver!)).toBe(true);
  });

  it("ends each line on a pill of its own, in its rules' order, then those nothing sends to", () => {
    const a = route([{ to: ['x'] }, { when: 'w', to: ['y', 'x'] }]);
    const b = route([{ to: ['x'] }]);
    expect(spots([a, b], ['x', 'n'])).toEqual([
      { at: 0, k: 0, via: 'p', dest: 'x' },
      { at: 0, k: 1, via: 1, dest: 'y' },
      { at: 0, k: 2, via: 1, dest: 'x' },
      { at: 1, k: 0, via: 'p', dest: 'x' },
      { at: -1, k: 0, via: 'p', dest: 'n' },
    ]);
  });

  it("moves a pill among its pipeline's by rewriting the order of its rules", () => {
    const r = route([{ to: ['a', 'b'] }, { when: 'w', to: ['c', 'd'] }]);
    expect(canMove(r, 0, -1)).toBe(false);
    moveDest(r, 1, 1);
    // Past a box's edge the whole box moves: its destinations sit together.
    expect(r.deliver).toEqual([{ to: ['a'] }, { when: 'w', to: ['c', 'd'] }, { to: ['b'] }]);
    moveDest(r, 1, 1);
    expect(r.deliver![1].to).toEqual(['d', 'c']);
    moveDest(r, 0, 1);
    expect(r.deliver).toEqual([{ when: 'w', to: ['d', 'c'] }, { to: ['a', 'b'] }]);
  });

  it('offers only configured destinations not already on the card', () => {
    expect(offCard(['a', 'b', 'c'], ['b'])).toEqual(['a', 'c']);
    expect(offCard(['a', 'b', 'a'], [])).toEqual(['a', 'b']);
  });

  it('names a blank rule by what it takes', () => {
    expect(ruleName({ to: [] })).toBe('every row');
    expect(ruleName({ when: '  ', to: [] })).toBe('every row');
    expect(ruleName({ when: 'a == 1', to: [] })).toBe('a == 1');
  });

  it('turns an edge in elbows, and draws one between two centers on one line as a straight line', () => {
    expect(corners({ x: 0, y: 20 }, { x: 100, y: 20 })).toEqual([
      { x: 0, y: 20 },
      { x: 100, y: 20 },
    ]);
    expect(corners({ x: 20, y: 0 }, { x: 60, y: 100 }, true)).toEqual([
      { x: 20, y: 0 },
      { x: 20, y: 50 },
      { x: 60, y: 50 },
      { x: 60, y: 100 },
    ]);
    expect(corners({ x: 20, y: 0 }, { x: 60, y: 100 }, true, 30)[1]).toEqual({ x: 20, y: 30 });
  });

  it('finds a point along an edge from either end', () => {
    const pts = corners({ x: 0, y: 0 }, { x: 40, y: 100 }, true);
    expect(at(pts, 10)).toEqual({ x: 0, y: 10 });
    expect(at(pts, -10)).toEqual({ x: 40, y: 90 });
    expect(halfway(pts)).toEqual({ x: 20, y: 50 });
  });

  it('centers a box over its middle destination, or between the middle two', () => {
    expect(centerOver([300])).toBe(300);
    expect(centerOver([500, 100])).toBe(300);
    expect(centerOver([100, 900, 300])).toBe(300);
  });

  it('pushes overlapping boxes aside, keeping them centered on what they want together', () => {
    expect(pushApart([100, 500], 100, 10, 0, 1000)).toEqual([100, 500]);
    expect(pushApart([300, 300], 100, 10, 0, 1000)).toEqual([245, 355]);
    expect(pushApart([300, 300, 300], 100, 10, 0, 1000)).toEqual([190, 300, 410]);
    // Pulled back inside the row.
    expect(pushApart([0, 0], 100, 10, 0, 1000)).toEqual([50, 160]);
  });
});

// The same routes as the server's `a_route_is_named_by_its_source_and_pipeline_and_numbered_among_its_twins`.
describe("a route's name", () => {
  const routes = [
    { id: 'a', source: 'k', pipeline: 'p', condition: 'true' },
    { id: 'b', source: 'k', pipeline: 'q-1f', condition: null },
    { id: 'c', source: 'j', pipeline: 'p', condition: null },
    { id: 'd', source: 'k', pipeline: 'p', condition: null },
  ];
  const pipelines = [
    { id: 'p', name: 'p', steps: [] },
    { id: 'q-1f', name: 'Enrich', steps: [] },
  ];

  it('is its source and pipeline, numbered among the routes that share both', () => {
    const names = routes.map((_, i) =>
      routeName(routes, i, (id) => (id === 'q-1f' ? 'Enrich' : id)),
    );
    expect(names).toEqual(['k \u2192 p (1)', 'k \u2192 Enrich', 'j \u2192 p', 'k \u2192 p (2)']);
  });

  it("counts a group's routes after the shared ones it runs first", () => {
    const shared = { routes: [routes[0]], pipelines };
    expect(routeNameIn({ routes: [routes[3]], pipelines: [] }, 'd', shared)).toBe('k \u2192 p (2)');
    expect(routeNameIn({ routes: [routes[1]], pipelines }, 'b')).toBe('k \u2192 Enrich');
    expect(routeNameIn({ routes: [], pipelines }, 'b')).toBeUndefined();
  });
});
