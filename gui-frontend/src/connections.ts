// What an editor's Connections line shows (user story A5, blast radius): the rows of one object's
// connections, the line they fold into, and where each name leads.

import type { Connected, ConnectionKind, ConnectionsResponse } from './api';

/** How many names a row shows before the rest fold into "and N more". */
export const SHOWN = 5;

export type Row = {
  label: string;
  /** What one of the row's objects is called, for the folded line's count. */
  noun: string;
  items: Connected[];
};

/** The kinds the rows of what is downstream come in, in this order. */
const DOWNSTREAM: ConnectionKind[] = ['source', 'route', 'pipeline', 'grokPattern', 'destination'];

const NOUN: Record<ConnectionKind, string> = {
  source: 'source',
  route: 'route',
  pipeline: 'pipeline',
  destination: 'destination',
  lookupTable: 'table',
  grokPattern: 'pattern',
};

function plural(noun: string): string {
  return `${noun}s`;
}

/** A row named for its kind, except where the editor's own kind sits on the other side of it:
 * a pipeline is called by the pipelines downstream of it, and a destination refuses to the
 * destinations downstream of it. */
function downstreamLabel(editing: ConnectionKind, kind: ConnectionKind): string {
  if (editing === 'pipeline' && kind === 'pipeline') return 'called by';
  if (editing === 'destination' && kind === 'destination') return 'refused to';
  return plural(NOUN[kind]);
}

/** The rows of `c`, an object of kind `editing`: what it holds a contract with upstream first,
 * then what is downstream of it. A row with nothing in it is left out. */
export function rows(editing: ConnectionKind, c: ConnectionsResponse): Row[] {
  const out: Row[] = [];
  const upstreamKinds = [...new Set(c.upstream.map((o) => o.kind))];
  for (const kind of upstreamKinds) {
    out.push({
      label: plural(NOUN[kind]),
      noun: NOUN[kind],
      items: c.upstream.filter((o) => o.kind === kind),
    });
  }
  for (const kind of DOWNSTREAM) {
    const items = c.downstream.filter((o) => o.kind === kind);
    if (items.length > 0)
      out.push({ label: downstreamLabel(editing, kind), noun: NOUN[kind], items });
  }
  return out;
}

/** One row on the folded line: the object itself where there is one, a count where there are
 * several. */
function folded(row: Row): string {
  const one = row.items.length === 1;
  const what = one ? row.items[0].id : `${row.items.length} ${plural(row.noun)}`;
  if (row.label === plural(row.noun)) return one ? `${row.noun} ${what}` : what;
  return `${row.label} ${what}`;
}

/** The line the rows fold into. Routes are left out of it: the path they decide shows in the
 * sources and destinations either side of them. */
export function summary(all: Row[], groups: string[]): string {
  const parts = all.filter((r) => r.noun !== 'route').map(folded);
  if (all.length === 0) parts.push('nothing yet');
  if (groups.length === 1) parts.push(groups[0]);
  else if (groups.length > 1) parts.push(`${groups.length} groups`);
  return parts.join(' · ');
}

/** Where one object opens. */
export function objectHash(o: Pick<Connected, 'kind' | 'id'>): string {
  const id = encodeURIComponent(o.id);
  switch (o.kind) {
    case 'source':
      return `#/build/sources/${id}`;
    case 'destination':
      return `#/build/destinations/${id}`;
    case 'pipeline':
      return `#/build/pipelines/${id}`;
    default:
      return listHash(o.kind, [o.id]);
  }
}

const LIST: Record<ConnectionKind, string> = {
  source: 'sources',
  route: 'routes',
  pipeline: 'pipelines',
  destination: 'destinations',
  lookupTable: 'lookups',
  grokPattern: 'grok',
};

/** `kind`'s list, narrowed to `ids`. Tables never appear in a row, so their lists
 * take no `only`. */
export function listHash(kind: ConnectionKind, ids: string[]): string {
  return `#/build/${LIST[kind]}?${new URLSearchParams(ids.map((id) => ['only', id]))}`;
}

/** `id` as one term of the map's search, whatever it holds (`map.ts`'s `parseQuery`). */
function quoted(id: string): string {
  return `"${id.replace(/["\\]/g, '\\$&')}"`;
}

/** Where the map draws the object, if it does: its node, or where several groups each draw it,
 * every one of them picked out by the map's search. */
export function mapHash(kind: ConnectionKind, id: string, drawnIn: string[]): string | undefined {
  if (drawnIn.length === 0) return undefined;
  if (kind !== 'source' && kind !== 'pipeline' && kind !== 'destination') return undefined;
  if (drawnIn.length > 1) {
    return `#/map?${new URLSearchParams({ q: `kind:${kind} id:${quoted(id)}` })}`;
  }
  return `#/map?${new URLSearchParams({ group: drawnIn[0], kind, id })}`;
}
