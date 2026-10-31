// What a merge conflict is called on the change page: the bundle it is in and
// the object as the rest of the GUI names it. A pipeline's id and a route's
// are minted, never read, so neither is the name; they stay in the JSON.

import type { MergeConflict } from './api';

const SECTIONS: Record<string, string> = {
  'sources.json': 'Source',
  'destinations.json': 'Destination',
  'routes.json': 'Route',
  'lookup-tables.json': 'Lookup table',
  'grok-patterns.json': 'Grok pattern',
};

/** `west · Pipeline Enrich`, `shared · Route kafka-1 → Enrich`, `west · lookups/t.csv`. A file outside any bundle
 * is its path. A route is called `route`, its `routeName`, which takes the rest of its bundle to work out. */
export function conflictLabel(conflict: MergeConflict, route?: string): string {
  const split = inBundle(conflict.path);
  if (!split) return conflict.path;
  const [bundle, file] = split;
  // The first side whose version names the object: one side may have removed it, or hold text that does not parse.
  const named = (name: (value: Record<string, unknown>) => string | null): string | null => {
    for (const text of [conflict.work, conflict.main, conflict.base]) {
      const value = parsed(text);
      const got = value && name(value);
      if (got) return got;
    }
    return null;
  };
  let label: string | null = null;
  if (file.startsWith('pipelines/')) {
    label = named((v) => (typeof v.name === 'string' ? `Pipeline ${v.name}` : null));
  } else if (file.startsWith('schemas/')) {
    label = named((v) => (typeof v.id === 'string' ? `Schema ${v.id}` : null));
  } else if (file === 'routes.json' && conflict.id !== undefined) {
    label = `Route ${route ?? ''}`.trimEnd();
  } else if (conflict.id !== undefined && SECTIONS[file]) {
    // Every other section's id is the name its author gave it.
    label = `${SECTIONS[file]} ${conflict.id}`;
  }
  return `${bundle} · ${label ?? file}`;
}

/** The bundle's name (a group's, or `shared`) and the file's path within it. */
function inBundle(path: string): [string, string] | null {
  const parts = path.split('/');
  if (parts[0] === 'groups' && parts.length > 2) return [parts[1], parts.slice(2).join('/')];
  if (parts[0] === 'shared' && parts.length > 1) return ['shared', parts.slice(1).join('/')];
  return null;
}

function parsed(text: string | null): Record<string, unknown> | null {
  if (text === null) return null;
  try {
    const value: unknown = JSON.parse(text);
    return typeof value === 'object' && value !== null && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}
