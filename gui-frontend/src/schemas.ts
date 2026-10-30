// Schemas as the GUI shows them: who uses a schema, a search over their fields, and a schema handed over as a file.

import type { DestinationDto, ExportFormat, PipelineDto, SchemaDto } from './api';

/** One object naming a schema: a pipeline producing it, or a destination accepting it. */
export type User = { kind: 'pipeline' | 'destination'; id: string };

/** Each schema id the pipelines produce and the destinations accept, with what names it, pipelines first. */
export function usersOf(
  pipelines: Pick<PipelineDto, 'id' | 'produces'>[],
  destinations: Pick<DestinationDto, 'id' | 'accepts'>[],
): Map<string, User[]> {
  const out = new Map<string, User[]>();
  const add = (schema: string, user: User) => {
    const list = out.get(schema);
    if (!list) out.set(schema, [user]);
    else if (!list.some((u) => u.kind === user.kind && u.id === user.id)) list.push(user);
  };
  for (const p of pipelines)
    for (const s of p.produces ?? []) add(s, { kind: 'pipeline', id: p.id });
  for (const d of destinations)
    for (const s of d.accepts ?? []) add(s, { kind: 'destination', id: d.id });
  return out;
}

/** Where a user is edited. */
export function userHash(user: User): string {
  return user.kind === 'pipeline'
    ? `#/build/pipelines/${encodeURIComponent(user.id)}`
    : `#/build/destinations/${encodeURIComponent(user.id)}`;
}

/** How many matching fields a match names. */
const FIELDS_NAMED = 5;

/** Every schema whose id, title or a field's path holds `q`, ignoring case, by id, each with the first fields whose
 * path holds it: for someone who knows a field and not the schema it is in. Null while `q` is blank. */
export function matchesOf(
  schemas: Pick<SchemaDto, 'id' | 'title' | 'fields'>[],
  q: string,
): Map<string, string[]> | null {
  const needle = q.trim().toLowerCase();
  if (!needle) return null;
  const out = new Map<string, string[]>();
  for (const s of schemas) {
    const fields = s.fields
      .map((f) => f.field)
      .filter((path) => path.toLowerCase().includes(needle))
      .slice(0, FIELDS_NAMED);
    const named =
      s.id.toLowerCase().includes(needle) || (s.title ?? '').toLowerCase().includes(needle);
    if (named || fields.length > 0) out.set(s.id, fields);
  }
  return out;
}

/** A schema's file name as `format` exports it: its id, with what a file name cannot hold made a dash. */
export function fileName(id: string, format: ExportFormat): string {
  const kind = format === 'json-schema' ? 'schema' : 'mapping';
  return `${id.replace(/[^A-Za-z0-9._-]+/g, '-')}.${kind}.json`;
}

/** Hands `value` to the browser as a JSON file named `name`. */
export function downloadJson(name: string, value: unknown) {
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(value, null, 2) + '\n'], { type: 'application/json' }),
  );
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

/**
 * The Elasticsearch types a field of the type `type` may map to, as the leader checks them
 * (`io::elastic_mapping::types_for`): first the one it maps to of itself, where it has one. An array's are its
 * element's.
 */
export function elasticsearchTypes(type: string): string[] {
  const array = /^array<(.*)>$/.exec(type.trim());
  if (array) return elasticsearchTypes(array[1]);
  const t = type.trim();
  if (t.startsWith('{')) return ['object', 'nested', 'flattened', 'geo_point'];
  switch (t) {
    case 'str':
      return [
        'keyword',
        'constant_keyword',
        'wildcard',
        'text',
        'match_only_text',
        'ip',
        'version',
        'date',
      ];
    case 'duration':
      return ['long', 'integer', 'short', 'byte', 'unsigned_long'];
    case 'i64':
      return [
        'long',
        'integer',
        'short',
        'byte',
        'unsigned_long',
        'double',
        'float',
        'half_float',
        'scaled_float',
        'date',
      ];
    case 'f64':
      return ['double', 'float', 'half_float', 'scaled_float'];
    case 'bool':
      return ['boolean'];
    case 'datetime':
      return ['date', 'date_nanos'];
    case 'bytes':
      return ['binary'];
    case 'variant':
      return [
        'keyword',
        'text',
        'wildcard',
        'long',
        'double',
        'boolean',
        'date',
        'object',
        'flattened',
        'binary',
      ];
    default:
      return [];
  }
}
