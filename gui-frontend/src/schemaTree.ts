// The fields of the schemas a pipeline produces, one level at a time, as the editor offers them: the name boxes'
// suggestions (`ValidatedField`) and the Fields menu (`FieldsMenu`) list the same fields in the same order.

import type { Requirement, SchemaDto, SchemaFieldDto } from './api';

/** One key under a path: a field the schemas declare there, the fields under it, or both. */
export interface SchemaChild {
  /** The key itself. */
  name: string;
  /** Its keys from the record's root, dotted. */
  path: string;
  /** The field the schemas declare at `path`, if they declare one. */
  field?: SchemaFieldDto;
  /** How many fields the schemas declare under `path`. Above 0, it is an object to open. */
  nested: number;
  /** How many of those are required. */
  required: number;
  /** How many of those are recommended. */
  recommended: number;
  /** The record holds it. */
  set: boolean;
}

const RANK: Record<Requirement, number> = { required: 0, recommended: 1, optional: 2 };

/** The strongest of two requirements: a field two schemas declare is held to the stricter. */
function stronger(a: Requirement | undefined, b: Requirement | undefined): Requirement {
  return RANK[a ?? 'optional'] <= RANK[b ?? 'optional'] ? (a ?? 'optional') : (b ?? 'optional');
}

/** `child`'s own requirement: optional where the schemas declare no field there. */
export function requirementOf(child: SchemaChild): Requirement {
  return child.field?.requirement ?? 'optional';
}

/** Where `child` sorts: what is asked for first, then objects holding what is asked for, then the optional fields,
 * then the other objects; what the record holds last. A list draws a rule where the group passes 2 and 4. */
export function groupOf(child: SchemaChild): number {
  if (child.set) return 5;
  const own = requirementOf(child);
  if (own !== 'optional') return RANK[own];
  if (child.nested > 0) return asked(child) > 0 ? 2 : 4;
  return 3;
}

const asked = (c: SchemaChild) => c.required + c.recommended;

function compare(a: SchemaChild, b: SchemaChild): number {
  return (
    groupOf(a) - groupOf(b) ||
    asked(b) - asked(a) ||
    (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)
  );
}

/** The produced schemas' fields, indexed by the path they sit under. Built once per schema list: a standard's
 * schema holds tens of thousands of fields. */
export class SchemaTree {
  /** The titles of the schemas, as a list names them. */
  readonly titles: string;
  readonly total: number;
  #fields = new Map<string, SchemaFieldDto>();
  #under = new Map<string, Map<string, SchemaChild>>();

  constructor(schemas: SchemaDto[]) {
    this.titles = schemas.map((s) => s.title || s.id).join(', ');
    const fields = this.#fields;
    for (const s of schemas)
      for (const f of s.fields) {
        const had = fields.get(f.field);
        fields.set(
          f.field,
          had ? { ...had, requirement: stronger(had.requirement, f.requirement) } : f,
        );
      }
    this.total = fields.size;
    for (const f of fields.values()) {
      const keys = f.field.split('.');
      const requirement = f.requirement ?? 'optional';
      let parent = '';
      for (let i = 0; i < keys.length; i++) {
        const path = parent ? `${parent}.${keys[i]}` : keys[i];
        let level = this.#under.get(parent);
        if (!level) this.#under.set(parent, (level = new Map()));
        let child = level.get(keys[i]);
        if (!child)
          level.set(
            keys[i],
            (child = { name: keys[i], path, nested: 0, required: 0, recommended: 0, set: false }),
          );
        if (i === keys.length - 1) child.field = f;
        else {
          child.nested++;
          if (requirement === 'required') child.required++;
          if (requirement === 'recommended') child.recommended++;
        }
        parent = path;
      }
    }
  }

  /** The keys under `parent` (`''` for the root), best first, each marked set where `held` has its path. */
  children(parent: string, held: ReadonlySet<string> = new Set()): SchemaChild[] {
    const level = this.#under.get(parent);
    if (!level) return [];
    return [...level.values()].map((c) => ({ ...c, set: held.has(c.path) })).sort(compare);
  }

  /** The requirement the schemas hold `path` to, or undefined where they declare no field there. */
  requirement(path: string): Requirement | undefined {
    const f = this.#fields.get(path);
    return f && (f.requirement ?? 'optional');
  }

  /** How many fields of each requirement the schemas declare, and how many of those `held` has. */
  tally(held: ReadonlySet<string>): Record<Requirement, { set: number; of: number }> {
    const t = {
      required: { set: 0, of: 0 },
      recommended: { set: 0, of: 0 },
      optional: { set: 0, of: 0 },
    };
    for (const f of this.#fields.values()) {
      const r = t[f.requirement ?? 'optional'];
      r.of++;
      if (held.has(f.field)) r.set++;
    }
    return t;
  }

  /** Where a search stands in the schemas: under the object it names, else as `typedPath` reads it. */
  at(text: string): { parent: string; key: string } | null {
    const q = text.trim();
    if (q && !q.endsWith('.') && this.nestedUnder(q) > 0) return { parent: q, key: '' };
    return typedPath(q);
  }

  /** How many fields the schemas declare under `parent`, at any depth. */
  nestedUnder(parent: string): number {
    if (parent === '') return this.total;
    const i = parent.lastIndexOf('.');
    const child = this.#under.get(i < 0 ? '' : parent.slice(0, i))?.get(parent.slice(i + 1));
    return child?.nested ?? 0;
  }
}

/** The path a name box's text stands under, and the key being typed after it: `destination.ge` is under
 * `destination`, typing `ge`. Null where the text is no plain dotted path. */
export function typedPath(text: string): { parent: string; key: string } | null {
  if (!/^[^.\s[\]"'`]*(\.[^.\s[\]"'`]*)*$/.test(text)) return null;
  const i = text.lastIndexOf('.');
  if (i < 0) return { parent: '', key: text };
  const parent = text.slice(0, i);
  if (parent.split('.').some((k) => k === '')) return null;
  return { parent, key: text.slice(i + 1) };
}

/** The objects of one group a short list names, while fields of other groups wait to be named. */
const OBJECTS_NAMED = 3;

/** The first `n` of `children`, in their order, naming at most a few objects of a group while there are other
 * entries to name: a standard nests most of its fields, and its objects would fill the list ahead of the fields. */
export function firstOf(children: SchemaChild[], n: number): SchemaChild[] {
  const objects = new Map<number, number>();
  const taken = new Set<SchemaChild>();
  for (const c of children) {
    if (taken.size === n) break;
    const g = groupOf(c);
    if (c.nested > 0 && (g === 2 || g === 4)) {
      const seen = objects.get(g) ?? 0;
      if (seen >= OBJECTS_NAMED) continue;
      objects.set(g, seen + 1);
    }
    taken.add(c);
  }
  for (const c of children) {
    if (taken.size === n) break;
    taken.add(c);
  }
  return children.filter((c) => taken.has(c));
}

/** `children` whose key starts with `key`, then those holding it, each in their own order. */
export function matching(children: SchemaChild[], key: string): SchemaChild[] {
  const k = key.toLowerCase();
  if (!k) return children;
  const starts = children.filter((c) => c.name.toLowerCase().startsWith(k));
  const holds = children.filter(
    (c) => !c.name.toLowerCase().startsWith(k) && c.name.toLowerCase().includes(k),
  );
  return [...starts, ...holds];
}
