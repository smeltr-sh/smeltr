// The schema catalog as the GUI shows it: who uses a schema, how OCSF's classes fall into its categories, and a
// schema handed over as a file.

import type { DestinationDto, PipelineDto, SchemaSummary } from './api';

/** One object naming a schema: a pipeline producing it, or a destination accepting it. */
export type User = { kind: 'pipeline' | 'destination'; id: string };

/** Each schema id the pipelines produce and the destinations accept, with what names it, pipelines first. */
export function usersOf(
  pipelines: Pick<PipelineDto, 'id' | 'schemas'>[],
  destinations: Pick<DestinationDto, 'id' | 'accepts'>[],
): Map<string, User[]> {
  const out = new Map<string, User[]>();
  const add = (schema: string, user: User) => {
    const list = out.get(schema);
    if (!list) out.set(schema, [user]);
    else if (!list.some((u) => u.kind === user.kind && u.id === user.id)) list.push(user);
  };
  for (const p of pipelines)
    for (const s of p.schemas ?? []) add(s, { kind: 'pipeline', id: p.id });
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

/** OCSF's classes under each of its categories, by class uid, the categories by uid. */
export function byCategory(
  categories: { uid: number; caption: string }[],
  schemas: SchemaSummary[],
): { uid: number; caption: string; classes: SchemaSummary[] }[] {
  return [...categories]
    .sort((a, b) => a.uid - b.uid)
    .map((c) => ({
      uid: c.uid,
      caption: c.caption,
      classes: schemas
        .filter((s) => s.origin === 'ocsf' && s.category === c.uid)
        .sort((a, b) => (a.classUid ?? 0) - (b.classUid ?? 0)),
    }))
    .filter((c) => c.classes.length > 0);
}

/** How many classes a category shows at once. */
export const PAGE = 10;

/** The page of `items` numbered `page`, from 0, and how many pages there are. */
export function paged<T>(items: T[], page: number): { shown: T[]; pages: number; first: number } {
  const pages = Math.max(1, Math.ceil(items.length / PAGE));
  const at = Math.min(Math.max(page, 0), pages - 1);
  return { shown: items.slice(at * PAGE, (at + 1) * PAGE), pages, first: at * PAGE };
}

/** A schema's file name: its id, with what a file name cannot hold made a dash. */
export function fileName(id: string): string {
  return `${id.replace(/[^A-Za-z0-9._-]+/g, '-')}.schema.json`;
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
