import { configAt, SHARED_SCOPE } from './api';

/** Pipeline names by id. A pipeline's id is a key nobody chose, so a page shows its name. */
export type PipelineNames = Record<string, string>;

/** The pipelines `scope` runs on `branch`, by id: its own and, in a group, `shared/`'s, which a group runs
 * merged with its own. Its own win. A bundle that cannot be read names nothing. */
export async function pipelineNamesAt(branch: string, scope: string): Promise<PipelineNames> {
  const scopes = scope === SHARED_SCOPE ? [SHARED_SCOPE] : [SHARED_SCOPE, scope];
  const names: PipelineNames = {};
  for (const res of await Promise.allSettled(scopes.map((s) => configAt(branch, s)))) {
    if (res.status !== 'fulfilled') continue;
    for (const p of res.value.config?.pipelines ?? []) names[p.id] = p.name;
  }
  return names;
}

/** What a reader calls pipeline `id`: its name, or the id of one nothing names. */
export function nameOf(names: PipelineNames, id: string): string {
  return names[id] || id;
}
