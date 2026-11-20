import { configAt, SHARED_SCOPE } from './api';
import { bundleRoutes } from './routeFlow';

/** Pipeline names by id. A pipeline's id is a key nobody chose, so a page shows its name. */
export type PipelineNames = Record<string, string>;

/** The pipelines and routes `scope` runs on `branch`, by id, each route by its `routeName`: its own and, in a group,
 * `shared/`'s, which a group runs merged with its own. Its own win. A bundle that cannot be read names nothing. */
export async function namesAt(branch: string, scope: string) {
  const scopes = scope === SHARED_SCOPE ? [SHARED_SCOPE] : [SHARED_SCOPE, scope];
  const read = await Promise.allSettled(scopes.map((s) => configAt(branch, s)));
  const [shared, own] = read.map((r) => (r.status === 'fulfilled' && r.value.config) || null);
  const pipelines: PipelineNames = {};
  for (const p of [...(shared?.pipelines ?? []), ...(own?.pipelines ?? [])])
    pipelines[p.id] = p.name;
  const bundle = scopes.length === 1 ? shared : own;
  const routes = bundle ? bundleRoutes(bundle, own && shared) : [];
  return {
    pipelines,
    routes: Object.fromEntries(routes.map((r) => [r.id, r.name])) as PipelineNames,
  };
}

/** `namesAt`'s pipelines. */
export async function pipelineNamesAt(branch: string, scope: string): Promise<PipelineNames> {
  return (await namesAt(branch, scope)).pipelines;
}

/** What a reader calls pipeline `id`: its name, or the id of one nothing names. */
export function nameOf(names: PipelineNames, id: string): string {
  return names[id] || id;
}
