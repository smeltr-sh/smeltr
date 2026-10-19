// How often the bundle's pipelines call each function, which autocomplete
// weighs a little when it ranks (`autocomplete.ts`).

import { SvelteMap } from 'svelte/reactivity';
import type { PipelineDto } from './api';

/** Calls by function name, across the pipelines the editor last read. */
export const functionUsage = new SvelteMap<string, number>();

/** Every `name(` in every string the pipelines hold: their expressions, filters and Code bodies. */
export function countCalls(pipelines: PipelineDto[]): Map<string, number> {
  const counts = new Map<string, number>();
  const visit = (v: unknown) => {
    if (typeof v === 'string') {
      for (const m of v.matchAll(/([A-Za-z_]\w*)\s*\(/g)) {
        counts.set(m[1], (counts.get(m[1]) ?? 0) + 1);
      }
    } else if (Array.isArray(v)) {
      v.forEach(visit);
    } else if (v !== null && typeof v === 'object') {
      Object.values(v).forEach(visit);
    }
  };
  pipelines.forEach((p) => visit(p.steps));
  return counts;
}

/** Counts the calls of the pipelines the editor has read, for autocomplete to rank by. */
export function setFunctionUsage(pipelines: PipelineDto[]): void {
  functionUsage.clear();
  for (const [name, n] of countCalls(pipelines)) functionUsage.set(name, n);
}
