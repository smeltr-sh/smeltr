import type { RoutePreviewResult, RoutePreviewRow } from './api';

/** What a route preview's run comes to, as the tiles count it and the diagram draws it. */
export interface RouteCounts {
  /** Every destination the sample would reach, with how many documents each would receive, most first. */
  reached: { id: number; name: string; documents: number }[];
  /** Records no route matched. */
  unrouted: number;
  /** Records a route matched that were sent nowhere. */
  dropped: RoutePreviewRow[];
}

export function routeCounts(result: RoutePreviewResult): RouteCounts {
  const by = new Map<number, number>();
  for (const row of result.rows)
    for (const t of row.through)
      for (const doc of t.documents)
        for (const d of doc.destinations) by.set(d, (by.get(d) ?? 0) + 1);
  return {
    reached: [...by.entries()]
      .map(([id, documents]) => ({
        id,
        name: result.destination_names[id] ?? `destination ${id}`,
        documents,
      }))
      .sort((a, b) => b.documents - a.documents),
    unrouted: result.rows.filter((r) => r.matched.length === 0).length,
    dropped: result.rows.filter(
      (r) => r.matched.length > 0 && !r.through.some((t) => t.documents.length > 0),
    ),
  };
}
