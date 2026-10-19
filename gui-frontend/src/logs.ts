/** The Logs screen's pure parts: the hash a set of filters is, and the
 * shape a stored field list renders as. Kept out of the component so they
 * can be tested without one (`logs.test.ts`). */

import type { LogRowDto } from './api';

/** The severities a filter may name, most severe first. Mirrors
 * `logs::LEVEL_LABELS`. */
export const LEVELS = ['error', 'warn', 'info', 'debug', 'trace'];

/** What the screen is looking at. Absent means "any", so an empty filter
 * asks for the newest of everything. */
export type Filters = {
  levels: string[];
  worker?: string;
  group?: string;
  site?: string;
  q?: string;
  /** The pager's position: rows below/above this id
   * (`LogsResponseDto.nextBefore` / `.nextAfter`). Carried like a filter, in
   * the hash and in the request, since a page worth returning to is a link. */
  before?: number;
  after?: number;
};

export const NO_FILTERS: Filters = { levels: [] };

/** These filters as query parameters. One builder, so the hash a reader
 * copies and the request the screen makes cannot drift apart. */
function params(f: Filters): URLSearchParams {
  const params = new URLSearchParams();
  // One comma-separated key, not a repeated one: the leader decodes its
  // query with `serde_urlencoded`, which refuses a repeated key outright.
  if (f.levels.length > 0) params.set('level', f.levels.join(','));
  for (const [name, value] of [
    ['worker', f.worker],
    ['group', f.group],
    ['site', f.site],
    ['q', f.q],
  ] as const) {
    if (value) params.set(name, value);
  }
  for (const [name, value] of [
    ['before', f.before],
    ['after', f.after],
  ] as const) {
    if (value !== undefined) params.set(name, String(value));
  }
  return params;
}

/** `f` with its page position dropped. A new search reads a different set of
 * rows, and a position in the old set means nothing in it, so every control
 * that changes a filter goes through this. */
export function resetCursor(f: Filters): Filters {
  const { before, after, ...rest } = f;
  return rest;
}

/** The hash these filters are, so a view worth showing someone is a link
 * and the back button walks the searches actually made. */
export function toHash(f: Filters): string {
  const query = params(f).toString();
  return query ? `#/status/logs?${query}` : '#/status/logs';
}

/** The query string for `GET /api/logs`: the same filters plus the page
 * size, which is the screen's business rather than the reader's and so
 * stays out of the hash. */
export function toQuery(f: Filters, limit: number): string {
  const q = params(f);
  q.set('limit', String(limit));
  return q.toString();
}

/** `levels` with `level` added or taken away -- what a severity chip does.
 * Kept in `LEVELS` order so the same set is always the same hash. */
export function toggleLevel(levels: string[], level: string): string[] {
  const next = levels.includes(level) ? levels.filter((l) => l !== level) : [...levels, level];
  return LEVELS.filter((l) => next.includes(l));
}

/** True when anything is narrowed, which is what decides whether a "clear"
 * control has something to do. */
export function isFiltered(f: Filters): boolean {
  return f.levels.length > 0 || Boolean(f.worker || f.group || f.site || f.q);
}

/** One field of a row, flattened for display from the delivered
 * `[name, {kind, value}]` pairs. A pair this build cannot read is dropped
 * rather than shown as `undefined`: the row is still worth reading. */
export type Field = { name: string; value: string };

export function fields(row: Pick<LogRowDto, 'fields'>): Field[] {
  if (!Array.isArray(row.fields)) return [];
  const out: Field[] = [];
  for (const pair of row.fields) {
    if (!Array.isArray(pair) || pair.length !== 2) continue;
    const [name, tagged] = pair;
    if (typeof name !== 'string' || tagged === null || typeof tagged !== 'object') continue;
    const value = (tagged as { value?: unknown }).value;
    if (value === undefined || value === null) continue;
    out.push({ name, value: String(value) });
  }
  return out;
}

/** Whether a message reached the leader whole. The heartbeat cuts one at
 * `messageLimit` characters, so a search cannot match past there and a
 * reader is owed that fact rather than left to wonder. */
export function wasTruncated(message: string, messageLimit: number): boolean {
  return message.length >= messageLimit;
}
