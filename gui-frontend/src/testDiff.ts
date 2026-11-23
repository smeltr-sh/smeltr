import type { TestDto } from './api';

/** How one row on screen differs from what a test locks of it. */
export type RowDifference = {
  /** Each field the test locks that the row holds otherwise, by path: the locked value, or absent where the test
   * locks the field as missing. */
  fields: Map<string, { locked?: unknown }>;
  /** The whole row differs: the test locks it as dropped and the run kept it, or the other way round. */
  whole?: 'kept' | 'dropped';
};

/** A row on screen, as a test reads it: the row the test numbers it by, and its fields by path. */
export type TestedRow = { key: number; at: number; dropped: boolean; leaves: Map<string, unknown> };

/** Each row in `rows` that differs from what `test` locks, by key. A row the test does not check is never read; the
 * documents one record made are matched to the test's in the order they come, as the leader's compare does. */
export function differences(test: TestDto, rows: TestedRow[]): Map<number, RowDifference> {
  const locked = new Map<number, TestDto['rows']>();
  for (const o of test.rows) locked.set(o.row, [...(locked.get(o.row) ?? []), o]);
  const seen = new Map<number, number>();
  const out = new Map<number, RowDifference>();
  for (const row of rows) {
    const outcomes = locked.get(row.at);
    if (!outcomes) continue;
    const nth = seen.get(row.at) ?? 0;
    seen.set(row.at, nth + 1);
    const want = outcomes[nth];
    const fields = new Map<string, { locked?: unknown }>();
    if (!want) {
      out.set(row.key, { fields, whole: 'kept' });
      continue;
    }
    if (!want.values || row.dropped) {
      if (!want.values !== row.dropped)
        out.set(row.key, { fields, whole: row.dropped ? 'dropped' : 'kept' });
      continue;
    }
    for (const path of test.fields) {
      const [has, now] = [Object.hasOwn(want.values, path), row.leaves.has(path)];
      if (!has && !now) continue;
      if (has && now && same(want.values[path], row.leaves.get(path))) continue;
      fields.set(path, has ? { locked: want.values[path] } : {});
    }
    if (fields.size > 0) out.set(row.key, { fields });
  }
  return out;
}

/** How far apart two non-integer numbers may be, relative to the larger, and still be one value: the leader's
 * tolerance (`config_file::compare`). */
const FLOAT_TOLERANCE = 1e-9;

/** Whether `a` and `b` are one value to a test, as the leader judges it: integers exactly, other numbers within
 * `FLOAT_TOLERANCE`, arrays and objects member by member. */
export function same(a: unknown, b: unknown): boolean {
  if (typeof a === 'number' && typeof b === 'number') {
    if (Number.isInteger(a) && Number.isInteger(b)) return a === b;
    return a === b || Math.abs(a - b) <= FLOAT_TOLERANCE * Math.max(Math.abs(a), Math.abs(b));
  }
  if (Array.isArray(a) && Array.isArray(b))
    return a.length === b.length && a.every((x, i) => same(x, b[i]));
  if (
    a &&
    b &&
    typeof a === 'object' &&
    typeof b === 'object' &&
    !Array.isArray(a) &&
    !Array.isArray(b)
  ) {
    const [x, y] = [a as Record<string, unknown>, b as Record<string, unknown>];
    const keys = Object.keys(x);
    return keys.length === Object.keys(y).length && keys.every((k) => k in y && same(x[k], y[k]));
  }
  return a === b;
}
