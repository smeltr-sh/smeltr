/**
 * A table's sort: which column orders its rows, and which way.
 *
 * A table starts in its own order -- the order its rows mean something in, a
 * log's time or a file's position -- and a click on a column head sorts by that
 * column, ascending, then descending on the next click. A click on another
 * column starts that one ascending.
 */

/** What a column sorts by. Absent values sort last, whichever way. */
export type SortValue = string | number | null | undefined;

export class TableSort<K extends string = string> {
  key = $state<K | null>(null);
  dir = $state<1 | -1>(1);

  constructor(key: K | null = null, dir: 1 | -1 = 1) {
    this.key = key;
    this.dir = dir;
  }

  toggle(key: K): void {
    if (this.key === key) this.dir = this.dir === 1 ? -1 : 1;
    else {
      this.key = key;
      this.dir = 1;
    }
  }

  /** The head's `aria-sort`. */
  aria(key: K): 'ascending' | 'descending' | 'none' {
    if (this.key !== key) return 'none';
    return this.dir === 1 ? 'ascending' : 'descending';
  }

  /** `rows` in this sort's order, as a new array; `rows` itself unchanged while no column is chosen.
   * Equal values keep the order `rows` had, so the table's own order breaks ties. */
  apply<T>(rows: readonly T[], columns: Record<K, (row: T) => SortValue>): T[] {
    const key = this.key;
    // A column can leave while it is the sorted one: a grid's hidden field, a commit's narrower table.
    if (key === null || !Object.hasOwn(columns, key)) return [...rows];
    const value = columns[key];
    const dir = this.dir;
    return rows
      .map((row, i) => ({ row, i, v: value(row) }))
      .sort((a, b) => compareValues(a.v, b.v, dir) || a.i - b.i)
      .map(({ row }) => row);
  }
}

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });

/** Numbers by size, text the way a person reads it (`worker-9` before `worker-10`); an absent value last
 * either way, since it is not small, only missing. */
export function compareValues(a: SortValue, b: SortValue, dir: 1 | -1 = 1): number {
  const absentA = a === null || a === undefined || a === '';
  const absentB = b === null || b === undefined || b === '';
  if (absentA || absentB) return absentA === absentB ? 0 : absentA ? 1 : -1;
  if (typeof a === 'number' && typeof b === 'number') return dir * (a - b);
  return dir * collator.compare(String(a), String(b));
}
