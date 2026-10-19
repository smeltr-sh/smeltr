import { describe, expect, it } from 'vitest';
import { compareValues, TableSort } from './sort.svelte';

type Row = { name: string; n?: number };
const rows: Row[] = [{ name: 'worker-10', n: 3 }, { name: 'worker-9' }, { name: 'Worker-2', n: 1 }];
const columns = { name: (r: Row) => r.name, n: (r: Row) => r.n };

describe('TableSort', () => {
  it('keeps the table its own order until a column is chosen', () => {
    expect(new TableSort<'name' | 'n'>().apply(rows, columns)).toEqual(rows);
  });

  it('sorts ascending on the first click and descending on the second', () => {
    const sort = new TableSort<'name' | 'n'>();
    sort.toggle('name');
    expect(sort.apply(rows, columns).map((r) => r.name)).toEqual([
      'Worker-2',
      'worker-9',
      'worker-10',
    ]);
    sort.toggle('name');
    expect(sort.apply(rows, columns).map((r) => r.name)).toEqual([
      'worker-10',
      'worker-9',
      'Worker-2',
    ]);
  });

  it('starts another column ascending', () => {
    const sort = new TableSort<'name' | 'n'>('name', -1);
    sort.toggle('n');
    expect(sort.key).toBe('n');
    expect(sort.dir).toBe(1);
  });

  it('puts a row with no value last whichever way', () => {
    const sort = new TableSort<'name' | 'n'>('n');
    expect(sort.apply(rows, columns).map((r) => r.n)).toEqual([1, 3, undefined]);
    sort.toggle('n');
    expect(sort.apply(rows, columns).map((r) => r.n)).toEqual([3, 1, undefined]);
  });

  it('breaks a tie with the table its own order', () => {
    const tied = [
      { name: 'b', n: 1 },
      { name: 'a', n: 1 },
    ];
    expect(new TableSort<'name' | 'n'>('n').apply(tied, columns)).toEqual(tied);
  });

  it('keeps the order when the sorted column has gone', () => {
    expect(new TableSort<string>('gone').apply(rows, columns)).toEqual(rows);
    expect(new TableSort<string>('toString').apply(rows, columns)).toEqual(rows);
  });
});

describe('compareValues', () => {
  it('compares numbers by size, not as text', () => {
    expect(compareValues(9, 10)).toBeLessThan(0);
  });
});
