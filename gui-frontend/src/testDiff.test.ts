import { describe, expect, it } from 'vitest';
import type { TestDto } from './api';
import { differences, same } from './testDiff';

const test: TestDto = {
  id: 't',
  name: 't',
  pipeline: 'p',
  sample: 's',
  lockedAt: 0,
  rowsIn: 3,
  fields: ['id', 'tax', 'coupon'],
  rows: [
    { row: 0, values: { id: 1, tax: 2 } },
    { row: 1, values: { id: 2, tax: 3.5, coupon: 'X' } },
    { row: 2 },
  ],
};

const row = (key: number, leaves: Record<string, unknown>, dropped = false) => ({
  key,
  at: key,
  dropped,
  leaves: new Map(Object.entries(leaves)),
});

describe('differences', () => {
  it('passes over a row that holds what the test locks, and one the test does not check', () => {
    const d = differences(test, [row(0, { id: 1, tax: 2, other: 9 }), row(7, { id: 9 })]);
    expect(d.size).toBe(0);
  });

  it('names each field that differs, with what the test locks', () => {
    const d = differences(test, [
      row(0, { id: 1, tax: 4, coupon: 'Y' }),
      row(1, { id: 2, tax: 3.5 }),
    ]);
    expect(d.get(0)?.fields).toEqual(
      new Map<string, { locked?: unknown }>([
        ['tax', { locked: 2 }],
        ['coupon', {}],
      ]),
    );
    expect(d.get(1)?.fields).toEqual(new Map([['coupon', { locked: 'X' }]]));
  });

  it('tells a row the test locks as dropped from one it locks a document on', () => {
    const d = differences(test, [
      row(0, {}, true),
      row(2, { id: 3 }),
      row(1, { id: 2, tax: 3.5, coupon: 'X' }),
    ]);
    expect(d.get(0)?.whole).toBe('dropped');
    expect(d.get(2)?.whole).toBe('kept');
    expect(d.has(1)).toBe(false);
  });

  it('matches the documents one record made in order, and marks one past what the test locks', () => {
    const two = { ...test, rows: [{ row: 0, values: { id: 1 } }] };
    const d = differences(two, [
      { ...row(10, { id: 1 }), at: 0 },
      { ...row(11, { id: 1 }), at: 0 },
    ]);
    expect(d.has(10)).toBe(false);
    expect(d.get(11)?.whole).toBe('kept');
  });
});

describe('same', () => {
  it('holds integers exact and other numbers within the tolerance', () => {
    expect(same(1, 1)).toBe(true);
    expect(same(10, 11)).toBe(false);
    expect(same(2.4000000000000004, 2.4)).toBe(true);
    expect(same(2.4, 2.5)).toBe(false);
    expect(same({ a: [1, 0.1 + 0.2] }, { a: [1, 0.3] })).toBe(true);
    expect(same({ a: 1 }, { a: 1, b: 2 })).toBe(false);
    expect(same('1', 1)).toBe(false);
  });
});
