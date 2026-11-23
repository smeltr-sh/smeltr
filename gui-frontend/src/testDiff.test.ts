import { describe, expect, it } from 'vitest';
import type { TestMark } from './api';
import { differenceNote, rowDifferences } from './testDiff';

const rows = [
  { key: 10, at: 0 },
  { key: 11, at: 1 },
  { key: 12, at: 1 },
  { key: 13, at: 2 },
];

describe('rowDifferences', () => {
  it('keeps each value that differs by its path, as the leader names it', () => {
    const marks: TestMark[] = [
      {
        row: 0,
        document: 0,
        fields: [
          { path: 'tax', locked: 2, made: true },
          { path: 'coupon', made: true },
        ],
        expected: { row: 0, values: { tax: 2 } },
        actual: { row: 0, values: { tax: 3, coupon: 'X' } },
      },
    ];
    const d = rowDifferences(marks, rows).get(10)!;
    expect([...d.fields.keys()]).toEqual(['tax', 'coupon']);
    expect(d.fields.get('tax')).toEqual({ path: 'tax', locked: 2, made: true });
    expect(d.notes).toEqual([]);
  });

  it("puts a mark on its row's document, and a document the run did not make on the row's last", () => {
    const marks: TestMark[] = [
      {
        row: 1,
        document: 1,
        fields: [{ path: 'id', locked: 1, made: false }],
        expected: { row: 1, values: { id: 1 } },
        actual: { row: 1, values: {} },
      },
      { row: 1, document: 2, fields: [], expected: { row: 1, values: { id: 2 } } },
    ];
    const d = rowDifferences(marks, rows);
    expect([...d.keys()]).toEqual([12]);
    expect(d.get(12)!.notes).toEqual([
      'The test locks another document on this row; this run made none.',
    ]);
  });

  it('passes over a mark on a row not on screen', () => {
    const marks: TestMark[] = [{ row: 7, document: 0, fields: [], expected: { row: 7 } }];
    expect(rowDifferences(marks, rows).size).toBe(0);
  });
});

describe('differenceNote', () => {
  it('says what differs of a whole document, and nothing where only fields differ', () => {
    const doc = { row: 0, values: { a: 1 } };
    const dropped = { row: 0 };
    expect(
      differenceNote({ row: 0, expected: doc, actual: { row: 0, values: { a: 2 } } }),
    ).toBeNull();
    expect(differenceNote({ row: 0, actual: doc })).toBe(
      'The test locks no more documents on this row; this run made another.',
    );
    expect(differenceNote({ row: 0, expected: dropped, actual: doc })).toBe(
      'The test locked this row as dropped; this run kept it.',
    );
    expect(differenceNote({ row: 0, expected: doc, actual: dropped })).toBe(
      'The test locked a document here; this run dropped it.',
    );
    const sent = (route: string) => ({
      ...doc,
      sent: { pipeline: 'p', route, destinations: ['d'] },
    });
    expect(differenceNote({ row: 0, expected: sent('a'), actual: sent('b') })).toBe(
      'This run sent the document elsewhere.',
    );
  });
});
