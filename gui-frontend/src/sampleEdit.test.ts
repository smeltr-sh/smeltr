import { describe, expect, it } from 'vitest';
import type { SampleDto } from './api';
import { pastedTable } from './capture';
import {
  cellMistyped,
  cellText,
  editTable,
  matches,
  parseCell,
  pastedRows,
  withCell,
} from './sampleEdit';

// A paste: one column of whole records.
const kafka: SampleDto = {
  id: 'k',
  name: 'orders',
  source: 'orders-west',
  ...pastedTable([
    { _raw: 'a', __source: { topic: 'o', partition: 0, offset: 7 } },
    { _raw: 'b', __source: { topic: 'o', partition: 1, offset: 9, bytes: btoa('b') } },
  ]),
};

// A capture, as it travels: `status` is the same on every row, the first row's `amount` failed, and no row has a
// `refund`.
const sql: SampleDto = {
  id: 's',
  name: 'invoices',
  source: 'billing-db',
  columns: [
    { name: 'invoice_id', type: 'i64' },
    { name: 'status', type: 'str', value: 'open' },
    { name: 'amount', type: 'f64' },
    { name: 'refund', type: 'f64' },
    { name: 'labels....', type: 'variant' },
  ],
  rows: [
    { values: [1, 'n/a', null, { env: 'prod' }], failed: [1] },
    { values: [2, 3.5, null, null] },
  ],
};

const edit = (sample: SampleDto) => {
  const { columns, rows, shown } = editTable(sample);
  const column = (name: string) => shown.find((c) => c.name === name)!;
  return { columns, rows, shown, column };
};

describe('editTable', () => {
  it("shows each column a row has a value in, and a column of unclaimed fields as a column per field, an internal object's by path", () => {
    expect(edit(sql).shown).toEqual([
      { name: 'invoice_id', type: 'i64', at: 0 },
      { name: 'status', type: 'str', at: 1 },
      { name: 'amount', type: 'f64', at: 2 },
      { name: 'labels.env', at: 4, field: 'env' },
    ]);
    expect(edit(kafka).shown.map((c) => c.name)).toEqual([
      '_raw',
      '__source.topic',
      '__source.partition',
      '__source.offset',
      '__source.bytes',
    ]);
  });

  it('holds every value on every row, a failed cell still its column', () => {
    const { rows, column } = edit(sql);
    expect(rows.map((r) => r.values)).toEqual([
      [1, 'open', 'n/a', null, { env: 'prod' }],
      [2, 'open', 3.5, null, null],
    ]);
    expect(cellText(rows[0], column('amount'))).toBe('n/a');
    expect(cellMistyped(rows[0], column('amount'))).toBe(true);
    expect(cellMistyped(rows[1], column('amount'))).toBe(false);
  });
});

describe('withCell', () => {
  it('fixing a failed value clears it, breaking one fails it, and emptying a typed cell is neither', () => {
    const { rows, shown, column } = edit(sql);
    const fixed = withCell(rows[0], shown, column('amount'), '20');
    expect([fixed.values[2], fixed.failed]).toEqual([20, []]);
    const broken = withCell(rows[1], shown, column('invoice_id'), 'x');
    expect([broken.values[0], broken.failed]).toEqual(['x', [0]]);
    const cleared = withCell(rows[1], shown, column('invoice_id'), '');
    expect([cleared.values[0], cleared.failed]).toEqual([null, []]);
    expect(withCell(rows[1], shown, column('status'), '').values[1]).toBe('');
    expect(rows[1].values[0]).toBe(2);
  });

  it('reads and writes a field inside a held object by its path, and clearing one removes it', () => {
    const { rows, shown, column } = edit(kafka);
    expect(cellText(rows[1], column('__source.offset'))).toBe('9');
    const edited = withCell(rows[1], shown, column('__source.offset'), '12');
    expect(edited.values[0]).toMatchObject({ _raw: 'b', __source: { partition: 1, offset: 12 } });
    const cleared = withCell(rows[0], shown, column('__source.partition'), '');
    expect(cleared.values[0]).toEqual({ _raw: 'a', __source: { topic: 'o', offset: 7 } });
    expect(rows[0].values[0]).toMatchObject({ __source: { partition: 0 } });
  });

  it("an edit to a byte record's text is an edit to the bytes it is read from", () => {
    const { rows, shown, column } = edit(kafka);
    const edited = withCell(rows[1], shown, column('_raw'), 'né');
    expect(edited.values[0]).toMatchObject({ _raw: 'né', __source: { bytes: 'bsOp' } });
  });
});

describe('parseCell', () => {
  it('parses a number column, and keeps text that is not one as mistyped', () => {
    expect(parseCell('12', 'i64', 1)).toEqual({ value: 12, mistyped: false });
    expect(parseCell('1.5', 'i64', 1)).toEqual({ value: '1.5', mistyped: true });
    expect(parseCell('1.5', 'f64', 1)).toEqual({ value: 1.5, mistyped: false });
    expect(parseCell('', 'f64', 1)).toEqual({ value: '', mistyped: true });
  });

  it('keeps an untyped value the kind it had', () => {
    expect(parseCell('12', undefined, 'x')).toEqual({ value: '12', mistyped: false });
    expect(parseCell('12', undefined, 3)).toEqual({ value: 12, mistyped: false });
    expect(parseCell('{"a":1}', undefined, {})).toEqual({ value: { a: 1 }, mistyped: false });
  });
});

describe('pastedRows', () => {
  it('takes only what a pasted record itself holds', () => {
    const columns = [{ name: '__proto__.toString', type: 'str' }];
    expect(pastedRows('{"a":1}', columns, [{ ...columns[0], at: 0 }])).toEqual({
      rows: [{ values: [null] }],
    });
    expect(typeof Object.prototype.toString).toBe('function');
  });

  it('takes a whole record as it is, internal fields and all, and anything else as text', () => {
    const { columns, shown } = edit(kafka);
    const record = '{"_raw":"a","__source":{"offset":9}}';
    expect(pastedRows(`${record}\n{"id":4}\n\nplain`, columns, shown)).toEqual({
      rows: [
        { values: [{ _raw: 'a', __source: { offset: 9 } }] },
        { values: [{ _raw: '{"id":4}' }] },
        { values: [{ _raw: 'plain' }] },
      ],
    });
  });

  it('takes a JSON object a record for a sample of fields, each value under its column', () => {
    const { columns, shown } = edit(sql);
    const pasted = '[{"invoice_id":3,"labels":{"env":"dev"},"extra":1},{"status":"paid"}]';
    expect(pastedRows(pasted, columns, shown)).toEqual({
      rows: [
        { values: [3, null, null, null, { env: 'dev' }] },
        { values: [null, 'paid', null, null, null] },
      ],
    });
    expect(pastedRows('{\n  "invoice_id": 3\n}', columns, shown)).toEqual({
      rows: [{ values: [3, null, null, null, null] }],
    });
    // A field under an object is its column's: the object it leaves empty is no unclaimed field.
    const nested = [
      { name: 'user.name', type: 'str' },
      { name: '...', type: 'variant' },
    ];
    expect(pastedRows('{"user":{"name":"ann","age":3},"extra":1}', nested, [])).toEqual({
      rows: [{ values: ['ann', { extra: 1 }] }],
    });
    expect(pastedRows('{"invoice_id":3}\nnope', columns, shown)).toEqual({
      error: 'line 2 is not a JSON object',
    });
    expect(pastedRows('[{"invoice_id":3}, 4]', columns, shown)).toEqual({
      error: 'record 2 is not a JSON object',
    });
  });
});

describe('matches', () => {
  it('finds a row by any of its values, as its text and not as JSON, ignoring case', () => {
    const { rows } = edit(sql);
    expect(matches(rows[0], 'PROD')).toBe(true);
    expect(matches(rows[1], 'prod')).toBe(false);
    expect(matches(rows[1], ' ')).toBe(true);
    const row = { values: [{ _raw: 'user acme\\mgarcia said "hi"', __source: { offset: 18233 } }] };
    expect(matches(row, 'acme\\mgarcia')).toBe(true);
    expect(matches(row, 'said "hi"')).toBe(true);
    expect(matches(row, '18233')).toBe(true);
  });
});
