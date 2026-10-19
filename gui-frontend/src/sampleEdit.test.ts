import { describe, expect, it } from 'vitest';
import type { SampleDto } from './api';
import {
  cellMistyped,
  cellText,
  editColumns,
  matches,
  parseCell,
  pastedRecords,
  readFrom,
  readFromFields,
  withCell,
} from './sampleEdit';

const kafka: SampleDto = {
  id: 'k',
  name: 'orders',
  source: 'orders-west',
  records: [
    {
      _raw: 'a',
      __source: { name: 'orders-west', type: 'kafka', topic: 'o', partition: 0, offset: 7 },
    },
    {
      _raw: 'b',
      __source: { name: 'orders-west', type: 'kafka', topic: 'o', partition: 1, offset: 9 },
    },
  ],
};

const sql: SampleDto = {
  id: 's',
  name: 'invoices',
  source: 'billing-db',
  columns: [
    { name: 'invoice_id', type: 'i64' },
    { name: 'amount', type: 'f64' },
    { name: 'status', type: 'str' },
    { name: '__source.database', type: 'str' },
  ],
  records: [
    {
      invoice_id: 1,
      amount: 'n/a',
      status: 'paid',
      smeltr: { mistyped: ['amount'] },
      __source: { name: 'billing-db', type: 'postgres', database: 'billing' },
    },
    {
      invoice_id: 2,
      amount: 3.5,
      status: 'open',
      __source: { name: 'billing-db', type: 'postgres', database: 'billing' },
    },
  ],
};

describe('editColumns', () => {
  it('shows a record its own fields, typed by the capture, and never its source or mistyped list', () => {
    expect(editColumns(sql)).toEqual([
      { name: 'invoice_id', type: 'i64' },
      { name: 'amount', type: 'f64' },
      { name: 'status', type: 'str' },
    ]);
    expect(editColumns(kafka)).toEqual([{ name: '_raw', type: undefined }]);
  });

  it('keeps a tag field that holds more than the mistyped list', () => {
    const at = { ...sql, records: [{ a: 1, smeltr: { mistyped: ['a'], message: 'x' } }] };
    expect(editColumns(at).map((c) => c.name)).toEqual(['a', 'smeltr']);
  });
});

describe('read from', () => {
  it('names the source fields that tell records apart, and only those', () => {
    expect(readFromFields(kafka)).toEqual(['partition', 'offset']);
    expect(readFrom(kafka.records[1], ['partition', 'offset'])).toBe('1 · 9');
  });

  it('keeps the order the capture gave its columns, which a stored record does not', () => {
    const sorted = {
      ...sql,
      columns: [{ name: 'status', type: 'str' }, ...sql.columns!],
      records: [{ amount: 1, invoice_id: 1, status: 'x' }],
    };
    expect(editColumns(sorted).map((c) => c.name)).toEqual(['status', 'invoice_id', 'amount']);
    const captured = {
      ...kafka,
      columns: [
        { name: '__source.partition', type: 'i64' },
        { name: '__source.offset', type: 'i64' },
      ],
      records: kafka.records.map(({ _raw, __source }) => ({
        _raw,
        __source: Object.fromEntries(Object.entries(__source as object).sort()),
      })),
    };
    expect(readFromFields(captured)).toEqual(['partition', 'offset']);
  });

  it('never shows the bytes a byte record was read as', () => {
    const withBytes = {
      ...kafka,
      records: kafka.records.map((r, i) => ({
        ...r,
        __source: { ...(r.__source as object), bytes: btoa(String(i)) },
      })),
    };
    expect(readFromFields(withBytes)).toEqual(['partition', 'offset']);
  });

  it('is nothing for a SQL sample, whose rows share one source', () => {
    expect(readFromFields(sql)).toEqual([]);
  });

  it('is empty for a pasted record', () => {
    expect(readFrom({ _raw: 'x' }, ['offset'])).toBe('');
  });
});

describe('cells', () => {
  it('reads a mistyped value as mistyped', () => {
    expect(cellText(sql.records[0], 'amount')).toBe('n/a');
    expect(cellMistyped(sql.records[0], 'amount')).toBe(true);
    expect(cellMistyped(sql.records[1], 'amount')).toBe(false);
  });

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

  it('fixing a mistyped value takes it off the mistyped list, and keeps where it was read from', () => {
    const fixed = withCell(sql.records[0], { name: 'amount', type: 'f64' }, '20');
    expect(fixed.amount).toBe(20);
    expect(fixed.smeltr).toBeUndefined();
    expect(fixed.__source).toEqual(sql.records[0].__source);
  });

  it("an edit to a byte record's text is an edit to the bytes it is read from", () => {
    const record = { _raw: 'old', __source: { partition: 0, offset: 7, bytes: btoa('old') } };
    const edited = withCell(record, { name: '_raw', type: 'str' }, 'né');
    expect(edited._raw).toBe('né');
    expect(edited.__source).toEqual({ partition: 0, offset: 7, bytes: 'bsOp' });
  });

  it('breaking a value puts it on the mistyped list', () => {
    const broken = withCell(sql.records[1], { name: 'invoice_id', type: 'i64' }, 'x');
    expect(broken.invoice_id).toBe('x');
    expect(broken.smeltr).toEqual({ mistyped: ['invoice_id'] });
  });

  it('emptying a typed cell removes the field, and does not list it as mistyped', () => {
    const cleared = withCell(sql.records[1], { name: 'invoice_id', type: 'i64' }, '');
    expect('invoice_id' in cleared).toBe(false);
    expect(cleared.smeltr).toBeUndefined();
  });

  it('leaves a record field named like the tag field alone', () => {
    const own = withCell({ n: 1, smeltr: 'abc' }, { name: 'n', type: 'i64' }, 'x');
    expect(own.smeltr).toBe('abc');
  });

  it('a string cell may be emptied', () => {
    expect(withCell(sql.records[1], { name: 'status', type: 'str' }, '').status).toBe('');
  });
});

describe('pastedRecords', () => {
  it('takes a line a record for a sample of text', () => {
    expect(pastedRecords('one\n\ntwo\n', [{ name: '_raw' }])).toEqual({
      records: [{ _raw: 'one' }, { _raw: 'two' }],
    });
  });

  it('takes a JSON object a line for a sample of fields, and names a line that is not one', () => {
    const columns = editColumns(sql);
    expect(pastedRecords('{"invoice_id":3}', columns)).toEqual({ records: [{ invoice_id: 3 }] });
    expect(pastedRecords('{"invoice_id":3}\nnope', columns)).toEqual({
      error: 'line 2 is not a JSON object',
    });
  });

  it('takes an array or a pretty-printed object of fields, as Add sample takes a paste', () => {
    const columns = editColumns(sql);
    expect(pastedRecords('[{"invoice_id":3},{"invoice_id":4}]', columns)).toEqual({
      records: [{ invoice_id: 3 }, { invoice_id: 4 }],
    });
    expect(pastedRecords('{\n  "invoice_id": 3\n}', columns)).toEqual({
      records: [{ invoice_id: 3 }],
    });
    expect(pastedRecords('[{"invoice_id":3}, 4]', columns)).toEqual({
      error: 'record 2 is not a JSON object',
    });
  });
});

describe('matches', () => {
  it('finds a record by any of its values, ignoring case', () => {
    expect(matches(sql.records[1], 'OPEN')).toBe(true);
    expect(matches(sql.records[1], 'paid')).toBe(false);
    expect(matches(sql.records[1], ' ')).toBe(true);
  });
});
