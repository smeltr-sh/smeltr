import { describe, expect, it } from 'vitest';
import { changesBetween, diffObjects, formatDiffValue, summarizeChanges } from './diff';

describe('diffObjects', () => {
  it('classifies each key as removed, added, unchanged or changed', () => {
    expect(diffObjects({ a: 1 }, {})).toEqual([{ key: 'a', kind: 'removed', before: 1 }]);
    expect(diffObjects({}, { a: 1 })).toEqual([{ key: 'a', kind: 'added', after: 1 }]);
    expect(diffObjects({ a: 1 }, { a: 1 })).toEqual([
      { key: 'a', kind: 'unchanged', before: 1, after: 1 },
    ]);
    expect(diffObjects({ a: 1 }, { a: 2 })).toEqual([
      { key: 'a', kind: 'changed', before: 1, after: 2 },
    ]);
  });

  it('orders before-side keys first, in before order, then after-only keys in after order', () => {
    const result = diffObjects({ a: 1, b: 2 }, { b: 2, c: 3 });
    expect(result.map((e) => e.key)).toEqual(['a', 'b', 'c']);
    expect(result.map((e) => e.kind)).toEqual(['removed', 'unchanged', 'added']);
  });

  it('null or an array on either side is treated as an empty object', () => {
    expect(diffObjects(null, null)).toEqual([]);
    expect(diffObjects(null, { a: 1 })).toEqual([{ key: 'a', kind: 'added', after: 1 }]);
    expect(diffObjects({ a: 1 }, null)).toEqual([{ key: 'a', kind: 'removed', before: 1 }]);
    expect(diffObjects([1, 2, 3], { a: 1 })).toEqual([{ key: 'a', kind: 'added', after: 1 }]);
  });

  it('compares nested values by their whole JSON text, not deeply', () => {
    expect(diffObjects({ a: { x: 1 } }, { a: { x: 1 } })[0].kind).toBe('unchanged');
    expect(diffObjects({ a: { x: 1 } }, { a: { x: 2 } })[0].kind).toBe('changed');
  });
});

describe('formatDiffValue', () => {
  it('renders a string without quotes and anything else as JSON', () => {
    expect(formatDiffValue('hello')).toBe('hello');
    expect(formatDiffValue({ x: 1 })).toBe('{"x":1}');
    expect(formatDiffValue(16)).toBe('16');
  });

  it('quotes a string that would otherwise read as a number, true, false or null', () => {
    expect(formatDiffValue('16')).toBe('"16"');
    expect(formatDiffValue('-3.5e2')).toBe('"-3.5e2"');
    expect(formatDiffValue('true')).toBe('"true"');
    expect(formatDiffValue('null')).toBe('"null"');
    // Not a bare number itself, so it stays unquoted.
    expect(formatDiffValue('16px')).toBe('16px');
  });
});

describe('changesBetween', () => {
  const a = '[{"id":"kafka-1","topics":["a"]},{"id":"kafka-2"}]';

  it('says nothing when the two are the same', () => {
    expect(changesBetween(a, a)).toEqual([]);
  });

  it('names a row that differs, by its id', () => {
    const b = '[{"id":"kafka-1","topics":["b"]},{"id":"kafka-2"}]';
    expect(changesBetween(a, b)).toEqual([
      {
        key: 'kafka-1',
        kind: 'changed',
        before: { id: 'kafka-1', topics: ['a'] },
        after: { id: 'kafka-1', topics: ['b'] },
      },
    ]);
  });

  it('names a row added and a row taken away', () => {
    const b = '[{"id":"kafka-1","topics":["a"]},{"id":"kafka-3"}]';
    expect(changesBetween(a, b).map((e) => [e.key, e.kind])).toEqual([
      ['kafka-2', 'removed'],
      ['kafka-3', 'added'],
    ]);
  });

  /** A row that only moved in the list is not one removed and another added. */
  it('does not read a reorder as a change', () => {
    const b = '[{"id":"kafka-2"},{"id":"kafka-1","topics":["a"]}]';
    expect(changesBetween(a, b)).toEqual([]);
  });

  it('has nothing to say about a value that is not a list of rows', () => {
    expect(changesBetween('"dlq-a"', '"dlq-b"')).toEqual([]);
  });

  it('has nothing to say about text that does not parse', () => {
    expect(changesBetween('{oops', a)).toEqual([]);
  });
});

describe('summarizeChanges', () => {
  const before = '[{"id":"kafka-1","brokers":"b:9092","topics":["orders"]}]';

  /** A source rendered whole is a wall of JSON, and the question is which
   * parts of it move. */
  it('names the fields that differ, not the whole row', () => {
    const after = '[{"id":"kafka-1","brokers":"b:9092","topics":["orders","billing"]}]';
    expect(summarizeChanges(before, after)).toEqual([
      { kind: 'changed', summary: 'kafka-1 — topics' },
    ]);
  });

  it('names every field that differs', () => {
    const after = '[{"id":"kafka-1","brokers":"c:9092","topics":["billing"]}]';
    expect(summarizeChanges(before, after)).toEqual([
      { kind: 'changed', summary: 'kafka-1 — brokers, topics' },
    ]);
  });

  it('needs only a name for a row added or taken away', () => {
    const after = '[{"id":"kafka-1","brokers":"b:9092","topics":["orders"]},{"id":"kafka-2"}]';
    expect(summarizeChanges(before, after)).toEqual([{ kind: 'added', summary: 'kafka-2' }]);
  });
});
