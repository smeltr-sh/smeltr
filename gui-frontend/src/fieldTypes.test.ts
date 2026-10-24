import { describe, expect, it } from 'vitest';
import { memberBefore, takes, type FieldTypes } from './fieldTypes';

const fields: FieldTypes = [
  ['_raw', 'str'],
  ['status', 'i64'],
  ['url.path', 'str'],
  ['url.port', 'i64'],
  ['labels["app.name"]', 'str'],
  ['tags', 'array<str>'],
  ['extra.*', 'variant'],
];

/** What comes before the `.` that ends `src`. */
const before = (src: string, f: FieldTypes | undefined = fields) =>
  memberBefore(src, src.length - 1, f);

describe('memberBefore', () => {
  it('reads a leaf field as its type', () => {
    expect(before('_raw.')).toEqual({ kind: 'value', type: 'str' });
    expect(before('x == 1 && .status.')).toEqual({ kind: 'value', type: 'i64' });
  });

  it('reads an object as its fields', () => {
    expect(before('url.')).toEqual({
      kind: 'object',
      fields: [
        { name: 'path', type: 'str' },
        { name: 'port', type: 'i64' },
      ],
    });
  });

  it('reads a dot with nothing before it as the record', () => {
    const member = before('status == 1 && .');
    expect(member?.kind).toBe('object');
    expect(member?.kind === 'object' && member.fields.map((f) => f.name)).toContain('url');
  });

  it('follows a quoted key, an index and a call', () => {
    expect(before('labels["app.name"].')).toEqual({ kind: 'value', type: 'str' });
    expect(before('tags[0].')).toEqual({ kind: 'value', type: 'str' });
    expect(before('_raw.length().')).toEqual({ kind: 'value', type: 'i64' });
    expect(before('url.path.toLowerCase().')).toEqual({ kind: 'value', type: 'str' });
  });

  it('reads a literal', () => {
    expect(before('"a".')).toEqual({ kind: 'value', type: 'str' });
  });

  it('knows nothing of a field it was not told of, or of an open object', () => {
    expect(before('nope.')).toEqual({ kind: 'unknown' });
    expect(before('extra.')).toEqual({ kind: 'unknown' });
    expect(memberBefore('_raw.', 4, undefined)).toEqual({ kind: 'unknown' });
  });

  it('is no member in a string, a comment or a number', () => {
    expect(before('"a.')).toBeNull();
    expect(before('x == 1.')).toBeNull();
  });
});

describe('takes', () => {
  it('says whether a function takes a type before its dot', () => {
    expect(takes('sign', 'str')).toEqual({ fits: false, takes: 'i64/f64' });
    expect(takes('sign', 'i64').fits).toBe(true);
    expect(takes('length', 'array<str>').fits).toBe(true);
    expect(takes('isNull', 'str').fits).toBe(true);
  });

  it('cannot say for a function the signatures do not name', () => {
    expect(takes('keys', 'str').fits).toBeNull();
  });
});
