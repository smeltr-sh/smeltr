import { describe, expect, it } from 'vitest';
import { firstOf, matching, SchemaTree, typedPath } from './schemaTree';
import type { Requirement, SchemaDto } from './api';

const schema = (id: string, fields: [string, string, Requirement?][]): SchemaDto => ({
  id,
  fields: fields.map(([field, type, requirement]) => ({ field, type, requirement })),
});

const ecs = schema('ecs', [
  ['@timestamp', 'datetime', 'required'],
  ['destination.ip', 'str', 'recommended'],
  ['destination.port', 'i64'],
  ['destination.bytes', 'i64', 'recommended'],
  ['destination.address', 'str'],
  ['destination.geo.city_name', 'str'],
  ['destination.geo.location', '{...}', 'recommended'],
  ['destination.as.number', 'i64'],
  ['message', 'str', 'recommended'],
]);

const names = (cs: { name: string }[]) => cs.map((c) => c.name);

describe('SchemaTree', () => {
  it('lists what is asked for first, then objects holding it, then optional fields, then the rest', () => {
    const tree = new SchemaTree([ecs]);
    expect(names(tree.children('destination'))).toEqual([
      'bytes',
      'ip',
      'geo',
      'address',
      'port',
      'as',
    ]);
  });

  it('lists what the record holds last, marked set', () => {
    const tree = new SchemaTree([ecs]);
    const children = tree.children('destination', new Set(['destination.bytes']));
    expect(children.at(-1)).toMatchObject({ name: 'bytes', set: true });
  });

  it('counts the fields under an object, and what of them is asked for', () => {
    const tree = new SchemaTree([ecs]);
    expect(tree.children('').find((c) => c.name === 'destination')).toMatchObject({
      nested: 7,
      recommended: 3,
      required: 0,
    });
    expect(tree.nestedUnder('destination.geo')).toBe(2);
    expect(tree.nestedUnder('')).toBe(9);
  });

  it('holds a field two schemas declare to the stricter requirement', () => {
    const tree = new SchemaTree([ecs, schema('mine', [['destination.port', 'i64', 'required']])]);
    expect(tree.requirement('destination.port')).toBe('required');
    expect(tree.total).toBe(9);
    expect(tree.titles).toBe('ecs, mine');
  });

  it('tallies what the record holds of each requirement', () => {
    const tree = new SchemaTree([ecs]);
    expect(tree.tally(new Set(['@timestamp', 'destination.port', 'other']))).toEqual({
      required: { set: 1, of: 1 },
      recommended: { set: 0, of: 4 },
      optional: { set: 1, of: 4 },
    });
  });

  it('reads a search naming an object as the fields under it', () => {
    const tree = new SchemaTree([ecs]);
    expect(tree.at('destination')).toEqual({ parent: 'destination', key: '' });
    expect(tree.at('destination.ge')).toEqual({ parent: 'destination', key: 'ge' });
    expect(tree.at('mess')).toEqual({ parent: '', key: 'mess' });
  });
});

describe('typedPath', () => {
  it('splits a dotted path at its last dot', () => {
    expect(typedPath('')).toEqual({ parent: '', key: '' });
    expect(typedPath('destination.')).toEqual({ parent: 'destination', key: '' });
    expect(typedPath('destination.geo.ci')).toEqual({ parent: 'destination.geo', key: 'ci' });
  });

  it('reads no path in an expression or an empty key', () => {
    expect(typedPath('a + b')).toBeNull();
    expect(typedPath('a[0]')).toBeNull();
    expect(typedPath('a..b')).toBeNull();
  });
});

describe('matching', () => {
  it('puts keys starting with the text before keys holding it', () => {
    const tree = new SchemaTree([ecs]);
    expect(names(matching(tree.children('destination'), 'b'))).toEqual(['bytes']);
    expect(names(matching(tree.children('destination'), 'p'))).toEqual(['port', 'ip']);
    expect(names(matching(tree.children('destination'), 'A'))).toEqual(['address', 'as']);
  });
});

describe('firstOf', () => {
  it('names a few objects of a group and fills the list with fields', () => {
    const many = schema('many', [
      ...['a', 'b', 'c', 'd', 'e'].map((o): [string, string, Requirement] => [
        `${o}.x`,
        'str',
        'recommended',
      ]),
      ['f', 'str'],
      ['g', 'str'],
    ]);
    const tree = new SchemaTree([many]);
    expect(names(firstOf(tree.children(''), 5))).toEqual(['a', 'b', 'c', 'f', 'g']);
    expect(names(firstOf(tree.children(''), 7))).toEqual(['a', 'b', 'c', 'd', 'e', 'f', 'g']);
  });
});
