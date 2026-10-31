import { describe, expect, it } from 'vitest';
import type { DestinationDto } from './api';
import {
  BUILT_IN,
  CATEGORIES,
  KIND_HELP,
  kindsOf,
  lay,
  ownAnswer,
  possible,
  problems,
  set,
  withCategories,
  withTagField,
  type Answer,
  type ExceptionsDto,
} from './exceptions';

describe('laying an exceptions layer', () => {
  it('answers each kind by its kind, then its category', () => {
    const answers = lay(BUILT_IN, { categories: { math: 'drop' }, kinds: { NaN: 'ignore' } });
    expect(answers.Overflow).toBe('drop');
    expect(answers.NaN).toBe('ignore');
    expect(answers.Unparsable).toBe('tag');
    expect(answers.GuessedEncoding).toBe('ignore');
    expect(answers.Refused).toBe('block');
    expect(lay(BUILT_IN, undefined)).toEqual(BUILT_IN);
  });
});

describe('answering a row of the matrix', () => {
  it('sets a category and clears its own kinds only', () => {
    const dto = set({ kinds: { NaN: 'block', Unparsable: 'drop' } }, 'math', 'drop');
    expect(dto).toEqual({ categories: { math: 'drop' }, kinds: { Unparsable: 'drop' } });
  });

  it('sets a kind alone, and unsets a row without touching those under it', () => {
    const dto = set({ categories: { math: 'drop' } }, 'NaN', 'block');
    expect(dto).toEqual({ categories: { math: 'drop' }, kinds: { NaN: 'block' } });
    expect(set(dto, 'math', undefined)).toEqual({ kinds: { NaN: 'block' } });
    expect(set({ kinds: { NaN: 'drop' } }, 'NaN', undefined)).toBeUndefined();
    expect(ownAnswer(dto, 'NaN')).toBe('block');
  });

  it('writes names in order, and keeps the tag field', () => {
    const dto = set(
      { categories: { encoding: 'tag' }, kinds: { NaN: 'drop' }, tagField: 'why' },
      'parse',
      'drop',
    );
    expect(JSON.stringify(dto)).toBe(
      '{"categories":{"encoding":"tag","parse":"drop"},"kinds":{"NaN":"drop"},"tagField":"why"}',
    );
  });

  it('writes an edit undone as it read', () => {
    const read = { kinds: { NaN: 'drop', Overflow: 'drop' } } as const;
    expect(JSON.stringify(set(set(read, 'NaN', undefined), 'NaN', 'drop'))).toBe(
      JSON.stringify(read),
    );
  });

  it('names a tag field, and an empty one names none', () => {
    expect(withTagField(undefined, 'why')).toEqual({ tagField: 'why' });
    expect(withTagField({ tagField: 'why' }, '')).toBeUndefined();
  });

  it('answers every category of the group, keeping what it set', () => {
    const group = withCategories({ categories: { math: 'drop' }, kinds: { NaN: 'block' } });
    expect(Object.keys(group?.categories ?? {})).toEqual([...CATEGORIES].sort());
    expect(group?.categories?.math).toBe('drop');
    expect(group?.categories?.delivery).toBe('block');
    expect(group?.kinds).toEqual({ NaN: 'block' });
  });

  it('never ignores or tags a delivery kind', () => {
    expect(possible('Refused', 'tag')).toBe(false);
    expect(possible('delivery', 'ignore')).toBe(false);
    expect(possible('Refused', 'drop')).toBe(true);
    expect(possible('NaN', 'tag')).toBe(true);
  });

  it('explains every kind', () => {
    expect(Object.keys(KIND_HELP)).toEqual(CATEGORIES.flatMap(kindsOf).sort(order));
  });
});

const order = (a: string, b: string) =>
  Object.keys(KIND_HELP).indexOf(a) - Object.keys(KIND_HELP).indexOf(b);

function kafka(id: string, exceptions?: ExceptionsDto): DestinationDto {
  return { id, type: 'kafka', brokers: 'b:9092', defaultTopic: 't', exceptions };
}
const refusedTo = (...deliver: string[]): ExceptionsDto => ({
  kinds: { Refused: { deliver } },
});
const elastic = (id: string): DestinationDto => ({
  id,
  type: 'elasticsearch',
  url: 'http://es:9200',
  defaultIndex: 'i',
});
describe('problems', () => {
  it('names a delivery to nowhere', () => {
    const group: ExceptionsDto = { kinds: { Unmatched: { deliver: [] } } };
    const routes = [
      { name: 's \u2192 p', exceptions: { categories: { parse: { deliver: ['es'] } } } },
    ];
    expect(problems(group, routes, [elastic('es')])).toEqual([
      'the group delivers Unmatched to no destination',
    ]);
  });

  it('checks a category only where a kind still takes its answer', () => {
    const toGone: Answer = { deliver: ['gone'] };
    const kinds = Object.fromEntries(kindsOf('parse').map((k) => [k, 'drop' as const]));
    expect(problems({ categories: { parse: toGone }, kinds }, [], [elastic('es')])).toEqual([]);
    expect(problems({ categories: { parse: toGone } }, [], [elastic('es')])).toEqual([
      'the group delivers parse to gone, which does not exist',
    ]);
  });

  it('names a delivery to a destination that does not exist', () => {
    expect(problems(refusedTo('gone'), [], [kafka('a', { kinds: { Refused: 'drop' } })])).toEqual([
      'the group delivers Refused to gone, which does not exist',
    ]);
  });

  it('names refusals that lead back to where they started', () => {
    expect(
      problems(undefined, [], [kafka('a', refusedTo('b')), kafka('b', refusedTo('a'))]),
    ).toEqual(['refusals at a lead back to it', 'refusals at b lead back to it']);
    expect(problems(refusedTo('a'), [], [kafka('a')])).toEqual(['refusals at a lead back to it']);
    expect(problems(undefined, [], [kafka('a', refusedTo('b')), kafka('b')])).toEqual([]);
  });
});
