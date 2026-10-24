import { describe, expect, it } from 'vitest';
import {
  currentWord,
  describedRuns,
  inGrokMacroName,
  memberSuggestions,
  rankedSuggestions,
} from './autocomplete';
import { CODE_FUNCTIONS, CONDITION_FUNCTIONS } from './stdlib';
import { GROK_BUILTIN_PATTERNS } from './grokPatterns';

describe('currentWord', () => {
  it('the whole identifier run ending at the cursor', () => {
    expect(currentWord('parseI', 6)).toEqual({ word: 'parseI', start: 0, pos: 6 });
    expect(currentWord('_my_field_2', 11)).toEqual({ word: '_my_field_2', start: 0, pos: 11 });
  });

  it("stops at a non-identifier character, including method-call syntax's dot", () => {
    expect(currentWord('status.toU', 10)).toEqual({ word: 'toU', start: 7, pos: 10 });
  });

  it('a cursor mid-expression sees only the word it is inside', () => {
    expect(currentWord('sta == 200', 3)).toEqual({ word: 'sta', start: 0, pos: 3 });
    expect(currentWord('status == ', 10)).toEqual({ word: '', start: 10, pos: 10 });
  });
});

describe('rankedSuggestions', () => {
  it('an empty word returns the whole list, unfiltered, in its original order', () => {
    expect(rankedSuggestions(CONDITION_FUNCTIONS, '')).toEqual(CONDITION_FUNCTIONS);
  });

  it('ranks every name the word starts ahead of the rest, shorter first', () => {
    const names = rankedSuggestions(CONDITION_FUNCTIONS, 'pars').map((f) => f.name);
    const starts = names.filter((n) => n.startsWith('parse'));
    expect(names.slice(0, starts.length)).toEqual(starts);
    expect(starts.indexOf('parseInt')).toBeLessThan(starts.indexOf('parseUserAgent'));
  });

  it('finds a name by the starts of its words', () => {
    expect(rankedSuggestions(CONDITION_FUNCTIONS, 'pt')[0].name).toBe('parseTime');
    expect(rankedSuggestions(CONDITION_FUNCTIONS, 'int')[0].name).toBe('parseInt');
  });

  it('puts an exact name first', () => {
    expect(rankedSuggestions(CODE_FUNCTIONS, 'json')[0].name).toBe('json');
  });

  it('weighs use a little: it orders close matches, never a weak match over a word start', () => {
    const plain = rankedSuggestions(CONDITION_FUNCTIONS, 'parse').map((f) => f.name);
    const last = plain.at(-1)!;
    const used = rankedSuggestions(CONDITION_FUNCTIONS, 'parse', { usage: new Map([[last, 500]]) });
    expect(used[0].name).toBe(last);
    const weak = rankedSuggestions(CONDITION_FUNCTIONS, 'pt', {
      usage: new Map([['replaceFirst', 500]]),
    });
    expect(weak[0].name).toBe('parseTime');
  });

  it('finds a function whose name merely contains the word', () => {
    const names = rankedSuggestions(CONDITION_FUNCTIONS, 'case').map((f) => f.name);
    expect(names).toContain('toUpperCase');
    expect(names).toContain('toLowerCase');
  });

  it('falls back to a description match when no name contains the word', () => {
    const names = rankedSuggestions(CONDITION_FUNCTIONS, 'integer').map((f) => f.name);
    expect(names).toContain('parseInt');
  });

  it('a word matching nothing returns an empty list', () => {
    expect(rankedSuggestions(CONDITION_FUNCTIONS, 'toInt')).toEqual([]);
  });

  it('matching is case-insensitive', () => {
    expect(rankedSuggestions(CONDITION_FUNCTIONS, 'PARSEINT')[0].name).toBe('parseInt');
  });

  it('an exact name match ranks ahead of a longer name it also prefixes', () => {
    // IP, IPV4 and IPV6 are all prefix matches for "ip"; the exact one
    // must not lose to catalog order among the tied prefix matches.
    expect(rankedSuggestions(GROK_BUILTIN_PATTERNS, 'ip')[0].name).toBe('IP');
    expect(rankedSuggestions(GROK_BUILTIN_PATTERNS, 'time')[0].name).toBe('TIME');
  });

  it('matchDescription: false skips the description-fallback tier', () => {
    // "with" matches no grok macro name, only prose in various descriptions.
    expect(rankedSuggestions(GROK_BUILTIN_PATTERNS, 'with', { matchDescription: false })).toEqual(
      [],
    );
    expect(
      rankedSuggestions(GROK_BUILTIN_PATTERNS, 'with', { matchDescription: true }).length,
    ).toBeGreaterThan(0);
  });
});

describe('inGrokMacroName', () => {
  it('true right after an open brace', () => {
    expect(inGrokMacroName('%{', 2)).toBe(true);
  });

  it('true while typing the macro name', () => {
    expect(inGrokMacroName('%{WO', 4)).toBe(true);
  });

  it('false with no open brace at all', () => {
    expect(inGrokMacroName('allow', 5)).toBe(false);
  });

  it('false once a field name follows the colon', () => {
    expect(inGrokMacroName('%{WORD:ac', 9)).toBe(false);
  });

  it('false past a closed macro, even mid-word', () => {
    expect(inGrokMacroName('%{WORD} allow', 13)).toBe(false);
  });

  it('true again inside a second macro after a closed one', () => {
    expect(inGrokMacroName('%{WORD:action} %{I', 19)).toBe(true);
  });

  it('reads the cursor position, not the end of the string', () => {
    expect(inGrokMacroName('%{WORD} and %{IP', 3)).toBe(true);
  });
});

describe('describedRuns', () => {
  it('marks code and reads a link as its text', () => {
    expect(describedRuns('The `text` as [JSON](/docs/x).')).toEqual([
      { text: 'The ', code: false },
      { text: 'text', code: true },
      { text: ' as JSON.', code: false },
    ]);
  });
});

describe('memberSuggestions', () => {
  const fn = (name: string, signature: string) => ({ name, signature, description: '' });
  const functions = [
    fn('sign', 'value.sign()'),
    fn('length', 'value.length()'),
    fn('keys', 'object.keys()'),
    fn('now', 'now()'),
  ];

  it('offers what takes the value first, and the rest after it, warned', () => {
    const got = memberSuggestions({ kind: 'value', type: 'str' }, functions, '');
    expect(got.map((f) => [f.name, f.warning])).toEqual([
      ['length', undefined],
      ['sign', 'works on i64/f64, not str'],
      ['keys', 'works on objects, not str'],
    ]);
  });

  it('names only the mismatch when a function works on many types', () => {
    const got = memberSuggestions(
      { kind: 'value', type: 'array<str>' },
      [fn('toString', 'value.toString()')],
      '',
    );
    expect(got[0].warning).toBe("doesn't work on array<str>");
  });

  it("offers an object's fields first", () => {
    const got = memberSuggestions(
      { kind: 'object', fields: [{ name: 'path', type: 'str' }] },
      functions,
      '',
    );
    expect(got.map((f) => [f.name, !!f.field, !!f.warning])).toEqual([
      ['path', true, false],
      ['keys', false, false],
      ['sign', false, true],
      ['length', false, true],
    ]);
  });

  it('keeps a misfit below every fit, however well it matches the word', () => {
    const got = memberSuggestions({ kind: 'value', type: 'str' }, functions, 'n');
    expect(got.map((f) => f.name)).toEqual(['length', 'sign']);
  });

  it('offers every function written on a value where the type is unknown', () => {
    const got = memberSuggestions({ kind: 'unknown' }, functions, '');
    expect(got.map((f) => f.name)).toEqual(['sign', 'length', 'keys']);
    expect(got.every((f) => !f.warning)).toBe(true);
  });
});
