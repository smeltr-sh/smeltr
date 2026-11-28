import { describe, expect, it } from 'vitest';
import { holdsName, laidSpanRuns, markRuns, quotedTokens, spanRuns } from './errorMarks';

describe('quotedTokens', () => {
  it('takes every backticked name', () => {
    expect(quotedTokens('`ipVersion` takes 1 argument, found 0')).toEqual(['ipVersion']);
    expect(quotedTokens('`a` and `b.c`')).toEqual(['a', 'b.c']);
    expect(quotedTokens('no names here')).toEqual([]);
  });
});

describe('markRuns', () => {
  const marked = (text: string, tokens: string[]) =>
    markRuns(text, tokens)
      .filter((r) => r.mark)
      .map((r) => r.text);

  it('marks every occurrence and joins back into the text', () => {
    const text = 'x = ipVersion();\ny = ipVersion(a);';
    const runs = markRuns(text, ['ipVersion']);
    expect(runs.map((r) => r.text).join('')).toBe(text);
    expect(marked(text, ['ipVersion'])).toEqual(['ipVersion', 'ipVersion']);
  });

  it('marks nothing for a token with no name in it', () => {
    expect(marked('u = a.b;\nx = u.', ['.'])).toEqual([]);
  });

  it('takes the longest token where two start at one place', () => {
    expect(marked('a.b = 1', ['a', 'a.b'])).toEqual(['a.b']);
  });

  it('marks a name only where it stands whole', () => {
    expect(marked('x = max(ex, index) + x', ['x'])).toEqual(['x', 'x']);
    expect(marked('a.b = $x', ['$x'])).toEqual(['$x']);
  });

  it('marks nothing when no token is in the text', () => {
    expect(markRuns('x = 1', ['y'])).toEqual([{ text: 'x = 1', mark: false }]);
  });
});

describe('holdsName', () => {
  it('finds a name only where it stands whole', () => {
    expect(holdsName('max(ex)', 'x')).toBe(false);
    expect(holdsName('max(x)', 'x')).toBe(true);
    expect(holdsName('a.b = $x', '$x')).toBe(true);
  });
});

describe('spanRuns', () => {
  const marked = (runs: { text: string; mark: boolean }[]) =>
    runs.filter((r) => r.mark).map((r) => r.text);

  it('marks the bytes the compiler names, counting UTF-8 as it does', () => {
    const text = 'seen = (n + ;';
    expect(marked(spanRuns(text, { start: 12, end: 13 }))).toEqual([';']);
    expect(
      spanRuns(text, { start: 12, end: 13 })
        .map((r) => r.text)
        .join(''),
    ).toBe(text);
    // `é` is two bytes and one unit, so the `)` after it is at byte 6 and unit 5.
    expect(marked(spanRuns('("é" )', { start: 6, end: 7 }))).toEqual([')']);
  });

  it('marks the last character for an empty span past the text', () => {
    expect(marked(spanRuns('1 +\n', { start: 3, end: 3 }))).toEqual(['+']);
  });

  it('marks the same characters where the text is laid out in lines', () => {
    const text = 'a == 1 && b ==)';
    expect(marked(laidSpanRuns(text, 'a == 1\n&& b ==)', { start: 14, end: 15 }))).toEqual([')']);
  });
});
