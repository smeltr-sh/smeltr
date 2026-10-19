import { describe, expect, it } from 'vitest';
import { holdsName, markRuns, quotedTokens } from './errorMarks';

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
