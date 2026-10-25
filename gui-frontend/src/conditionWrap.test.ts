import { describe, expect, it } from 'vitest';
import { wrapCondition, wrapWords } from './conditionWrap';

describe('wrapCondition', () => {
  it('leaves a condition that fits on one line', () => {
    expect(wrapCondition('status >= 400', 20)).toEqual(['status >= 400']);
  });

  it('breaks before an operator, packing what fits', () => {
    expect(wrapCondition('a == 1 && b == 2 && c == 3', 17)).toEqual([
      'a == 1 && b == 2',
      '&& c == 3',
    ]);
  });

  it('keeps a bracketed group on one line where it fits', () => {
    const src = '.action == "deny" && (.level == "warning" || .level == "alert") && .wan';
    expect(wrapCondition(src, 45)).toEqual([
      '.action == "deny"',
      '&& (.level == "warning" || .level == "alert")',
      '&& .wan',
    ]);
  });

  it('breaks inside a group only when it cannot fit, at its own operators', () => {
    expect(wrapCondition('x && (aaaa == 1 || bbbb == 2)', 16)).toEqual([
      'x',
      '&& (aaaa == 1',
      '|| bbbb == 2)',
    ]);
  });

  it('never breaks inside a string', () => {
    expect(wrapCondition('msg == "a && b && c" || x', 12)).toEqual([
      'msg == "a && b && c"',
      '|| x',
    ]);
  });

  it('keeps a run with no operator whole', () => {
    expect(wrapCondition('starts_with(message, "a very long prefix")', 10)).toEqual([
      'starts_with(message, "a very long prefix")',
    ]);
  });

  it('gives the text back, but for the spaces at a break', () => {
    const src = 'a == 1 || (b == 2 && (c == 3 || d == 4)) && e != null';
    for (const cols of [4, 10, 20, 40, 80]) {
      expect(wrapCondition(src, cols).join(' ')).toBe(src);
    }
  });
});

describe('wrapWords', () => {
  it('packs words into lines at spaces, and cuts a word longer than a line', () => {
    expect(wrapWords('%{IP:a} %{WORD:b} %{INT:c}', 17)).toEqual(['%{IP:a} %{WORD:b}', '%{INT:c}']);
    expect(wrapWords('abcdefghij k', 4)).toEqual(['abcd', 'efgh', 'ij k']);
  });
});
