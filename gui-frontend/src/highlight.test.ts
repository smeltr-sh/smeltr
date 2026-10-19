import { describe, expect, it } from 'vitest';
import {
  CONTEXTUAL_KEYWORDS,
  KEYWORDS,
  LITERALS,
  highlight,
  tokenize,
} from '../../website/src/lib/highlight/highlight';
import lexer from '../../src/expr/lex.rs?raw';
import parser from '../../src/expr/parse.rs?raw';

const words = (src: string, re: RegExp) => [...src.matchAll(re)].map((m) => m[m.length - 1]);

describe("the highlighter knows the engine's words", () => {
  // A new keyword in the engine fails this until it is colored here too.
  it("the lexer's keywords", () => {
    const lexed = words(lexer, /\("(\w+)", Keyword::/g);
    expect(lexed.length).toBeGreaterThan(0);
    expect(new Set(lexed)).toEqual(new Set([...KEYWORDS, ...LITERALS].filter((w) => w !== 'of')));
  });

  it('the words the parser takes as keywords in one place', () => {
    const code = parser.split('#[cfg(test)]')[0];
    const contextual = [
      ...words(code, /Token::Ident\((\w+)\)[^;{]{0,60}?\bif \1 == "(\w+)"/g),
      ...words(code, /at_word\("(\w+)"\)/g),
    ];
    expect(new Set(contextual)).toEqual(new Set(['of', ...Object.keys(CONTEXTUAL_KEYWORDS)]));
  });
});

const kinds = (src: string) =>
  tokenize(src)
    .filter((t) => t.kind !== 'plain')
    .map((t) => `${t.kind}:${t.text}`);

describe('tokenize', () => {
  it('gives the source back unchanged', () => {
    const src = 'let n: i64 = 1; s = `a${n}` + "b"; // done';
    expect(
      tokenize(src)
        .map((t) => t.text)
        .join(''),
    ).toBe(src);
  });

  it('colors a type name only where a type goes', () => {
    expect(kinds('x: duration = d')).toEqual(['type:duration']);
    expect(kinds('y = n as f64')).toEqual(['keyword:as', 'type:f64']);
    expect(kinds('t: array<str>')).toEqual(['type:array', 'type:str']);
    expect(kinds('ms = duration * 1000')).toEqual(['number:1000']);
    expect(kinds('c ? 1 : duration')).toEqual(['number:1']);
    expect(kinds('x = n < str')).toEqual([]);
    expect(kinds('{"d": duration}')).toEqual(['key:"d"']);
    expect(kinds('x = c ? 1;\ny: duration = 2')).toEqual(['number:1', 'type:duration', 'number:2']);
    expect(kinds('labels: {region: str, ...} = {region: str}')).toEqual(['type:str']);
    expect(kinds('a: array<{n: i64}> = []')).toEqual(['type:array', 'type:i64']);
    expect(kinds('function f(a: str): i64 { return 1; }')).toEqual([
      'keyword:function',
      'function:f',
      'type:str',
      'type:i64',
      'keyword:return',
      'number:1',
    ]);
    expect(kinds('duration(1, "s")')).toEqual(['function:duration', 'number:1', 'string:"s"']);
  });

  it('a string before a ternary colon is a string, not a key', () => {
    expect(kinds('a ? "ok" : "bad"')).toEqual(['string:"ok"', 'string:"bad"']);
    expect(kinds('{"a": 1}')).toEqual(['key:"a"', 'number:1']);
    expect(kinds('c ? {"k": 1} : 2')).toEqual(['key:"k"', 'number:1', 'number:2']);
    expect(kinds('x ?? "y"')).toEqual(['string:"y"']);
  });

  it('a slash after a value divides; elsewhere it starts a regex', () => {
    expect(kinds('a / 2')).toEqual(['number:2']);
    expect(kinds('test(p, /^a\\//)')).toEqual(['function:test', 'regex:/^a\\//']);
  });
});

describe('highlight', () => {
  it('escapes HTML', () => {
    expect(highlight('a < "<b>"')).toBe('a &lt; <span class="tok-string">"&lt;b&gt;"</span>');
  });
});

describe('contextual keywords', () => {
  const keywords = (src: string) =>
    tokenize(src)
      .filter((t) => t.kind === 'keyword')
      .map((t) => t.text);

  it('colors try, catch, finally and throw where they start their construct', () => {
    expect(keywords('try { a = 1; } catch (e: NaN) { throw e; } finally { b = 2; }')).toEqual([
      'try',
      'catch',
      'throw',
      'finally',
    ]);
  });

  it('colors switch, case and default in a switch', () => {
    expect(keywords('switch (n) { case 1: a = 1; break; default: a = 2; }')).toEqual([
      'switch',
      'case',
      'break',
      'default',
    ]);
  });

  it('leaves fields with those names alone', () => {
    expect(
      keywords('try = 1; case = catch + finally; x = { default: throw }; default = 2;'),
    ).toEqual([]);
    expect(keywords('switch (n) { case 1: case = 2; }')).toEqual(['switch', 'case']);
  });
});
