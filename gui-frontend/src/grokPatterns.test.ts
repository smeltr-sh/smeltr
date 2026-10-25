import { describe, expect, it } from 'vitest';
import {
  builtinRegex,
  GROK_BUILTIN_PATTERNS,
  grokNameProblem,
  sharedGrokSuggestions,
} from './grokPatterns';

describe('grokNameProblem', () => {
  it('refuses what the leader refuses, and takes the rest', () => {
    expect(grokNameProblem('VENDOR_1', ['OTHER'])).toBeNull();
    expect(grokNameProblem('', [])).not.toBeNull();
    expect(grokNameProblem('MY-NAME', [])).not.toBeNull();
    expect(grokNameProblem('WORD', [])).toContain('built-in');
    expect(grokNameProblem('OTHER', ['OTHER'])).toContain('taken');
  });
});

describe('sharedGrokSuggestions', () => {
  it('suggests each named pattern by its definition, and skips one still unnamed', () => {
    expect(
      sharedGrokSuggestions([
        { id: 'VENDOR', pattern: 'acme-%{INT}' },
        { id: '', pattern: 'x' },
      ]),
    ).toEqual([{ name: 'VENDOR', signature: '%{VENDOR}', description: 'acme-%{INT}' }]);
  });
});

describe('builtinRegex', () => {
  it('reads the regex every built-in quotes, one holding a backtick whole', () => {
    const named = (n: string) => GROK_BUILTIN_PATTERNS.find((b) => b.name === n)!;
    expect(builtinRegex(named('WORD'))).toBe('\\b\\w+\\b');
    expect(builtinRegex(named('COMMONAPACHELOG'))).toMatch(/^%\{IPORHOST:source\.address\} .*`.*-\)$/);
    expect(GROK_BUILTIN_PATTERNS.filter((b) => builtinRegex(b) === null)).toEqual([]);
    expect(builtinRegex({ name: 'X', signature: '%{X}', description: 'no regex' })).toBeNull();
  });
});
