import { describe, expect, it } from 'vitest';
import { ASSIGNMENT_FUNCTIONS, CODE_FUNCTIONS, CONDITION_FUNCTIONS } from './stdlib';

const names = (list: { name: string }[]) => list.map((f) => f.name);

describe('the suggested names', () => {
  it('a condition has the documented functions and the object functions', () => {
    for (const name of ['parseTime', 'toUpperCase', 'keys', 'omit']) {
      expect(names(CONDITION_FUNCTIONS)).toContain(name);
      expect(names(ASSIGNMENT_FUNCTIONS)).toContain(name);
      expect(names(CODE_FUNCTIONS)).toContain(name);
    }
  });

  it('an assignment adds the parse functions, and map and filter', () => {
    for (const name of ['json', 'kv', 'cef', 'csv', 'grok', 'lookup', 'map', 'filter']) {
      expect(names(ASSIGNMENT_FUNCTIONS)).toContain(name);
      expect(names(CODE_FUNCTIONS)).toContain(name);
      expect(names(CONDITION_FUNCTIONS)).not.toContain(name);
    }
  });

  it('a Code body adds the statements written as a call', () => {
    for (const name of ['drop', 'delete', 'move', 'unroll']) {
      expect(names(CODE_FUNCTIONS)).toContain(name);
      expect(names(ASSIGNMENT_FUNCTIONS)).not.toContain(name);
    }
  });

  it('names each once', () => {
    for (const list of [CONDITION_FUNCTIONS, ASSIGNMENT_FUNCTIONS, CODE_FUNCTIONS]) {
      expect(new Set(names(list)).size).toBe(list.length);
    }
  });
});
