import { describe, expect, it } from 'vitest';
import { countCalls } from './usage.svelte';

describe('countCalls', () => {
  it('counts each call in every string the steps hold, method calls too', () => {
    const counts = countCalls([
      {
        id: 'p',
        name: 'p',
        steps: [
          { type: 'eval', filter: 'true', assignments: [{ target: 't', expr: 'parseTime(ts)' }] },
          { type: 'code', body: 'x = json(_raw);\ny = x.msg.toUpperCase();\nz = parseTime(a);' },
        ],
      },
    ]);
    expect(counts.get('parseTime')).toBe(2);
    expect(counts.get('json')).toBe(1);
    expect(counts.get('toUpperCase')).toBe(1);
    expect(counts.has('ts')).toBe(false);
  });
});
