import { describe, expect, it } from 'vitest';
import type { Framing } from './api';
import { setFraming } from './sources';

type Framed = { framing?: Framing; lineBreaker?: string };

describe('an object source framing', () => {
  it('writes the framing by name, and nothing for lines', () => {
    const d: Framed = {};
    setFraming(d, 'object');
    expect(d).toEqual({ framing: 'object' });
    setFraming(d, 'lines');
    expect(d).toEqual({});
  });

  it('keeps the line breaker for lines, and drops it for another framing', () => {
    const d: Framed = { lineBreaker: '([\\r\\n]+)\\d{4}-' };
    setFraming(d, 'lines');
    expect(d).toEqual({ lineBreaker: '([\\r\\n]+)\\d{4}-' });
    setFraming(d, 'escapedLines');
    expect(d).toEqual({ framing: 'escapedLines' });
  });
});
