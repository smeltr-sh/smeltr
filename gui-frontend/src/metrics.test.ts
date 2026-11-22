import { describe, expect, it } from 'vitest';
import { formatFunction } from './metrics';

describe('formatFunction', () => {
  it('names the ingest sentinel', () => {
    expect(formatFunction('4294967295')).toBe('ingest');
  });

  it('leaves a step id alone', () => {
    expect(formatFunction('3')).toBe('3');
  });
});
