import { describe, expect, it } from 'vitest';
import { formatFunction } from './metrics';

describe('formatFunction', () => {
  it('shows the ingest label a worker through 1.3 sends as ingest', () => {
    expect(formatFunction('4294967295')).toBe('ingest');
  });

  it('shows the label `ingest` as ingest', () => {
    expect(formatFunction('ingest')).toBe('ingest');
  });

  it('leaves a step id alone', () => {
    expect(formatFunction('3')).toBe('3');
  });
});
