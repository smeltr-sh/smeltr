import { describe, expect, it } from 'vitest';
import { mintPipelineId, mintRouteId, mintSampleId, suggestId } from './ids';

describe('suggestId', () => {
  it('names the kind, so two sources of different kinds are told apart before either is opened', () => {
    expect(suggestId('kafka', [])).toBe('kafka-1');
    expect(suggestId('metrics', [])).toBe('metrics-1');
  });

  it('counts past what that kind already uses, not past the list', () => {
    expect(suggestId('kafka', ['kafka-1', 'kafka-2'])).toBe('kafka-3');
    expect(suggestId('metrics', ['kafka-1', 'kafka-2'])).toBe('metrics-1');
  });

  it('fills a gap a removal left, rather than offering an id still in the list', () => {
    // `length + 1` offered `kafka-2` here, which is taken.
    expect(suggestId('kafka', ['kafka-2'])).toBe('kafka-1');
    expect(suggestId('kafka', ['kafka-1', 'kafka-3'])).toBe('kafka-2');
  });
});

describe('mintRouteId', () => {
  it('mints a distinct id every time, so a deleted route cannot hand its history on', () => {
    const minted = new Set(Array.from({ length: 500 }, () => mintRouteId()));
    expect(minted.size).toBe(500);
  });
});

describe('mintPipelineId', () => {
  it('uses the same random token policy as routes while keeping their namespaces distinct', () => {
    const pipelines = Array.from({ length: 500 }, () => mintPipelineId());
    const routes = Array.from({ length: 500 }, () => mintRouteId());
    expect(new Set(pipelines).size).toBe(500);
    expect(pipelines.every((id) => /^pipeline-[a-z0-9]{8}$/.test(id))).toBe(true);
    expect(routes.every((id) => /^route-[a-z0-9]{8}$/.test(id))).toBe(true);
    expect(new Set([...pipelines, ...routes]).size).toBe(1000);
  });
});

describe('mintSampleId', () => {
  it('mints a distinct sample id every time, never derived from the name a rename changes', () => {
    const samples = Array.from({ length: 500 }, () => mintSampleId());
    expect(new Set(samples).size).toBe(500);
    expect(samples.every((id) => /^sample-[a-z0-9]{8}$/.test(id))).toBe(true);
  });
});
