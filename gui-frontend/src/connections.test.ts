import { describe, expect, it } from 'vitest';
import type { Connected, ConnectionsResponse } from './api';
import { listHash, mapHash, objectHash, rows, summary } from './connections';

const at = (kind: Connected['kind'], id: string, scope = 'network'): Connected => ({
  kind,
  id,
  scope,
});

function answer(over: Partial<ConnectionsResponse>): ConnectionsResponse {
  return {
    ok: true,
    errors: [],
    upstream: [],
    downstream: [],
    groups: ['network'],
    drawnIn: [],
    ...over,
  };
}

describe('connections', () => {
  it('names one and counts several, and leaves routes off the folded line', () => {
    const destinations = Array.from({ length: 100 }, (_, i) => at('destination', `es-${i}`));
    const c = answer({
      upstream: [at('source', 'panw-firewalls')],
      downstream: [at('route', 'panos'), ...destinations],
    });
    const all = rows('pipeline', c);
    expect(all.map((r) => r.label)).toEqual(['sources', 'routes', 'destinations']);
    expect(summary(all, c.groups)).toBe('source panw-firewalls · 100 destinations · network');
  });

  it("labels the editor's own kind by which side it sits on", () => {
    const pipeline = rows('pipeline', answer({ downstream: [at('pipeline', 'fw')] }));
    expect(summary(pipeline, ['network', 'edge'])).toBe('called by fw · 2 groups');
    const destination = rows(
      'destination',
      answer({
        upstream: [at('pipeline', 'a'), at('pipeline', 'b')],
        downstream: [at('destination', 'dlq')],
      }),
    );
    expect(destination.map((r) => r.label)).toEqual(['pipelines', 'refused to']);
    expect(summary(destination, ['network'])).toBe('2 pipelines · refused to dlq · network');
  });

  it('says nothing yet where nothing is connected', () => {
    expect(summary(rows('lookupTable', answer({})), ['network'])).toBe('nothing yet · network');
  });

  it('opens an object, a narrowed list, and the map where it draws one', () => {
    expect(objectHash(at('pipeline', 'a b'))).toBe('#/build/pipelines/a%20b');
    expect(objectHash(at('route', 'r'))).toBe('#/build/routes?only=r');
    expect(listHash('destination', ['x', 'y,z'])).toBe('#/build/destinations?only=x&only=y%2Cz');
    expect(mapHash('source', 's', ['edge'])).toBe('#/map?group=edge&kind=source&id=s');
    expect(mapHash('pipeline', 'p', ['edge', 'core'])).toBe('#/map?q=kind%3Apipeline+id%3A%22p%22');
    expect(mapHash('pipeline', 'p', [])).toBeUndefined();
    expect(mapHash('route', 'r', ['edge'])).toBeUndefined();
  });
});
