import { describe, expect, it } from 'vitest';
import { acceptedBy } from './schemas';

describe("a new route's pipeline", () => {
  const destinations = [
    { id: 'elastic', accepts: ['ecs'] },
    { id: 'lake', accepts: ['network', 'ecs'] },
    { id: 'archive' },
  ];

  it('produces what the destinations it delivers to accept, each once', () => {
    expect(acceptedBy(['elastic', 'lake'], destinations)).toEqual(['ecs', 'network']);
    expect(acceptedBy(['archive'], destinations)).toEqual([]);
    expect(acceptedBy([], destinations)).toEqual([]);
  });
});
