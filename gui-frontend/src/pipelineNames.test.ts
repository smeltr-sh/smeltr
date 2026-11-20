import { describe, expect, it, vi } from 'vitest';

const bundles: Record<string, { id: string; name: string }[] | Error> = {};
vi.mock('./api', () => ({
  SHARED_SCOPE: 'shared',
  configAt: async (_branch: string, scope: string) => {
    const b = bundles[scope];
    if (b instanceof Error) throw b;
    const routes =
      scope === 'west' ? [{ id: 'r-1', source: 'in', pipeline: 'p-2', condition: null }] : [];
    return { config: { pipelines: b ?? [], routes } };
  },
}));

const { nameOf, namesAt, pipelineNamesAt } = await import('./pipelineNames');

describe("a scope's pipeline names", () => {
  it("names a group's own pipelines and shared's, its own winning", async () => {
    bundles.shared = [
      { id: 'p-1', name: 'Shared one' },
      { id: 'p-2', name: 'Enrich' },
    ];
    bundles.west = [{ id: 'p-1', name: 'West one' }];
    expect(await pipelineNamesAt('work', 'west')).toEqual({ 'p-1': 'West one', 'p-2': 'Enrich' });
    expect((await namesAt('work', 'west')).routes).toEqual({ 'r-1': 'in \u2192 Enrich' });
  });

  it('names what it can read when one bundle cannot be read', async () => {
    bundles.shared = new Error('unreachable');
    bundles.west = [{ id: 'p-1', name: 'West one' }];
    expect(await pipelineNamesAt('work', 'west')).toEqual({ 'p-1': 'West one' });
  });

  it('shows the id of a pipeline nothing names', () => {
    expect(nameOf({ 'p-1': 'Orders' }, 'p-1')).toBe('Orders');
    expect(nameOf({}, 'p-9')).toBe('p-9');
  });
});
