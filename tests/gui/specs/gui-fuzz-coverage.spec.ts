// Runs after every seed of `gui-fuzz.spec.ts` (the `fuzz-coverage` project depends on `fuzz`), and fails naming
// each option no seed reached.
import { test, expect } from '@playwright/test';
import { Coverage, replaying, seeds } from '../fuzz';
import { reached } from '../run';
import { ACTIONS, DEFAULT_SEEDS, THEMES, VIEWPORTS, WHO } from '../gui-fuzz';

test('the gui fuzzer reached every option', () => {
  test.skip(replaying(), 'one seed replayed is not expected to reach them all');
  const runs = reached('fuzz');
  test.skip(runs.length === 0, 'no seed ran');
  expect(runs.length, 'a seed left no record').toBe(seeds(DEFAULT_SEEDS).length);
  const coverage = new Coverage([
    ...WHO.map((w) => `who:${w}`),
    ...THEMES.map((t) => `theme:${t}`),
    ...Object.keys(VIEWPORTS).map((v) => `viewport:${v}`),
    ...ACTIONS.map((a) => `action:${a}`),
  ]);
  for (const run of runs) for (const option of run) coverage.reach(option);
  expect(coverage.missed()).toEqual([]);
});
