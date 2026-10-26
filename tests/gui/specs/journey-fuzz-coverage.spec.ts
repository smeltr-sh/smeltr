// Runs after every seed of `journey-fuzz.spec.ts` (the `journey-coverage` project depends on `journey`), and fails
// naming each move no seed made.
import { test, expect } from '@playwright/test';
import { Coverage, seeds } from '../fuzz';
import { reached } from '../run';
import { JOURNEY_SEEDS, MOVES } from '../moves/journey';

test('the journey fuzzer made every move', () => {
  test.skip(
    seeds(JOURNEY_SEEDS).length === 1,
    'one journey makes only some of the moves: `just fuzz-journey` runs enough to make them all',
  );
  const runs = reached('journey');
  test.skip(runs.length === 0, 'no seed ran');
  expect(runs.length, 'a seed left no record').toBe(seeds(JOURNEY_SEEDS).length);
  const coverage = new Coverage(MOVES.map((m) => m.name));
  for (const run of runs) for (const move of run) coverage.reach(move);
  expect(coverage.missed()).toEqual([]);
});
