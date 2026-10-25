// Runs after every seed of `pipeline-fuzz.spec.ts` (the `pipeline-coverage` project depends on `pipeline`), and fails
// naming each move no seed made.
import { test, expect } from '@playwright/test';
import { Coverage, seeds } from '../fuzz';
import { reached } from '../run';
import { MOVES, PIPELINE_SEEDS } from '../moves/pipeline';

test('the pipeline fuzzer made every move', () => {
  test.skip(
    seeds(PIPELINE_SEEDS).length === 1,
    'one journey makes only some of the moves: `just fuzz-pipeline` runs enough to make them all',
  );
  const runs = reached('pipeline');
  test.skip(runs.length === 0, 'no seed ran');
  expect(runs.length, 'a seed left no record').toBe(seeds(PIPELINE_SEEDS).length);
  const coverage = new Coverage(MOVES.map((m) => m.name));
  for (const run of runs) for (const move of run) coverage.reach(move);
  expect(coverage.missed()).toEqual([]);
});
