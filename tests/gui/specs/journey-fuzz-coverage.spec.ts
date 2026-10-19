// Runs after every seed of `journey-fuzz.spec.ts` (the `journey-coverage` project depends on `journey`), and fails
// naming each move no seed made.
import fs from 'node:fs';
import path from 'node:path';
import { test, expect } from '@playwright/test';
import { Coverage, seeds } from '../fuzz';
import { RUN_DIR } from '../run';
import { JOURNEY_SEEDS, MOVES } from '../moves/journey';

test('the journey fuzzer made every move', () => {
  test.skip(
    seeds(JOURNEY_SEEDS).length === 1,
    'one journey makes only some of the moves: `just fuzz-journey` runs enough to make them all',
  );
  const dir = path.join(process.env[RUN_DIR] ?? '', 'journey');
  const files = fs.existsSync(dir) ? fs.readdirSync(dir) : [];
  test.skip(files.length === 0, 'no seed ran');
  expect(files.length, 'a seed left no record').toBe(seeds(JOURNEY_SEEDS).length);
  const coverage = new Coverage(MOVES.map((m) => m.name));
  for (const f of files)
    for (const move of JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')) as string[])
      coverage.reach(move);
  expect(coverage.missed()).toEqual([]);
});
