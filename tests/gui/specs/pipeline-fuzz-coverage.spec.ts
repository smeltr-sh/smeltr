// Runs after every seed of `pipeline-fuzz.spec.ts` (the `pipeline-coverage` project depends on `pipeline`), and fails
// naming each move no seed made.
import fs from 'node:fs';
import path from 'node:path';
import { test, expect } from '@playwright/test';
import { Coverage, seeds } from '../fuzz';
import { RUN_DIR } from '../run';
import { MOVES, PIPELINE_SEEDS } from '../moves/pipeline';

test('the pipeline fuzzer made every move', () => {
  const dir = path.join(process.env[RUN_DIR] ?? '', 'pipeline');
  const files = fs.existsSync(dir) ? fs.readdirSync(dir) : [];
  test.skip(files.length === 0, 'no seed ran');
  expect(files.length, 'a seed left no record').toBe(seeds(PIPELINE_SEEDS).length);
  const coverage = new Coverage(MOVES.map((m) => m.name));
  for (const f of files)
    for (const move of JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')) as string[])
      coverage.reach(move);
  expect(coverage.missed()).toEqual([]);
});
