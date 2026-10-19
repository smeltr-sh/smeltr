// Runs after every seed of `gui-fuzz.spec.ts` (the `fuzz-coverage` project depends on `fuzz`), and fails naming
// each option no seed reached.
import fs from 'node:fs';
import path from 'node:path';
import { test, expect } from '@playwright/test';
import { Coverage, replaying, seeds } from '../fuzz';
import { RUN_DIR } from '../run';
import { ACTIONS, DEFAULT_SEEDS, THEMES, VIEWPORTS, WHO } from '../gui-fuzz';

test('the gui fuzzer reached every option', () => {
  test.skip(replaying(), 'one seed replayed is not expected to reach them all');
  const dir = path.join(process.env[RUN_DIR] ?? '', 'fuzz');
  const files = fs.existsSync(dir) ? fs.readdirSync(dir) : [];
  test.skip(files.length === 0, 'no seed ran');
  expect(files.length, 'a seed left no record').toBe(seeds(DEFAULT_SEEDS).length);
  const coverage = new Coverage([
    ...WHO.map((w) => `who:${w}`),
    ...THEMES.map((t) => `theme:${t}`),
    ...Object.keys(VIEWPORTS).map((v) => `viewport:${v}`),
    ...ACTIONS.map((a) => `action:${a}`),
  ]);
  for (const f of files)
    for (const option of JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')) as string[])
      coverage.reach(option);
  expect(coverage.missed()).toEqual([]);
});
