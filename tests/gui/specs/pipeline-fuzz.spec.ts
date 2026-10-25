// The pipeline journey fuzzer. Each seed is one author on a fresh leader of their own, with a theme and a window size
// the seed draws, editing one pipeline's steps through the GUI for many moves in a row: adding each kind of step at
// any place and depth, deleting, moving, disabling, editing, opening and closing them, previewing them on a sample and
// pausing the records after one, saving, discarding, reloading and leaving the editor. The moves are in
// `moves/pipeline.ts`. After every move the steps on screen must be the ones the model holds, with no card left over
// and the pause where the model put it, and what is on screen must work (`onscreen.ts`); after a save, the leader's
// copy must be the model's too.
//
// A failure names its seed and the moves that led there. `SMELTR_FUZZ_SEED=n` replays one; `SMELTR_FUZZ_SEEDS=n`
// runs `n`; `SMELTR_PIPELINE_MOVES=n` makes each seed's journey `n` moves long. `pipeline-fuzz-coverage.spec.ts` checks
// that the seeds made every move.
import fs from 'node:fs';
import path from 'node:path';
import { test, expect, settled } from '../fleet';
import { Rng, seeds } from '../fuzz';
import { THEMES, VIEWPORTS } from '../gui-fuzz';
import {
  CALLEES,
  ON,
  PIPELINE,
  PIPELINE_SEEDS,
  expected,
  expectedPause,
  pauseOnScreen,
  strayCards,
  RAWS,
  SAMPLE,
  newModel,
  nextMove,
  onScreenSteps,
  open,
  type Journey,
} from '../moves/pipeline';
import { listen, onScreen } from '../onscreen';
import { RUN_DIR } from '../run';

const MOVES_PER_SEED = Number(process.env.SMELTR_PIPELINE_MOVES ?? 40);

for (const seed of seeds(PIPELINE_SEEDS)) {
  test(`pipeline fuzz seed ${seed}`, async ({ admin: page, api, leader }) => {
    test.setTimeout(600_000);
    const rng = new Rng(seed);
    const theme = rng.pick(THEMES);
    const size = rng.pick(Object.keys(VIEWPORTS) as (keyof typeof VIEWPORTS)[]);
    await page.setViewportSize(VIEWPORTS[size]);
    await page.evaluate((t) => localStorage.setItem('smeltr-theme', t), theme);
    for (const id of CALLEES)
      await api.put(`/api/pipelines/${id}${ON}`, {
        id,
        name: id,
        steps: [{ type: 'eval', id: `${id}-set`, assignments: [{ target: id, expr: '1' }] }],
      });
    await api.put(`/api/samples${ON}`, {
      id: SAMPLE,
      name: SAMPLE,
      records: RAWS.map((_raw) => ({ _raw })),
    });
    await api.put(`/api/pipelines/${PIPELINE}${ON}`, { id: PIPELINE, name: PIPELINE, steps: [] });
    const problems: string[] = [];
    listen(page, leader, problems);
    const model = newModel();
    const journey: Journey = { page, rng, leader, api, model };
    const made = new Set<string>();
    const steps = [`${theme}, ${size}`];
    const replay = () =>
      `seed ${seed} (replay with SMELTR_FUZZ_SEED=${seed}):\n  ${steps.join('\n  ')}`;
    try {
      await open(page, leader);
      for (let i = 0; i < MOVES_PER_SEED && problems.length === 0; i++) {
        const move = nextMove(rng, model);
        // Named before it runs, so a move that fails is the last one a replay lists.
        steps.push(move.name);
        steps[steps.length - 1] = await move.run(journey);
        made.add(move.name);
        await expect.poll(() => onScreenSteps(page), { timeout: 5_000 }).toEqual(expected(model));
        await expect.poll(() => strayCards(page), { timeout: 5_000 }).toEqual([]);
        await expect
          .poll(() => pauseOnScreen(page), { timeout: 5_000 })
          .toEqual(expectedPause(model));
        // Off the steps, so no head's hover-only controls lie over its summary, and once the preview has answered and
        // the steps have glided to the places its answer gives them.
        await page.mouse.move(0, 0);
        await settled(page);
        // Two frames on, since a glide starts on the frame after the change that moves it.
        await page.waitForFunction(
          () =>
            new Promise((done) =>
              requestAnimationFrame(() =>
                requestAnimationFrame(() =>
                  done(document.getAnimations().every((a) => a.playState !== 'running')),
                ),
              ),
            ),
        );
        problems.push(...(await onScreen(page, false)));
      }
    } catch (e) {
      throw new Error(`${replay()}\n${e instanceof Error ? e.message : e}`);
    } finally {
      const dir = path.join(process.env[RUN_DIR] ?? '', 'pipeline');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `${seed}.json`), JSON.stringify([...made]));
    }
    expect(problems, replay()).toEqual([]);
  });
}
