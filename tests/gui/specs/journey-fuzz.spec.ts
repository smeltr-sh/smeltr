// The journey fuzzer. Each seed is one administrator on a fresh leader of their own, with a theme and a window size
// the seed draws, taking smeltr from its first start to rows flowing and on: enrolling workers into groups, adding
// sources, destinations, pipelines and their steps, routes, lookup tables and samples, opening, reviewing, closing,
// bringing current and deploying changes, rolling back, holding secrets, adding accounts and roles, setting up
// single sign-on and the System settings. The moves (`moves/journey.ts`) are done through the GUI and checked against the leader's API, the workers and
// the rows at the sink. After every move, what is on screen must work too (`onscreen.ts`).
//
// A failure names its seed and the moves that led there. `SMELTR_FUZZ_SEED=n` replays one; `SMELTR_FUZZ_SEEDS=n`
// runs `n`. One journey makes only some of the moves; over several, `journey-fuzz-coverage.spec.ts` checks that
// the seeds made every one.
import fs from 'node:fs';
import path from 'node:path';
import { test as base, expect, signedIn, watchRequests, type Api } from '../fleet';
import { Rng, seeds } from '../fuzz';
import { THEMES, VIEWPORTS } from '../gui-fuzz';
import {
  AVRO_OBJECTS,
  JOURNEY_SEEDS,
  PROVIDER_GROUP,
  newWorld,
  nextMove,
  type Journey,
} from '../moves/journey';
import { FakeS3 } from '../buckets';
import { FakeModel } from '../model';
import { Authority } from '../acme';
import { Provider } from '../oidc';
import { listen, onScreen } from '../onscreen';
import { RUN_DIR } from '../run';
import { FakeElasticsearch } from '../sinks';

/** Moves a journey makes after its first start: enough in `check-gui` for one long journey, and more where
 * `SMELTR_JOURNEY_MOVES` asks, as `just fuzz-journey` does so its seeds reach the moves that need a long way there. */
const MOVES_PER_SEED = Number(process.env.SMELTR_JOURNEY_MOVES ?? 24);

// The fake S3 checks no signature, but a worker signs with something: these, before the host's instance
// metadata, which is not here to answer.
process.env.AWS_ACCESS_KEY_ID = 'journey';
process.env.AWS_SECRET_ACCESS_KEY = 'journey';

// The sink and the bucket are stopped after the fleet, so no worker spends its shutdown failing to reach them.
const test = base.extend<{ sink: FakeElasticsearch; bucket: FakeS3 }>({
  sink: async ({}, use) => {
    const sink = await FakeElasticsearch.start();
    await use(sink);
    await sink.stop();
  },
  bucket: async ({}, use) => {
    const bucket = await FakeS3.start(AVRO_OBJECTS);
    await use(bucket);
    await bucket.stop();
  },
  fleet: async ({ sink, bucket, fleet }, use) => {
    void sink;
    void bucket;
    await use(fleet);
    await fleet.stop();
  },
});

for (const seed of seeds(JOURNEY_SEEDS)) {
  test(`journey fuzz seed ${seed}`, async ({ browser, leader, fleet, sink, bucket }) => {
    test.setTimeout(900_000);
    const rng = new Rng(seed);
    const theme = rng.pick(THEMES);
    const size = rng.pick(Object.keys(VIEWPORTS) as (keyof typeof VIEWPORTS)[]);
    const made = new Set<string>();
    const context = await browser.newContext({
      ignoreHTTPSErrors: true,
      viewport: VIEWPORTS[size],
    });
    let api: Api | null = null;
    const provider = await Provider.start({
      subject: 'journey-person',
      name: 'Journey Person',
      email: 'journey.person@example.com',
      groups: [PROVIDER_GROUP],
    });
    const model = await FakeModel.start();
    const authority = await Authority.start({
      tlsPort: leader.port,
      httpPort: leader.acmeHttpPort,
    });
    try {
      await context.addInitScript((t) => {
        if (location.protocol === 'https:') localStorage.setItem('smeltr-theme', t);
      }, theme);
      const page = await context.newPage();
      watchRequests(page);
      const problems: string[] = [];
      listen(page, leader, problems);
      const world = newWorld();
      const journey: Journey = {
        page,
        rng,
        leader,
        fleet,
        sink,
        bucket,
        provider,
        model,
        authority,
        browser,
        world,
        truth: async () => (api ??= await signedIn(leader, leader.username, world.password!)),
      };
      const steps = [`${theme}, ${size}`];
      const replay = () =>
        `seed ${seed} (replay with SMELTR_FUZZ_SEED=${seed}):\n  ${steps.join('\n  ')}`;
      try {
        for (let i = 0; i <= MOVES_PER_SEED && problems.length === 0; i++) {
          const move = nextMove(rng, world, made);
          if (!move) break;
          // Named before it runs, so a move that fails is the last one a replay lists.
          steps.push(move.name);
          steps[steps.length - 1] = `${move.name}: ${await move.run(journey)}`;
          made.add(move.name);
          problems.push(...(await onScreen(page, false)));
        }
      } catch (e) {
        // A move that could not finish, or found the leader not as it should be, fails the seed with the moves
        // that led there.
        throw new Error(`${replay()}\n${e instanceof Error ? e.message : e}`);
      }
      expect(problems, replay()).toEqual([]);
    } finally {
      await context.close().catch(() => {});
      await (api as Api | null)?.ctx.dispose();
      await provider.stop();
      await model.stop();
      await authority.stop();
      const dir = path.join(process.env[RUN_DIR] ?? '', 'journey');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `${seed}.json`), JSON.stringify([...made]));
    }
  });
}
