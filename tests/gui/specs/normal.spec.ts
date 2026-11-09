// A source against its normal: a week of history written into the leader's registry, the last of it a drop, and
// what the Status page and the Issues page make of it. The history is written straight into `registry.db`, as
// `examples/demo-traffic.rs history` writes the demo fleet's, since a week cannot be waited for.
import type { Page } from '@playwright/test';
import { DatabaseSync } from 'node:sqlite';
import path from 'node:path';
import { test as base, expect, goto, type Api, type Leader } from '../fleet';
import { FakeElasticsearch } from '../sinks';

const test = base.extend<{ es: FakeElasticsearch }>({
  es: async ({}, use) => {
    const es = await FakeElasticsearch.start('accept');
    await use(es);
    await es.stop();
  },
  fleet: async ({ es, fleet }, use) => {
    void es;
    await use(fleet);
    await fleet.stop();
  },
});

const FIVE = 5 * 60_000;
const DAY = 24 * 60 * 60_000;
/** The five-minute tier's number in the registry (`series::Tier::FiveMinutes`). */
const FIVE_MINUTE_TIER = 2;
/** Where screenshots go, when asked for. */
const SHOTS = process.env.SMELTR_SHOTS;

/** The group's sources, each its worker's own metrics once a minute, delivered to `url`: a row or two a second,
 * which the history written below dwarfs. */
async function deploy(api: Api, url: string, sources: string[]) {
  const q = new URLSearchParams({ branch: 'normal' }).toString();
  await api.post(`/api/worker-groups?${q}`, { id: 'edge', name: 'edge', hue: null });
  const at = '/api/areas/edge';
  await api.put(`${at}/destinations?${q}`, [
    { id: 'search', type: 'elasticsearch', url, defaultIndex: 'metrics' },
  ]);
  await api.put(`${at}/pipelines/pass?${q}`, { id: 'pass', name: 'pass', steps: [] });
  await api.put(
    `${at}/sources?${q}`,
    sources.map((id) => ({ id, type: 'metrics', bucketSeconds: 60 })),
  );
  await api.put(
    `${at}/routes?${q}`,
    sources.map((id) => ({ id, source: id, pipeline: 'pass', deliver: [{ to: ['search'] }] })),
  );
  await api.openChange('normal', 'normal');
  await api.deploy('normal');
}

/** A seeded draw in [-1, 1], so every run writes the same week. */
function noise(seed: number): number {
  const x = Math.sin(seed * 12.9898) * 43758.5453;
  return 2 * (x - Math.floor(x)) - 1;
}

/** Writes eight days of `source`'s rows read into the leader's registry, five minutes at a time: `perSecond(at)` rows a second, each bucket up to 5% off. */
function history(leader: Leader, source: string, perSecond: (at: number) => number) {
  const db = new DatabaseSync(path.join(leader.proc.dir, 'smeltr-leader-data', 'registry.db'));
  db.exec('PRAGMA busy_timeout = 10000');
  const series = db
    .prepare(
      `SELECT id FROM series WHERE metric = 'smeltr_rows_polled_total' AND labels_json LIKE ?`,
    )
    .get(`%"source","${source}"%`) as { id: number } | undefined;
  expect(series, `the leader has heard ${source} read`).toBeDefined();
  const now = Math.floor(Date.now() / FIVE) * FIVE;
  const write = db.prepare(
    `INSERT OR REPLACE INTO series_points (series_id, tier, bucket_ms, value) VALUES (?, ?, ?, ?)`,
  );
  db.exec('BEGIN');
  // Two buckets on past now too, so the window the leader judges after the next bucket starts is still written.
  for (let at = now - 8 * DAY; at < now + 2 * FIVE; at += FIVE) {
    const rows = perSecond(at) * 300 * (1 + 0.05 * noise(at / FIVE));
    write.run(series!.id, FIVE_MINUTE_TIER, at, Math.round(rows));
  }
  db.exec('COMMIT');
  db.close();
}

async function shoot(page: Page, name: string) {
  if (!SHOTS) return;
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.screenshot({ path: path.join(SHOTS, `${name}-${scheme}.png`) });
  }
  await page.emulateMedia({ colorScheme: 'light' });
}

test('a source that drops below its normal is ranked and warned of, and an editor lowers it or resets its history', async ({
  admin,
  api,
  fleet,
  leader,
  es,
}) => {
  test.setTimeout(900_000);
  await admin.setViewportSize({ width: 1920, height: 1080 });
  await deploy(api, es.url, ['firewall-syslog', 'nginx-access', 'linux-auth']);
  await fleet.startWorker(leader, api, 'edge', 'edge-1');
  await expect
    .poll(async () => (await api.get('/api/flow?windowMinutes=5')).rowsIn.length, {
      timeout: 60_000,
    })
    .toBe(3);

  // About 30 rows a second for a week, then about 5 for the last half hour: long enough for the leader's four
  // readings in a row.
  const dropped = Date.now() - 30 * 60_000;
  history(leader, 'firewall-syslog', (at) => (at >= dropped ? 5 : 30));
  history(leader, 'nginx-access', () => 120);
  history(leader, 'linux-auth', () => 60);

  await goto(admin, leader, '#/status');
  const ranked = admin.locator('section.ranked');
  // The leader judges each source once a five-minute bucket, so what it judged before the history is held until the
  // next.
  await expect(ranked.getByRole('row').nth(1)).toContainText('firewall-syslog', {
    timeout: 420_000,
  });
  await ranked.scrollIntoViewIfNeeded();
  await shoot(admin, '1-furthest-from-normal');
  await ranked.getByRole('button', { name: 'About Furthest from normal' }).click();
  await expect(admin.getByText('learned from its past weeks')).toBeVisible();
  await shoot(admin, '1b-furthest-from-normal-help');
  await admin.keyboard.press('Escape');

  await goto(admin, leader, '#/status/issues');
  const row = admin
    .getByRole('list', { name: 'Rows below normal', exact: true })
    .getByRole('listitem')
    .filter({ hasText: 'firewall-syslog' });
  await expect(row).toBeVisible({ timeout: 60_000 });
  await shoot(admin, '2-issue');

  await row.getByRole('button', { name: 'Lower to info' }).click();
  await expect(row).toContainText('lowered to info by');
  await expect(row.getByRole('button', { name: 'Lower to info' })).toHaveCount(0);
  await shoot(admin, '3-lowered');

  await row.getByRole('button', { name: 'Reset history' }).click();
  const dialog = admin.getByRole('dialog');
  await expect(dialog).toBeVisible();
  await shoot(admin, '4-reset-history');
  await dialog.getByRole('button', { name: 'Reset history' }).click();
  await expect(row).toHaveCount(0, { timeout: 30_000 });
  await shoot(admin, '5-reset');
});
