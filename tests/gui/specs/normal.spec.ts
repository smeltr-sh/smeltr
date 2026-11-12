// A source against its normal, and its senders against theirs: a week of history written into the leader's registry,
// the last of it a drop, and what the Status page, the Issues page and the map's panel make of it. The history is
// written straight into `registry.db`, as `examples/demo-traffic.rs history` writes the demo fleet's, since a week
// cannot be waited for.
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

/** Writes `days` days of `source`'s rows read into the leader's registry, five minutes at a time: `perSecond(at)` rows a
 * second, each bucket up to 5% off. The leader then reads the source's quiet spells again from the first of them. */
function history(
  leader: Leader,
  source: string,
  perSecond: (at: number) => number,
  days: number = 8,
) {
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
  db.exec('DELETE FROM silence_watch');
  // Two buckets on past now too, so the window the leader judges after the next bucket starts is still written.
  for (let at = now - days * DAY; at < now + 2 * FIVE; at += FIVE) {
    const rows = perSecond(at) * 300 * (1 + 0.05 * noise(at / FIVE));
    write.run(series!.id, FIVE_MINUTE_TIER, at, Math.round(rows));
  }
  db.exec('COMMIT');
  db.close();
}

/** Writes `days` days of what each of `values` of `source`'s `field` sent, as `history` writes a source's rows: the
 * series the leader keeps each sender's rows in, at the five-minute tier it keeps them at (`leader::senders`). */
function sent(
  leader: Leader,
  source: string,
  field: string,
  values: Record<string, (at: number) => number>,
  days: number = 8,
) {
  const db = new DatabaseSync(path.join(leader.proc.dir, 'smeltr-leader-data', 'registry.db'));
  db.exec('PRAGMA busy_timeout = 10000');
  const series = db.prepare(
    `INSERT INTO series (metric, labels_json, kind) VALUES ('smeltr_sender_rows_total', ?, 'counter')
     ON CONFLICT DO UPDATE SET kind = kind RETURNING id`,
  );
  const write = db.prepare(
    `INSERT OR REPLACE INTO series_points (series_id, tier, bucket_ms, value) VALUES (?, ?, ?, ?)`,
  );
  const now = Math.floor(Date.now() / FIVE) * FIVE;
  db.exec('BEGIN');
  for (const [value, perSecond] of Object.entries(values)) {
    const labels = [
      ['field', field],
      ['group', 'edge'],
      ['source', source],
      ['value', value],
    ];
    const { id } = series.get(JSON.stringify(labels)) as { id: number };
    for (let at = now - days * DAY; at < now + 2 * FIVE; at += FIVE) {
      const rows = Math.round(perSecond(at) * 300 * (1 + 0.05 * noise(at / FIVE + value.length)));
      if (rows > 0) write.run(id, FIVE_MINUTE_TIER, at, rows);
    }
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

test('an editor says a quiet spell of a sparse source was an outage, and its normal stops learning it', async ({
  admin,
  api,
  fleet,
  leader,
  es,
}) => {
  test.setTimeout(600_000);
  await admin.setViewportSize({ width: 1920, height: 1080 });
  await deploy(api, es.url, ['audit-log']);
  await fleet.startWorker(leader, api, 'edge', 'edge-1');
  await expect
    .poll(async () => (await api.get('/api/flow?windowMinutes=5')).rowsIn.length, {
      timeout: 60_000,
    })
    .toBe(1);

  // A row every three hours for four weeks, but for one quiet spell of nine hours two weeks ago.
  const HOURS = 60 * 60_000;
  const long = Math.floor((Date.now() - 14 * DAY) / (3 * HOURS)) * 3 * HOURS;
  history(
    leader,
    'audit-log',
    (at) => (at % (3 * HOURS) === 0 && !(at >= long && at < long + 6 * HOURS) ? 1 / 300 : 0),
    29,
  );
  const past = '/api/areas/edge/sources/audit-log/past';
  // The leader reads the quiet spells at its next reading, once a five-minute bucket.
  await expect
    .poll(async () => (await api.get(past)).silences.length, { timeout: 420_000 })
    .toBeGreaterThan(0);
  const [longest] = (await api.get(past)).silences;
  expect(longest.toMillis - longest.fromMillis).toBe(9 * HOURS - FIVE);
  expect(longest.learned).toBe(true);

  await goto(admin, leader, '#/map');
  await admin
    .getByRole('region', { name: 'edge', exact: true })
    .getByRole('link', { name: /audit-log/ })
    .click();
  const panel = admin.getByRole('complementary', { name: 'audit-log', exact: true });
  await panel.getByRole('link', { name: 'History' }).click();
  // Eight hours and 55 minutes, in whole hours.
  const spell = panel.getByRole('listitem').filter({ hasText: '8 hours' });
  await expect(spell.getByRole('button', { name: 'Normal' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  await shoot(admin, '6-history');
  await spell.getByRole('button', { name: 'Outage' }).click();
  await expect(spell.getByRole('button', { name: 'Outage' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  expect((await api.get(past)).silences[0].learned).toBe(false);
  await shoot(admin, '7-outage');

  // Widened, the panel takes the map's place on the same tab; Escape gives the map back.
  const band = admin.getByRole('region', { name: 'edge', exact: true });
  await panel.getByRole('link', { name: 'Widen' }).click();
  await expect(band).toBeHidden();
  await expect(spell.getByRole('button', { name: 'Outage' })).toHaveAttribute(
    'aria-pressed',
    'true',
  );
  await shoot(admin, '8-history-wide');
  await admin.keyboard.press('Escape');
  await expect(band).toBeVisible();
  await expect(panel.getByRole('link', { name: 'Widen' })).toBeVisible();
});

test('senders that stop while their source stays within its normal are warned of, and an editor lowers one and edits the fields', async ({
  admin,
  api,
  fleet,
  leader,
  es,
}) => {
  test.setTimeout(900_000);
  await admin.setViewportSize({ width: 1920, height: 1080 });
  await deploy(api, es.url, ['fortigate-firewalls']);
  await fleet.startWorker(leader, api, 'edge', 'edge-1');
  await expect
    .poll(async () => (await api.get('/api/flow?windowMinutes=5')).rowsIn.length, {
      timeout: 60_000,
    })
    .toBe(1);

  // Ten firewalls of about 3 rows a second each for a week, then two of them stop for the last half hour: the source
  // reads a fifth less, still within its normal.
  const stopped = Date.now() - 30 * 60_000;
  const firewalls = Object.fromEntries(
    Array.from({ length: 10 }, (_, i) => {
      const name = `fw-${String(i + 1).padStart(2, '0')}`;
      const stops = name === 'fw-01' || name === 'fw-07';
      return [name, (at: number) => (stops && at >= stopped ? 0 : 3)];
    }),
  );
  history(leader, 'fortigate-firewalls', (at) => (at >= stopped ? 24 : 30));
  sent(leader, 'fortigate-firewalls', 'devname', firewalls);
  sent(leader, 'fortigate-firewalls', 'action', { deny: () => 30 });

  await goto(admin, leader, '#/status/issues');
  const row = admin
    .getByRole('list', { name: 'Senders below normal', exact: true })
    .getByRole('listitem')
    .filter({ hasText: 'fortigate-firewalls' });
  // The leader picks and judges senders at its next reading, once a five-minute bucket.
  await expect(row).toContainText('2 of 10 senders below normal, devname fw-01 and fw-07', {
    timeout: 420_000,
  });
  await shoot(admin, '9-senders-issue');

  await goto(admin, leader, '#/map');
  await admin
    .getByRole('region', { name: 'edge', exact: true })
    .getByRole('link', { name: /fortigate-firewalls/ })
    .click();
  const panel = admin.getByRole('complementary', { name: 'fortigate-firewalls', exact: true });
  await panel.getByRole('link', { name: 'Senders' }).click();
  // A field with a sender below normal opens on its senders, those below first.
  const senders = panel.getByRole('row').filter({ hasText: /fw-\d/ });
  await expect(senders.nth(0)).toContainText('fw-01');
  await expect(senders.nth(1)).toContainText('fw-07');
  await expect(panel.getByRole('listitem').filter({ hasText: 'action' })).toContainText(
    'One value',
  );
  await shoot(admin, '10-senders');
  await panel.getByRole('link', { name: 'Widen' }).click();
  await shoot(admin, '11-senders-wide');

  const fw01 = senders.filter({ hasText: 'fw-01' });
  await fw01.getByRole('button', { name: 'Lower to info' }).click();
  await expect(fw01).toContainText('Lowered to info by');

  // An editor stops watching a field, and adds one the pick cannot offer, then removes it.
  const field = (name: string) =>
    panel
      .getByRole('listitem')
      .filter({ has: admin.getByRole('checkbox', { name: `Watch ${name}`, exact: true }) });
  await field('action').getByRole('checkbox').uncheck();
  await expect(field('action')).toContainText('Not watched');
  await panel.getByRole('textbox', { name: 'Field' }).fill('policyid');
  await panel.getByRole('button', { name: 'Add field' }).click();
  await expect(field('policyid')).toContainText('No values yet');
  await shoot(admin, '12-senders-edited');
  const edits = await api.get('/api/areas/edge/sources/fortigate-firewalls/senders');
  expect(
    edits.fields
      .filter((f: { checked: boolean; added: boolean }) => !f.checked || f.added)
      .map((f: { field: string }) => f.field)
      .sort(),
  ).toEqual(['action', 'policyid']);
  await field('policyid').getByRole('button', { name: 'Remove' }).click();
  await expect(field('policyid')).toHaveCount(0);
});
