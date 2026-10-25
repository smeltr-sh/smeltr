// The data engineer's view of a running fleet, and the groups it is split into. Rows flow from each worker's own
// `metrics` source into a fake Elasticsearch the test stands up (`sinks.ts`), which accepts, refuses or is down.
import type { Locator, Page } from '@playwright/test';
import dgram from 'node:dgram';
import os from 'node:os';
import { test as base, expect, goto, type Api } from '../fleet';
import { FakeElasticsearch, type SinkMode } from '../sinks';
import { described, workerDetail } from '../moves/admin';

type Pipeline = { id: string; name: string; steps: unknown[] };
type Bundle = {
  sources?: unknown[];
  destinations?: unknown[];
  pipelines?: Pipeline[];
  routes?: unknown[];
  manifest?: unknown;
};

// Sinks are set up before the fleet and torn down after it, so a worker never spends its shutdown failing to
// reach one that is already gone.
const test = base.extend<{ sink: (mode?: SinkMode) => Promise<FakeElasticsearch> }>({
  sink: async ({}, use) => {
    const started: FakeElasticsearch[] = [];
    await use(async (mode = 'accept') => {
      const s = await FakeElasticsearch.start(mode);
      started.push(s);
      return s;
    });
    for (const s of started) await s.stop();
  },
  fleet: async ({ sink, fleet }, use) => {
    void sink;
    await use(fleet);
    // Workers stop before the sinks they deliver to; `stop` is safe to call again.
    await fleet.stop();
  },
});

function query(branch: string, scope?: string) {
  const p = new URLSearchParams({ branch });
  if (scope) p.set('scope', scope);
  return p.toString();
}

/** Creates `newGroups` on `branch`, writes each scope's bundle there, then opens a change and deploys it. */
async function deploy(
  api: Api,
  branch: string,
  newGroups: string[],
  scopes: Record<string, Bundle>,
) {
  for (const id of newGroups)
    await api.post(`/api/worker-groups?${query(branch)}`, { id, name: id, hue: null });
  for (const [scope, b] of Object.entries(scopes)) {
    const q = query(branch, scope);
    if (b.destinations) await api.put(`/api/destinations?${q}`, b.destinations);
    for (const p of b.pipelines ?? []) await api.put(`/api/pipelines/${p.id}?${q}`, p);
    if (b.sources) await api.put(`/api/sources?${q}`, b.sources);
    if (b.routes) await api.put(`/api/routes?${q}`, b.routes);
    if (b.manifest) await api.put(`/api/manifest?${q}`, b.manifest);
  }
  await api.openChange(branch, branch);
  await api.deploy(branch);
}

const PASS: Pipeline = { id: 'pass', name: 'pass', steps: [] };

/** A worker's own metrics, every second, through `pipeline` into the Elasticsearch at `url`. */
function metricsTo(url: string, pipeline: Pipeline = PASS, route: object = {}): Bundle {
  return {
    destinations: [{ id: 'search', type: 'elasticsearch', url, defaultIndex: 'metrics' }],
    pipelines: [pipeline],
    sources: [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 1 }],
    routes: [
      {
        id: 'all',
        source: 'own-metrics',
        pipeline: pipeline.id,
        deliver: [{ to: ['search'] }],
        ...route,
      },
    ],
  };
}

/** The Map's band for `group`. */
function band(page: Page, group: string) {
  return page.getByRole('region', { name: group, exact: true });
}

/** The panel the Map opens beside itself for the object named `name`. */
function focus(page: Page, name: string) {
  return page.getByRole('complementary', { name, exact: true });
}

/** The Issues page's row for `subject` in `group`, under the condition labelled `condition`. */
function issue(page: Page, condition: string, subject: string, group: string) {
  return page
    .getByRole('list', { name: condition, exact: true })
    .getByRole('listitem')
    .filter({ has: page.getByText(subject, { exact: true }) })
    .filter({ has: page.getByText(group, { exact: true }) });
}

/** The value a focus panel's description list gives for `term`. */
function fact(panel: Locator, term: string) {
  return panel
    .getByRole('term')
    .filter({ hasText: new RegExp(`^\\s*${term}\\b`) })
    .locator('xpath=following-sibling::dd[1]');
}

/** A rate the GUI prints that is above zero. */
const MOVING = /(0\.\d*[1-9]|[1-9][\d.]*) rows\/s/;

test('the Status page shows every worker of every group, and one that stops reporting needs attention', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E1
  const es = await sink();
  await deploy(api, 'two-groups', ['east', 'west'], {
    east: metricsTo(es.url),
    west: metricsTo(es.url),
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  const stalled = await fleet.startWorker(leader, api, 'east', 'east-2');
  await fleet.startWorker(leader, api, 'west', 'west-1');

  await goto(admin, leader, '#/status');
  const totals = admin.getByLabel('Fleet totals');
  await expect(totals).toContainText('3 online', { timeout: 30_000 });
  await expect(admin.getByRole('button', { name: /^east\s*2 workers/ })).toBeVisible();
  await expect(admin.getByRole('button', { name: /^west\s*1 worker/ })).toBeVisible();
  for (const w of ['east-1', 'east-2', 'west-1'])
    await expect(admin.getByRole('button', { name: new RegExp(`^${w}\\b`) })).toBeVisible();
  await expect(admin.getByRole('button', { name: /^Needs attention\s*0$/ })).toBeVisible();

  // Frozen, the worker sends nothing: it is silent, not stopped.
  stalled.child.kill('SIGSTOP');
  try {
    await expect(totals).toContainText('1 silent', { timeout: 20_000 });
    await admin.getByRole('button', { name: /^Needs attention\s*1$/ }).click();
    await expect(admin.getByRole('button', { name: /^east-2\b/ })).toBeVisible();
    await expect(admin.getByRole('button', { name: /^east-1\b/ })).toHaveCount(0);
    await expect(admin.getByRole('link', { name: 'Active fleet issues' })).toContainText('east-2');

    await admin.getByRole('link', { name: 'Active fleet issues' }).click();
    await expect(admin.getByRole('heading', { name: 'Issues' })).toBeVisible();
    await expect(admin.getByRole('button', { name: /^Failed\s*1$/ })).toBeVisible();
    await expect(issue(admin, 'Worker silent', 'east-2', 'east')).toContainText(
      'stopped sending heartbeats',
    );
  } finally {
    stalled.child.kill('SIGCONT');
  }

  // Back, it is online again and the condition clears by itself.
  await expect(admin.getByText('No active issues.')).toBeVisible({ timeout: 20_000 });
});

test("a worker's detail names where each receiver it runs listens", async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  const port = await freeUdpPort();
  const bundle = metricsTo(es.url);
  bundle.sources!.push({
    id: 'firewalls',
    type: 'syslogReceiver',
    port,
    transport: 'udp',
    allowFrom: ['127.0.0.0/8'],
  });
  bundle.routes!.push({
    id: 'syslog',
    source: 'firewalls',
    pipeline: PASS.id,
    deliver: [{ to: ['search'] }],
  });
  await deploy(api, 'receiving', [], { default: bundle });
  await fleet.startWorker(leader, api, 'default', 'w1');

  const detail = await workerDetail(admin, 'w1');
  // Every address of the host, which is where the worker listens by default, reads as the host's name.
  await expect(described(detail, 'Receivers')).toHaveText(
    `firewalls on ${os.hostname()}:${port} over UDP`,
    { timeout: 60_000 },
  );
});

test('a worker whose source is blocked needs attention, though it heartbeats', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E1
  const es = await sink();
  await deploy(api, 'blocked-west', ['east', 'west'], {
    east: metricsTo(es.url),
    west: metricsTo(es.url, PASS, { condition: '_raw.includes("smeltr_rows")' }),
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await fleet.startWorker(leader, api, 'west', 'west-1');

  await goto(admin, leader, '#/status');
  await expect(admin.getByLabel('Fleet totals')).toContainText('2 online', { timeout: 30_000 });
  await admin.getByRole('button', { name: /^Needs attention\s*1$/ }).click({ timeout: 30_000 });
  const blocked = admin.getByRole('button', { name: /^west-1\b/ });
  await expect(blocked).toHaveClass(/\battention\b/);
  await expect(blocked.locator('.state-dot')).toHaveClass(/\bfailed\b/);
  await expect(admin.getByRole('button', { name: /^east-1\b/ })).toHaveCount(0);

  await admin.getByRole('button', { name: /^All\s*\d/ }).click();
  const fine = admin.getByRole('button', { name: /^east-1\b/ });
  await expect(fine).not.toHaveClass(/\battention\b/);
  await expect(fine.locator('.state-dot')).toHaveClass(/\bonline\b/);
  await expect(fine.locator('.state-dot')).not.toHaveClass(/\b(failed|warning)\b/);
});

test("the Issues page filters by severity, and tells a stalled destination's log on its failure", async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E1
  const fine = await sink();
  const down = await sink('down');
  await deploy(api, 'two-groups', ['east', 'north'], {
    east: metricsTo(fine.url),
    north: metricsTo(down.url),
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await fleet.startWorker(leader, api, 'north', 'north-1');

  await goto(admin, leader, '#/status/issues');
  // What north-1 logged as the destination stalled is told on the failure it repeats, not as a warning of its own.
  const unreachable = issue(admin, 'Destination unreachable', 'search', 'north');
  await expect(unreachable).toContainText('Last reported by worker north-1', { timeout: 30_000 });
  await expect(admin.getByText(/egress\.destination_stalled/)).toHaveCount(0);

  await admin.getByRole('button', { name: /^Failed\s*1$/ }).click();
  await expect(admin.getByText('Destination unreachable', { exact: true })).toBeVisible();
  await expect(admin.getByText('Logged a warning', { exact: true })).toHaveCount(0);

  await admin.getByRole('button', { name: /^Warning\s*\d/ }).click();
  await expect(admin.getByText('Destination unreachable', { exact: true })).toHaveCount(0);
  await expect(admin.getByText('east-1')).toHaveCount(0);
});

test('rows in and out are charted over the window the engineer picks', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E2
  const es = await sink();
  await deploy(api, 'flow', ['east', 'west'], {
    east: metricsTo(es.url),
    west: metricsTo(es.url),
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await fleet.startWorker(leader, api, 'west', 'west-1');

  await goto(admin, leader, '#/status');
  const flow = admin.getByRole('region', { name: /^Rows in and out/ });
  await expect(flow.getByRole('heading')).toContainText('whole fleet');
  await expect(flow.getByText(new RegExp(`^in ${MOVING.source}`))).toBeVisible({ timeout: 40_000 });
  // Rows are delivered after they are read, so out can reach its first moving bucket a bucket after in.
  await expect(flow.getByText(new RegExp(`^out ${MOVING.source}`))).toBeVisible({
    timeout: 30_000,
  });

  const windows = flow.getByRole('group', { name: 'Chart window' });
  await windows.getByRole('button', { name: '15m' }).click();
  await expect(flow.getByText(new RegExp(`^in ${MOVING.source}`))).toBeVisible();
  // The last few minutes have not aged into a day's buckets yet.
  await windows.getByRole('button', { name: '24h' }).click();
  await expect(flow.getByText(/Nothing has aged into this window's buckets yet/)).toBeVisible();
  await windows.getByRole('button', { name: '60m' }).click();
  await expect(flow.getByText(new RegExp(`^in ${MOVING.source}`))).toBeVisible();

  // One group's share.
  await admin.getByRole('button', { name: /^east\s*1 worker/ }).click();
  await expect(flow.getByRole('heading')).toContainText('group east');
  // The fleet moving says nothing of one group. A reading moves a 10s bucket at a time, and the map polls once a bucket.
  await expect(flow.getByText(new RegExp(`^in ${MOVING.source}`))).toBeVisible({ timeout: 30_000 });

  // Per source and per destination, on the map.
  await admin.getByRole('link', { name: 'Map' }).click();
  await expect(band(admin, 'east').getByRole('link', { name: /own-metrics/ })).toContainText(
    MOVING,
    { timeout: 30_000 },
  );
  await expect(band(admin, 'east').getByRole('link', { name: /search/ })).toContainText(MOVING, {
    timeout: 30_000,
  });
});

test('a source blocked by rows no route takes says which, since when, why and what to do', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E4
  const es = await sink();
  await deploy(api, 'narrow-route', ['west'], {
    west: metricsTo(es.url, PASS, { condition: '_raw.includes("smeltr_rows")' }),
  });
  await fleet.startWorker(leader, api, 'west', 'west-1');

  await goto(admin, leader, '#/status/issues');
  const row = issue(admin, 'Source blocked', 'own-metrics', 'west');
  await expect(row).toContainText('matched no route', { timeout: 30_000 });
  await expect(row).toContainText('add a route that takes them');
  await expect(row).toContainText(/\d+s ago/);

  await admin.getByRole('link', { name: 'Map' }).click();
  const source = band(admin, 'west').getByRole('link', { name: /own-metrics/ });
  await expect(source).toContainText('Source blocked');
  await source.click();
  await expect(
    focus(admin, 'own-metrics').getByRole('button', { name: 'About Source blocked' }),
  ).toBeVisible();
});

test('a capture of a blocked source shows the records that blocked it, and why', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  await deploy(api, 'narrow-route', ['west'], {
    west: metricsTo(es.url, PASS, { condition: '_raw.includes("smeltr_rows")' }),
  });
  await fleet.startWorker(leader, api, 'west', 'west-1');
  await goto(admin, leader, '#/status/issues');
  await expect(issue(admin, 'Source blocked', 'own-metrics', 'west')).toBeVisible({
    timeout: 30_000,
  });

  await admin.screenshot({ path: 'results/blocked-source-issue.png' });

  // Opened once the source is blocked, from the source on the Map: the batch it holds never runs again, so this is
  // the only way to see it.
  await admin.getByRole('link', { name: 'Map' }).click();
  await band(admin, 'west')
    .getByRole('link', { name: /own-metrics/ })
    .click();
  await admin.getByRole('button', { name: 'Capture', exact: true }).click();
  const dialog = admin.getByRole('dialog');
  await expect(dialog.getByRole('combobox', { name: /^Capture target/ })).toHaveValue(
    'source:own-metrics',
  );
  await expect(dialog.getByRole('combobox', { name: /^Group/ })).toHaveValue('west');
  await dialog.getByRole('spinbutton', { name: 'records' }).fill('1');
  await dialog.getByRole('button', { name: 'Start' }).click();

  const saving = admin.getByRole('dialog', { name: /Save the capture/ });
  await expect(saving).toBeVisible({ timeout: 60_000 });
  await expect(saving).toContainText('__exception.message');
  await expect(saving).toContainText('matched no route');
  await admin.screenshot({ path: 'results/blocked-source-capture.png' });
});

test('a source blocked by a refusing destination names the fix', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E4
  const refusing = await sink('refuse');
  const bundle = metricsTo(refusing.url);
  bundle.destinations = (bundle.destinations as object[]).map((d) => ({
    ...d,
    exceptions: { kinds: { Refused: 'block' } },
  }));
  await deploy(api, 'refused-blocks', ['west'], { west: bundle });
  await fleet.startWorker(leader, api, 'west', 'west-1');

  await goto(admin, leader, '#/status/issues');
  const row = issue(admin, 'Blocked by destination refusals', 'own-metrics', 'west');
  await expect(row).toContainText('refused a record and is set to block', { timeout: 30_000 });
  await admin.screenshot({ path: 'results/refused-source-issue.png' });
  // The fix survives the cut a heartbeat makes, however long the destination's own words.
  await expect(row).toContainText("fix the destination's schema or the pipeline");
});

test('a capture at a destination set to block shows the rows it refused, and why', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const refusing = await sink('refuse');
  const bundle = metricsTo(refusing.url);
  bundle.destinations = (bundle.destinations as object[]).map((d) => ({
    ...d,
    exceptions: { kinds: { Refused: 'block' } },
  }));
  await deploy(api, 'refused-capture', ['west'], { west: bundle });
  await fleet.startWorker(leader, api, 'west', 'west-1');
  await goto(admin, leader, '#/status/issues');
  await expect(issue(admin, 'Blocked by destination refusals', 'own-metrics', 'west')).toBeVisible({
    timeout: 30_000,
  });

  // Opened once the source is blocked: no new row reaches the destination, so what it shows is what it holds.
  await admin.getByRole('link', { name: 'Map' }).click();
  await band(admin, 'west')
    .getByRole('link', { name: /search/ })
    .first()
    .click();
  await focus(admin, 'search').getByRole('button', { name: 'Capture' }).click();
  const dialog = admin.getByRole('dialog', { name: /search/ });
  await dialog.getByRole('spinbutton', { name: 'records' }).fill('1');
  await dialog.getByRole('button', { name: 'Start' }).click();

  const saving = admin.getByRole('dialog', { name: /Save the capture/ });
  await expect(saving).toBeVisible({ timeout: 60_000 });
  await expect(saving).toContainText('__exception.message');
  await expect(saving).toContainText('mapper_parsing_exception');
  await admin.screenshot({ path: 'results/refused-destination-capture.png' });
});

test('a destination refusing rows and one that cannot be reached are each their own condition', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E5
  const refusing = await sink('refuse');
  const down = await sink('down');
  await deploy(api, 'bad-destinations', ['west', 'north'], {
    west: metricsTo(refusing.url),
    north: metricsTo(down.url),
  });
  await fleet.startWorker(leader, api, 'west', 'west-1');
  await fleet.startWorker(leader, api, 'north', 'north-1');

  await goto(admin, leader, '#/status/issues');
  const refused = issue(admin, 'Refusing writes', 'search', 'west');
  await expect(refused).toContainText('mapper_parsing_exception', { timeout: 30_000 });
  await expect(refused).toContainText("fix the destination's schema");
  const unreachable = issue(admin, 'Destination unreachable', 'search', 'north');
  await expect(unreachable).toContainText('cannot be reached', { timeout: 30_000 });

  await admin.getByRole('link', { name: 'Map' }).click();
  await band(admin, 'west')
    .getByRole('link', { name: /search/ })
    .click();
  await expect(
    focus(admin, 'search').getByRole('button', { name: 'About Refusing writes' }),
  ).toBeVisible();
  await expect(fact(focus(admin, 'search'), 'Refused')).toHaveText(/[1-9]/, {
    timeout: 20_000,
  });

  await band(admin, 'north')
    .getByRole('link', { name: /search/ })
    .click();
  await expect(
    focus(admin, 'search').getByRole('button', { name: 'About Destination unreachable' }),
  ).toBeVisible();
  await expect(fact(focus(admin, 'search'), 'Stalls')).toHaveText(/[1-9]/, {
    timeout: 20_000,
  });
});

test('rows that do not come out are put down to a route or a step', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // E7
  const es = await sink();
  const trim: Pipeline = {
    id: 'trim',
    name: 'trim',
    steps: [{ type: 'drop', id: 'no-polled', condition: '_raw.includes("polled")' }],
  };
  await deploy(api, 'attribution', ['east'], {
    east: {
      ...metricsTo(es.url, trim, { condition: '_raw.includes("smeltr_rows")' }),
      manifest: { exceptions: { kinds: { Unmatched: 'drop' } } },
    },
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');

  await goto(admin, leader, '#/map');
  const east = band(admin, 'east');
  await expect(east.getByRole('link', { name: /own-metrics/ })).toContainText(/unmatched/, {
    timeout: 40_000,
  });

  await east.getByRole('link', { name: /own-metrics/ }).click();
  await focus(admin, 'own-metrics').getByRole('link', { name: 'Routes' }).click();
  await expect(
    focus(admin, 'own-metrics').getByText(/^[1-9][\d,]* of [\d,]+ met Unmatched\.$/),
  ).toBeVisible();

  await east.getByRole('link', { name: /trim/ }).click();
  await focus(admin, 'trim').getByRole('link', { name: 'Steps' }).click();
  await expect(focus(admin, 'trim').getByText(/[1-9][\d,]* dropped/)).toBeVisible({
    timeout: 20_000,
  });
});

test('each group runs a configuration of its own', async ({ admin, api, fleet, leader, sink }) => {
  // G1
  const eastEs = await sink();
  const westEs = await sink();
  const westTo = (url: string, sources: { id: string; [field: string]: unknown }[]): Bundle => ({
    destinations: [{ id: 'search', type: 'elasticsearch', url, defaultIndex: 'logs' }],
    pipelines: [PASS],
    sources,
    routes: sources.map(({ id }) => ({
      id,
      source: id,
      pipeline: 'pass',
      deliver: [{ to: ['search'] }],
    })),
  });
  const ownLogs = { id: 'own-logs', type: 'logs' };
  await deploy(api, 'boundaries', ['east', 'west'], {
    east: metricsTo(eastEs.url),
    west: westTo(westEs.url, [ownLogs]),
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await fleet.startWorker(leader, api, 'west', 'west-1');

  // A source in one group does not exist in the other.
  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: 'shared', exact: true }).click();
  await admin.getByRole('button', { name: 'east east — live' }).click();
  await expect(admin.getByText('own-metrics').first()).toBeVisible();
  await expect(admin.getByText('own-logs')).toHaveCount(0);
  await admin.getByRole('button', { name: 'east', exact: true }).click();
  await admin.getByRole('button', { name: 'west west — live' }).click();
  await expect(admin.getByText('own-logs').first()).toBeVisible();
  await expect(admin.getByText('own-metrics')).toHaveCount(0);

  // A change to west's configuration leaves east's as it was.
  await deploy(api, 'west-only', [], {
    west: westTo(westEs.url, [ownLogs, { id: 'west-metrics', type: 'metrics', bucketSeconds: 1 }]),
  });
  await goto(admin, leader, '#/map');
  await expect(band(admin, 'west').getByRole('link', { name: /west-metrics/ })).toBeVisible();
  await expect(band(admin, 'east').getByRole('link', { name: /west-metrics/ })).toHaveCount(0);
  await expect(band(admin, 'east').getByRole('link', { name: /own-metrics/ })).toContainText(
    MOVING,
    { timeout: 30_000 },
  );
});

test('the whole fleet is one view, grouped by group, and a group only filters it', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // G2
  const es = await sink();
  const groups = ['east', 'west', 'north'];
  await deploy(
    api,
    'three-groups',
    groups,
    Object.fromEntries(groups.map((g) => [g, metricsTo(es.url)])),
  );
  for (const g of groups) await fleet.startWorker(leader, api, g, `${g}-1`);

  await goto(admin, leader, '#/status');
  await expect(admin.getByLabel('Fleet totals')).toContainText('3 online', { timeout: 30_000 });
  for (const g of groups)
    await expect(
      admin.getByRole('button', { name: new RegExp(`^${g}\\s*1 worker`) }),
    ).toBeVisible();

  await admin.getByRole('link', { name: 'Map' }).click();
  for (const g of groups)
    await expect(band(admin, g).getByRole('link', { name: /own-metrics/ })).toBeVisible();

  const filter = admin.getByLabel('filter by worker group');
  await filter.selectOption({ label: 'west' });
  await expect(admin.getByRole('heading', { name: 'west', exact: true })).toBeVisible();
  await expect(admin.getByRole('heading', { name: 'east', exact: true })).toHaveCount(0);
  await expect(admin.getByRole('heading', { name: 'north', exact: true })).toHaveCount(0);

  await filter.selectOption({ label: 'All groups' });
  for (const g of groups)
    await expect(admin.getByRole('heading', { name: g, exact: true })).toBeVisible();
});

test('sources of one system into the same pipeline are one stack, whose panel lists them', async ({
  admin,
  api,
  leader,
  sink,
}) => {
  // G2
  const es = await sink();
  const ids = ['metrics-a', 'metrics-b', 'metrics-c'];
  await deploy(api, 'stacked', ['east'], {
    east: {
      destinations: [{ id: 'search', type: 'elasticsearch', url: es.url, defaultIndex: 'm' }],
      pipelines: [PASS],
      sources: ids.map((id) => ({ id, type: 'metrics', bucketSeconds: 1 })),
      routes: ids.map((id) => ({
        id,
        source: id,
        pipeline: 'pass',
        deliver: [{ to: ['search'] }],
      })),
    },
  });

  await goto(admin, leader, '#/map');
  const east = band(admin, 'east');
  await expect(east.getByRole('link', { name: /^3 sources/ })).toBeVisible();
  await expect(east.getByRole('link', { name: /metrics-a/ })).toHaveCount(0);

  await east.getByRole('link', { name: /^3 sources/ }).click();
  const stack = focus(admin, '3 sources');
  for (const id of ids)
    await expect(stack.getByRole('link', { name: new RegExp(id) })).toBeVisible();

  // A member selected from the list stands out of the stack, and Back returns to the stack.
  await stack.getByRole('link', { name: /metrics-b/ }).click();
  await expect(focus(admin, 'metrics-b')).toBeVisible();
  await expect(east.getByRole('link', { name: /metrics-b/ })).toBeVisible();
  await admin.goBack();
  await expect(focus(admin, '3 sources')).toBeVisible();
});

test('logic shared by several groups is written once and a change to it reaches each', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  // G3
  const sinks = { east: await sink(), west: await sink() };
  const stamp = (version: string): Pipeline => ({
    id: 'stamp',
    name: `stamp-${version}`,
    steps: [
      { type: 'eval', id: 'mark', assignments: [{ target: 'stamped', expr: `"${version}"` }] },
    ],
  });
  await deploy(api, 'shared-logic', ['east', 'west'], {
    shared: { pipelines: [stamp('v1')] },
    east: { ...metricsTo(sinks.east.url, stamp('v1')), pipelines: [] },
    west: { ...metricsTo(sinks.west.url, stamp('v1')), pipelines: [] },
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await fleet.startWorker(leader, api, 'west', 'west-1');
  const stampedWith = (group: keyof typeof sinks) => () =>
    new Set(sinks[group].documents.map((d) => d.stamped));
  for (const g of ['east', 'west'] as const)
    await expect.poll(stampedWith(g), { timeout: 30_000 }).toContain('v1');

  await goto(admin, leader, '#/map');
  for (const g of ['east', 'west'])
    await expect(band(admin, g).getByRole('link', { name: /stamp-v1/ })).toContainText('shared');
  await band(admin, 'east')
    .getByRole('link', { name: /stamp-v1/ })
    .click();
  const panel = focus(admin, 'stamp-v1');
  await expect(panel).toContainText('Also runs in');
  await expect(panel.getByRole('link', { name: 'west', exact: true })).toBeVisible();

  // One change to the shared pipeline, and each group's worker runs it.
  await deploy(api, 'shared-logic-v2', [], { shared: { pipelines: [stamp('v2')] } });
  for (const g of ['east', 'west'] as const)
    await expect.poll(stampedWith(g), { timeout: 30_000 }).toContain('v2');
  await admin.reload();
  for (const g of ['east', 'west'])
    await expect(band(admin, g).getByRole('link', { name: /stamp-v2/ })).toContainText('shared');
});

test('a capture at a destination shows each row its pipeline finished for it, and saves', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  await deploy(api, 'outgoing', ['east'], { east: metricsTo(es.url) });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await expect.poll(() => es.indexed, { timeout: 60_000 }).toBeGreaterThan(0);

  // From the destination's row, in the group that runs it.
  await admin.evaluate(() => {
    localStorage.setItem('smeltr-scope', 'east');
    localStorage.setItem('smeltr-branch', 'main');
  });
  await goto(admin, leader, '#/build/destinations');
  await admin.reload();
  await admin.getByRole('button', { name: 'More actions for search' }).click();
  await admin
    .getByRole('menuitem', { name: 'Capture' })
    .or(admin.getByRole('button', { name: 'Capture' }))
    .first()
    .click();
  // The same form as a source's capture, with the destination fixed.
  const dialog = admin.getByRole('dialog', { name: /search/ });
  await dialog.getByRole('textbox', { name: /Keep records where/ }).fill('_raw != ""');
  await dialog.getByRole('spinbutton', { name: 'records' }).fill('3');
  await admin.screenshot({ path: 'results/destination-capture-form.png' });
  await dialog.getByRole('button', { name: 'Start' }).click();

  // Saved as a sample like any capture, naming no source, its `__` fields kept.
  const saving = admin.getByRole('dialog', { name: /Save the capture/ });
  await expect(saving).toBeVisible({ timeout: 60_000 });
  await expect(saving).toContainText('__destination.route');
  await expect(saving).toContainText('__source.');
  await admin.screenshot({ path: 'results/destination-capture-row.png' });
  await saving.getByLabel('Save as').fill('outgoing rows');
  await saving.getByRole('button', { name: 'Save sample' }).click();
  // `main` is never edited directly: the save lands on a branch.
  const branch = admin.getByRole('dialog', { name: /Branch required/i });
  await branch.getByLabel('branch to edit on').fill('captured');
  await branch.getByRole('button', { name: 'Edit on this branch' }).click();
  await expect(saving).toBeHidden();
  const { samples } = await api.get('/api/samples?branch=captured&scope=east');
  const saved = samples.find((s: { name: string }) => s.name === 'outgoing rows');
  expect(saved).toMatchObject({ records: 3 });
  expect(saved.source).toBeUndefined();

  // From the map's panel for the same destination.
  await goto(admin, leader, '#/map');
  await band(admin, 'east')
    .getByRole('link', { name: /search/ })
    .first()
    .click();
  // Once read, so the click lands where the button stays.
  await expect(focus(admin, 'search').getByText('Accepted output')).toBeVisible();
  await focus(admin, 'search').getByRole('button', { name: 'Capture' }).click();
  await admin
    .getByRole('dialog', { name: /search/ })
    .getByRole('button', { name: 'Start' })
    .click();
  // A hundred rows fill in seconds, and the dialog is then the save's.
  const fromMap = admin.getByRole('dialog', { name: /Save the capture/ });
  await expect(fromMap).toContainText('__destination.route', { timeout: 60_000 });
  await admin.screenshot({ path: 'results/destination-capture-map.png' });
});

test('a capture whose routes type one field two ways saves it as a variant', async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  const writing = (id: string, target: string): Pipeline => ({
    id,
    name: id,
    steps: [{ type: 'eval', id: 'set', assignments: [{ target, expr: '1' }] }],
  });
  const route = (id: string) => ({
    id,
    source: 'own-metrics',
    pipeline: id,
    deliver: [{ to: ['search'] }],
  });
  await deploy(api, 'overlapping', ['east'], {
    east: {
      ...metricsTo(es.url),
      pipelines: [writing('flat', 'a'), writing('nested', 'a.b')],
      routes: [route('flat'), route('nested')],
    },
  });
  await fleet.startWorker(leader, api, 'east', 'east-1');
  await expect.poll(() => es.indexed, { timeout: 60_000 }).toBeGreaterThan(1);

  await admin.evaluate(() => {
    localStorage.setItem('smeltr-scope', 'east');
    localStorage.setItem('smeltr-branch', 'main');
  });
  await goto(admin, leader, '#/build/destinations');
  await admin.reload();
  await admin.getByRole('button', { name: 'More actions for search' }).click();
  await admin
    .getByRole('menuitem', { name: 'Capture' })
    .or(admin.getByRole('button', { name: 'Capture' }))
    .first()
    .click();
  const dialog = admin.getByRole('dialog', { name: /search/ });
  // Enough that both routes' rows are held, not only the first batch's.
  await dialog.getByRole('spinbutton', { name: 'records' }).fill('1000');
  await dialog.getByRole('button', { name: 'Start' }).click();

  const saving = admin.getByRole('dialog', { name: /Save the capture/ });
  await expect(saving).toBeVisible({ timeout: 60_000 });
  await saving.getByLabel('Save as').fill('both routes');
  await saving.getByRole('button', { name: 'Save sample' }).click();
  const branch = admin.getByRole('dialog', { name: /Branch required/i });
  await branch.getByLabel('branch to edit on').fill('overlap');
  await branch.getByRole('button', { name: 'Edit on this branch' }).click();

  await expect(saving).toBeHidden();
  await admin.screenshot({ path: 'results/destination-capture-variant.png' });

  const { samples } = await api.get('/api/samples?branch=overlap&scope=east');
  const saved = samples.find((s: { name: string }) => s.name === 'both routes');
  expect(saved).toBeDefined();
  const { sample } = await api.get(
    `/api/samples/${encodeURIComponent(saved.id)}?branch=overlap&scope=east`,
  );
  // `a.b` makes `a` an object on one route and a number on the other.
  const a = sample.columns.find((c: { name: string }) => c.name === 'a');
  expect(a?.type).toBe('variant');
});

test("a worker's memory by part folds the runtime's parts under its whole", async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  await deploy(api, 'lanes', ['east'], { east: metricsTo(es.url) });
  await fleet.startWorker(leader, api, 'east', 'east-1');

  await goto(admin, leader, '#/status');
  await admin.getByRole('button', { name: /^east-1\b/ }).click();
  const parts = admin.locator('details.memory-parts');
  await expect(parts).toBeVisible({ timeout: 30_000 });
  await parts.locator('summary').click();
  const row = (name: string | RegExp) =>
    parts.getByRole('row').filter({ has: admin.getByRole('cell', { name, exact: true }) });
  // A measured part reads in bytes, even when empty; one nothing measures reads "--".
  await expect(row('supervisor').getByRole('cell').nth(1)).toHaveText(/\d/, { timeout: 30_000 });
  // The runtime's cell holds its toggle too, which names itself.
  await expect(
    row(/runtime$/)
      .getByRole('cell')
      .nth(1),
  ).toHaveText(/\d/);
  await expect(row('lanes')).toHaveCount(0);

  await parts.getByRole('button', { name: 'Expand runtime' }).click();
  await expect(row('overhead')).toBeVisible();
  await expect(row('lanes').getByRole('cell').nth(1)).toHaveText(/\d/);

  await admin.getByRole('button', { name: 'About Memory by part' }).click();
  await expect(admin.getByText(/^Used: memory the worker holds now\./)).toBeVisible();
});

test("a worker's partitions and threads fold, closed until opened", async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  await deploy(api, 'lanes', ['east'], { east: metricsTo(es.url) });
  await fleet.startWorker(leader, api, 'east', 'east-1');

  await goto(admin, leader, '#/status');
  await admin.getByRole('button', { name: /^east-1\b/ }).click();
  const threads = admin.locator('details.threads');
  await expect(threads.locator('summary')).toHaveText(/^Thread breakdown \([1-9]/, {
    timeout: 30_000,
  });
  await expect(admin.locator('details.memory-parts')).toBeVisible({ timeout: 30_000 });
  await admin.screenshot({ path: 'results/worker-folds-closed.png', fullPage: true });
  const line = threads.locator('.thread-line').first();
  await expect(line).toBeHidden();
  await threads.locator('summary').click();
  await expect(line).toBeVisible();

  const partitions = admin.locator('details.partitions');
  await expect(partitions.getByText('No partitions owned.')).toBeHidden();
  await partitions.locator('summary').click();
  await expect(partitions.getByText('No partitions owned.')).toBeVisible();
  await admin.screenshot({ path: 'results/worker-folds-open.png', fullPage: true });
});

test("a worker's memory by part reads what an API source holds", async ({
  admin,
  api,
  fleet,
  leader,
  sink,
}) => {
  const es = await sink();
  const bundle = metricsTo(es.url);
  bundle.sources!.push({ id: 'poll', type: 'httpApi', url: es.url, pollIntervalSecs: 1 });
  bundle.routes!.push({
    id: 'polled',
    source: 'poll',
    pipeline: 'pass',
    deliver: [{ to: ['search'] }],
  });
  await deploy(api, 'answer', ['east'], { east: bundle });
  await fleet.startWorker(leader, api, 'east', 'east-1');

  await goto(admin, leader, '#/status');
  await admin.getByRole('button', { name: /^east-1\b/ }).click();
  const parts = admin.locator('details.memory-parts');
  await expect(parts).toBeVisible({ timeout: 30_000 });
  await parts.locator('summary').click();
  await parts.getByRole('button', { name: 'Expand runtime' }).click();
  const answer = parts.getByRole('row').filter({ hasText: /^\s*answer\s*source poll/ });
  await expect(answer.getByRole('cell').nth(1)).toHaveText(/\d/, { timeout: 30_000 });
});

/** A UDP port nothing on this host holds now. */
async function freeUdpPort(): Promise<number> {
  const socket = dgram.createSocket('udp4');
  await new Promise<void>((resolve) => socket.bind(0, '127.0.0.1', resolve));
  const { port } = socket.address();
  await new Promise<void>((resolve) => socket.close(() => resolve()));
  return port;
}
