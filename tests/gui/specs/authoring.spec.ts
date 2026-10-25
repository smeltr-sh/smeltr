// The data analyst's stories (A1-A10 in docs/user-stories.md): samples, the schema a sample proposes, where a sample
// is thin, a route preview, samples locked as tests, bad rows, lookup tables, and sources and destinations from a
// form. Each test drives the real binary through the GUI from an empty data directory; `api` only arranges what a
// test is not about.
//
// A fresh browser edits branch `work` in the `shared` scope, which is where `api` arranges things too.
import { test, expect, goto, type Api, type Leader } from '../fleet';
import type { Page } from '@playwright/test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const ON = '?branch=work&scope=shared';

/** Captures three records in the open capture dialog and saves them as the sample `name`. */
async function captureThree(page: Page, name: string) {
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('spinbutton', { name: 'records' }).fill('3');
  await dialog.getByRole('spinbutton', { name: 'seconds' }).fill('60');
  await dialog.getByRole('button', { name: 'Start' }).click();
  const capture = page.getByRole('dialog', { name: /Save the capture/ });
  await expect(capture).toBeVisible({ timeout: 90_000 });
  await expect(capture.getByText(/3\s*of 3/)).toBeVisible();
  await capture.getByLabel('Save as').fill(name);
  await capture.getByRole('button', { name: 'Save sample' }).click();
  await expect(capture).toBeHidden();
}

// A1, a sample that no longer loads -- written in a shape this build does not read -- is not lost from view.
test('A1: a sample that does not load is listed on the Samples page and can be deleted', async ({
  admin,
  api,
  leader,
}) => {
  await api.put(`/api/samples${ON}`, {
    id: 'old',
    name: 'old',
    records: [{ a: 1 }],
  });
  // Rewritten straight into the leader's repository, since no route saves a sample it cannot read.
  const clone = fs.mkdtempSync(path.join(os.tmpdir(), 'old-sample-'));
  const git = (...args: string[]) =>
    execFileSync('git', ['-c', 'user.name=t', '-c', 'user.email=t@t', ...args], { cwd: clone });
  git(
    'clone',
    '-q',
    '-b',
    'work',
    path.join(leader.proc.dir, 'smeltr-leader-data/repos/config.git'),
    '.',
  );
  fs.writeFileSync(
    path.join(clone, 'shared/samples/old.json'),
    '{"id": "old", "name": "old", "records": ["x"]}',
  );
  git('commit', '-qam', 'an old sample');
  git('push', '-q', 'origin', 'work');
  fs.rmSync(clone, { recursive: true });

  await goto(admin, leader, '#/build');
  const unreadable = admin.getByRole('alert').filter({ hasText: 'shared/samples/old.json' });
  await expect(unreadable).toContainText('expected a map');
  await expect(unreadable.getByRole('link', { name: 'Edit' })).toBeVisible();
  await admin.screenshot({ path: 'results/unreadable-sample.png' });
  await unreadable.getByRole('link', { name: 'Edit' }).click();
  await expect(admin.getByRole('textbox')).toHaveValue(/"records": \["x"\]/);
  await admin.screenshot({ path: 'results/unreadable-sample-edit.png' });
  await admin.getByRole('link', { name: 'Back' }).click();
  await unreadable.getByRole('button', { name: 'Delete' }).click();
  await admin.getByRole('dialog').getByRole('button', { name: 'Delete' }).click();
  await expect(unreadable).toBeHidden();
  await expect(admin.getByText('No samples yet')).toBeVisible();
  await admin.screenshot({ path: 'results/unreadable-sample-deleted.png' });
});

// A1, real records to work from.
test('A1: a pasted sample is saved under its name and is there to come back to', async ({
  admin,
  leader,
}) => {
  await goto(admin, leader, '#/build');
  await expect(admin.getByRole('heading', { name: 'Samples' })).toBeVisible();
  await admin.getByRole('button', { name: 'Add sample' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Paste' }).click();
  await dialog.getByLabel('Name').fill('orders, an ordinary hour');
  await dialog.getByLabel(/^Source it stands for/).fill('orders-west');
  await dialog.getByLabel(/^Records/).fill('{"id":1,"total":20}\n{"id":2,"total":35}\n');
  await expect(dialog.getByText('2 records')).toBeVisible();
  await dialog.getByRole('button', { name: 'Save sample' }).click();
  await expect(dialog).toBeHidden();

  const row = admin.getByRole('row', { name: /orders, an ordinary hour/ });
  await expect(row).toContainText('orders-west');
  await expect(row.getByRole('cell', { name: '2', exact: true })).toBeVisible();

  await admin.reload();
  await expect(admin.getByRole('row', { name: /orders, an ordinary hour/ })).toBeVisible();
});

// A1, real records to work from.
test('A1: an uploaded file becomes a sample, one record per line', async ({
  admin,
  leader,
}, testInfo) => {
  const file = testInfo.outputPath('checkout.log');
  fs.writeFileSync(file, 'GET /cart 200\nPOST /checkout 500\n\nGET /health 200\n');

  await goto(admin, leader, '#/build');
  await admin.getByRole('button', { name: 'Add sample' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Upload' }).click();
  await dialog.getByLabel(/^File/).setInputFiles(file);
  await expect(dialog.getByLabel('Name')).toHaveValue('checkout');
  await expect(dialog.getByText('3 records')).toBeVisible();
  await dialog.getByRole('button', { name: 'Save sample' }).click();
  await expect(dialog).toBeHidden();

  const row = admin.getByRole('row', { name: /checkout/ });
  await expect(row).toContainText('pasted');
  await expect(row.getByRole('cell', { name: '3', exact: true })).toBeVisible();
});

// A1, real records to work from.
test("A1: a capture from smeltr's own metrics source, read by a worker, is saved as a sample", async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 1 }]);
  await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build');
  await admin.getByRole('button', { name: 'Add sample' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Capture' }).click();
  await expect(dialog.getByRole('combobox', { name: /^Capture target/ })).toHaveValue(
    'source:own-metrics',
  );
  await expect(dialog.getByRole('combobox', { name: /^Group/ })).toHaveValue('default');
  await captureThree(admin, 'worker counters');

  const row = admin.getByRole('row', { name: /worker counters/ });
  await expect(row).toContainText('own-metrics');
  await expect(row.getByRole('cell', { name: '3', exact: true })).toBeVisible();
});

// A1, a source's card: one route to one destination is a line; a second destination turns it top to bottom, a [+]
// on a line writes a condition there, and a second pipeline is a second route from the same source.
test('A1: a route card turns at a second destination, takes conditions on its lines, and grows a pipeline', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 10 }]);
  for (const id of ['p', 'q'])
    await api.put(`/api/pipelines/${id}${ON}`, {
      id,
      name: id,
      steps: [{ type: 'parser', id: 'body', input: '_raw', kind: { type: 'json' }, declared: [] }],
    });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
    { id: 'es-b', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'b' },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.getByRole('button', { name: '+ Add route' }).click();
  const card = admin.locator('.flow[data-source="own-metrics"]');
  await expect(card).toHaveClass(/\bline\b/);
  const plus = card.getByRole('button', { name: 'add a condition' });
  // One on the line into the pipeline, one on the line out of it.
  await expect(plus).toHaveCount(2);

  await plus.first().click();
  await card.getByRole('textbox', { name: 'takes rows where' }).fill('_raw != ""');
  await expect(card).toHaveClass(/\bline\b/);

  await card.getByRole('button', { name: 'add a destination' }).click();
  await admin.locator('.menu').getByRole('button', { name: 'es-b' }).click();
  await expect(card).toHaveClass(/\btree\b/);
  await expect(card.locator('.pill')).toHaveCount(2);

  // The line to es-b, the second out of the pipeline, takes a condition of its own.
  await plus.nth(1).click();
  await card.getByRole('textbox', { name: 'sends rows where', exact: true }).fill('_raw != "x"');
  await admin.getByRole('heading', { name: 'Routes' }).click();

  // A second pipeline, wired from its socket's list.
  await card.getByRole('button', { name: 'add a pipeline' }).click();
  await card.getByRole('combobox', { name: 'pipeline' }).last().selectOption('q');
  await card
    .locator('.pipe')
    .last()
    .getByRole('button', { name: /drag to a destination/ })
    .click();
  await admin.locator('.menu').getByRole('button', { name: 'es-a' }).click();

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect.poll(async () => (await api.get(`/api/config${ON}`)).config.routes.length).toBe(2);
  const config = await api.get(`/api/config${ON}`);
  expect(config.config.routes).toEqual([
    expect.objectContaining({
      source: 'own-metrics',
      condition: '_raw != ""',
      pipeline: 'p',
      deliver: [{ to: ['es-a'] }, { when: '_raw != "x"', to: ['es-b'] }],
    }),
    expect.objectContaining({
      source: 'own-metrics',
      pipeline: 'q',
      deliver: [{ to: ['es-a'] }],
    }),
  ]);
});

// A1, one route leaves a card: its pipeline's ⋯ moves it to another source, so a card a source change merged splits
// back one route at a time.
test('A1: one route of a card moves to another source and opens on its card', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [
    { id: 'm1', type: 'metrics', bucketSeconds: 10 },
    { id: 'm2', type: 'metrics', bucketSeconds: 10 },
  ]);
  for (const id of ['p', 'q'])
    await api.put(`/api/pipelines/${id}${ON}`, { id, name: id, steps: [] });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
  ]);
  await api.put(`/api/routes${ON}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'p', deliver: [{ to: ['es-a'] }] },
    { id: 'r2', source: 'm1', condition: null, pipeline: 'q', deliver: [{ to: ['es-a'] }] },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.reload();
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  await card.locator('.pipe').last().getByRole('button', { name: 'more' }).click();
  await admin
    .locator('.menu')
    .getByRole('button', { name: /move to another source/ })
    .click();
  await admin.locator('.menu').getByRole('button', { name: 'm2' }).click();

  // The route is on its own card, open, and the one it left is a line again.
  await expect(admin.locator('.flow[data-source="m2"]')).toBeVisible();
  await expect(card).toHaveClass(/\bline\b/);
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect
    .poll(async () =>
      (await api.get(`/api/config${ON}`)).config.routes.map((r: { source: string }) => r.source),
    )
    .toEqual(['m1', 'm2']);
});

test('A1: a destination keeps its place when a condition is added on its line, and moves from its menu', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/p${ON}`, { id: 'p', name: 'p', steps: [] });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
    { id: 'es-b', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'b' },
  ]);
  await api.put(`/api/routes${ON}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'p', deliver: [{ to: ['es-a', 'es-b'] }] },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.reload();
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  const order = () => card.locator('.pill .name').allTextContents();
  await expect.poll(order).toEqual(['es-a', 'es-b']);

  await card.getByRole('button', { name: 'add a condition' }).nth(1).click();
  await expect(card.getByRole('textbox', { name: 'sends rows where', exact: true })).toBeFocused();
  await admin.keyboard.type('_raw == "x"');
  await expect.poll(order).toEqual(['es-a', 'es-b']);

  await card.locator('.pill', { hasText: 'es-a' }).getByRole('button', { name: 'more' }).click();
  await admin
    .locator('.menu')
    .getByRole('button', { name: /move right/ })
    .click();
  await expect.poll(order).toEqual(['es-b', 'es-a']);

  // The order is the rules' order, so it is saved with the route.
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect
    .poll(async () => (await api.get(`/api/config${ON}`)).config.routes[0].deliver)
    .toEqual([{ to: ['es-b'] }, { when: '_raw == "x"', to: ['es-a'] }]);
});

test('A1: a destination two pipelines send to has a pill under each, and a pipeline moves with its own', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  for (const id of ['p', 'q'])
    await api.put(`/api/pipelines/${id}${ON}`, { id, name: id, steps: [] });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
    { id: 'es-b', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'b' },
  ]);
  await api.put(`/api/routes${ON}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'p', deliver: [{ to: ['es-a'] }] },
    { id: 'r2', source: 'm1', condition: null, pipeline: 'q', deliver: [{ to: ['es-b', 'es-a'] }] },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.reload();
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  const order = () => card.locator('.pill .name').allTextContents();
  await expect.poll(order).toEqual(['es-a', 'es-b', 'es-a']);

  await card.locator('.pipe').first().getByRole('button', { name: 'more' }).click();
  await admin
    .locator('.menu')
    .getByRole('button', { name: /move right/ })
    .click();
  await expect.poll(order).toEqual(['es-b', 'es-a', 'es-a']);
});

test("A1: a condition's suggestions show over the destinations below it", async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/p${ON}`, { id: 'p', name: 'p', steps: [] });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
    { id: 'es-b', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'b' },
  ]);
  await api.put(`/api/routes${ON}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'p', deliver: [{ to: ['es-a', 'es-b'] }] },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.reload();
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  await card.getByRole('button', { name: 'add a condition' }).nth(1).click();
  await admin.keyboard.type('tr');
  const list = card.getByRole('listbox');
  await expect(list).toBeVisible();
  // What is on top where the pill under the box sits is the list, not the pill.
  const pill = await card.locator('.pill', { hasText: 'es-a' }).boundingBox();
  const onTop = await admin.evaluate(
    ([x, y]) => !!document.elementFromPoint(x, y)?.closest('[role="listbox"]'),
    [pill!.x + pill!.width / 2, pill!.y + pill!.height / 2],
  );
  expect(onTop).toBe(true);
});

test("A1: a destination two of one pipeline's lines reach has a pill for each", async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/p${ON}`, { id: 'p', name: 'p', steps: [] });
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
  ]);
  await api.put(`/api/routes${ON}`, [
    {
      id: 'r1',
      source: 'm1',
      condition: null,
      pipeline: 'p',
      deliver: [{ to: ['es-a'] }, { when: '_raw == "x"', to: ['es-a'] }],
    },
  ]);
  await goto(admin, leader, '#/build/routes');
  await admin.reload();
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  await expect.poll(() => card.locator('.pill .name').allTextContents()).toEqual(['es-a', 'es-a']);

  // Taking off the condition's pill leaves the plain line's.
  await card.locator('.pill').nth(1).getByRole('button', { name: 'more' }).click();
  await admin
    .locator('.menu')
    .getByRole('button', { name: /remove this destination/ })
    .click();
  await expect.poll(() => card.locator('.pill .name').allTextContents()).toEqual(['es-a']);
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect
    .poll(async () => (await api.get(`/api/config${ON}`)).config.routes[0].deliver)
    .toEqual([{ to: ['es-a'] }, { when: '_raw == "x"', to: [] }]);
});

// A1, a fresh install starts in Build on Routes, and a route's card makes its source there and then.
test('A1: a route is started before anything else is saved, its source made from the card', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/status');
  await admin.getByRole('link', { name: 'Build', exact: true }).click();
  await expect(admin).toHaveURL(/#\/build\/routes$/);
  await admin.getByRole('button', { name: '+ Add route' }).click();
  const source = admin.getByRole('combobox', { name: 'from source' });
  await source.selectOption({ label: '+ New source\u2026' });
  const dialog = admin.getByRole('dialog', { name: 'add source' });
  await dialog.getByRole('button', { name: /^Local, each worker/ }).click();
  await dialog.getByRole('button', { name: 'smeltr metrics', exact: true }).click();
  await admin.getByRole('dialog').getByRole('button', { name: 'Add', exact: true }).click();
  await expect(admin.getByRole('dialog')).toBeHidden();
  await expect(source).toHaveValue('metrics-1');
  // Saved there and then, as a destination made from the card is.
  const config = await api.get(`/api/config${ON}`);
  expect(config.config.sources).toContainEqual(
    expect.objectContaining({ id: 'metrics-1', type: 'metrics' }),
  );
});

// A1, a source saved with no pipeline yet gets a route, and its pipeline from a capture. A capture that proposes no
// steps says why: its records arrive as fields, or no format read them. What the leader proposes is stubbed, since
// no source in the fleet sends either.
for (const [what, proposal, note] of [
  [
    'arrive as fields',
    { read: 0, of: 0, structured: true },
    'The captured records arrive as fields, so they need no parser.',
  ],
  [
    'are in no format it reads',
    { read: 0, of: 3, structured: false },
    'No format read these records, so they need a parser of your own.',
  ],
] as const) {
  test(`A1: a capture whose records ${what} says so`, async ({ admin, leader, api, fleet }) => {
    await api.put(`/api/sources${ON}`, [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 1 }]);
    await fleet.startWorker(leader, api, 'default', 'w1');
    await admin.route('**/api/onboard/propose', (r) => r.fulfill({ json: proposal }));
    await goto(admin, leader, '#/build/routes');
    await admin.getByRole('button', { name: '+ Add route' }).click();
    await admin
      .getByRole('combobox', { name: 'pipeline' })
      .selectOption({ label: '+ New pipeline\u2026' });

    await captureThree(admin, 'own metrics');

    await expect(admin.getByText(note)).toBeVisible();
  });
}

// A1, a source no route takes yet, routed from what it reads: the map marks it, + route opens a new route on the
// Routes page, a new pipeline is made there from a capture with its parser proposed, and each Save comes back.
test('A1: a source with no route is routed from the map, its pipeline made from a capture', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  // Deployed, so a worker runs it and reports the rows no route takes.
  await api.put(`/api/sources?branch=arrive&scope=shared`, [
    { id: 'own-metrics', type: 'metrics', bucketSeconds: 1 },
  ]);
  await api.openChange('arrive', 'Read own metrics');
  await api.deploy('arrive');
  // The browser's branch, from what is deployed.
  await api.put(`/api/sources${ON}`, [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 2 }]);
  await fleet.startWorker(leader, api, 'default', 'w1');

  // Once the worker reports rows no route takes, the map draws the source, and its panel lists the stream they
  // came under.
  const card = admin.locator('a.node[title="own-metrics"]');
  const plus = admin
    .getByRole('complementary', { name: 'own-metrics' })
    .getByRole('listitem')
    .filter({ hasText: '__smeltr_metrics' })
    .getByRole('button', { name: '+ Add route' });
  await goto(admin, leader, '#/map');
  await expect(async () => {
    await admin.reload();
    await card.click({ timeout: 2_000 });
    await expect(plus).toBeVisible({ timeout: 2_000 });
  }).toPass({ timeout: 60_000 });
  await plus.click();

  await expect(admin).toHaveURL(/#\/build\/routes\?only=/);
  const source = admin.getByRole('combobox', { name: 'from source' });
  await expect(source).toHaveValue('own-metrics');
  const pipeline = admin.getByRole('combobox', { name: 'pipeline' });
  await expect(pipeline).toHaveValue('');
  await pipeline.selectOption({ label: '+ New pipeline\u2026' });

  // The editor opens on a capture of the source.
  const dialog = admin.getByRole('dialog');
  await expect(dialog.getByRole('combobox', { name: /^Capture target/ })).toHaveValue(
    'source:own-metrics',
  );
  await expect(dialog.getByRole('combobox', { name: /^Group/ })).toHaveValue('default');
  await captureThree(admin, 'own metrics');

  // A metrics record is a line of JSON: a JSON Parser is proposed, its fields declared from the capture.
  await expect(admin.getByText('3 of 3 records read as JSON')).toBeVisible();
  // Raw JSON holds what the editor made, and its Save comes back to the route as the editor's does.
  await admin.getByRole('button', { name: 'Raw JSON' }).click();
  await expect(admin.locator('textarea.json-editor')).toHaveValue(/"type": "parser"/);
  await admin.getByRole('button', { name: 'Save', exact: true }).click();

  // Save comes back to the route, the new pipeline on it.
  await expect(admin).toHaveURL(/#\/build\/routes\?only=/);
  await expect(pipeline.locator('option:checked')).toHaveText('own-metrics-parse');
  // The route's raw JSON holds the unsaved route, and its Save comes back to the map as the page's does.
  await admin.getByRole('button', { name: 'Edit JSON' }).click();
  await expect(admin.locator('textarea.json-editor')).toHaveValue(/"pipeline": "pipeline-/);
  await admin.getByRole('button', { name: 'Save', exact: true }).click();

  // And Save there comes back to the map. The warning stays until the route is deployed and an hour passes with
  // none of the stream's rows untaken.
  await expect(admin).toHaveURL(/#\/map/);

  const config = await api.get(`/api/config${ON}`);
  // Both ids are minted; the pipeline is named for the source.
  const made = config.config.pipelines.find(
    (p: { name: string }) => p.name === 'own-metrics-parse',
  );
  expect(made.id).toMatch(/^pipeline-/);
  expect(config.config.routes).toContainEqual(
    expect.objectContaining({
      id: expect.stringMatching(/^route-/),
      source: 'own-metrics',
      pipeline: made.id,
    }),
  );
  expect(made.steps[0]).toMatchObject({
    type: 'parser',
    input: '_raw',
    kind: { type: 'json' },
  });
  expect(made.steps[0].declared.map((f: { name: string }) => f.name)).toEqual(
    expect.arrayContaining(['metric', 'value', 'ts']),
  );
});

// A1, records from a chosen point rather than only what arrives now.
test('A1: a capture of a Kafka source starts where it is asked to', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  await api.put(`/api/sources${ON}`, [
    { id: 'orders', type: 'kafka', brokers: '127.0.0.1:9', topics: ['orders'], groupId: 'orders' },
  ]);
  await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build');
  await admin.getByRole('button', { name: 'Add sample' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Capture' }).click();
  await expect(dialog.getByRole('combobox', { name: /^Capture target/ })).toHaveValue(
    'source:orders',
  );
  await expect(dialog.getByRole('combobox', { name: /^Group/ })).toHaveValue('default');
  await expect(dialog.getByText('defaults', { exact: true })).toBeVisible();
  await dialog.getByRole('button', { name: /Advanced/ }).click();
  await dialog.getByRole('combobox', { name: /^Start at/ }).selectOption('timestamp');
  const from = dialog.getByLabel(/^from/);
  const chosen = await from.inputValue();
  await from.fill('');
  await expect(from, 'a cleared time shows the time still sent').toHaveValue(chosen);
  await admin.screenshot({ path: test.info().outputPath('capture-start-cleared-time.png') });
  await dialog.getByRole('combobox', { name: /^Start at/ }).selectOption('earliest');
  await expect(dialog.getByText('changed from defaults')).toBeVisible();

  const sent = admin.waitForRequest(
    (r) => r.url().endsWith('/api/captures') && r.method() === 'POST',
  );
  await dialog.getByRole('button', { name: 'Start', exact: true }).click();
  expect((await sent).postDataJSON().start).toEqual({ type: 'earliest' });
});

// A1, at a destination: the same form, wherever it is opened, offers sources and destinations.
test('A1: a capture from the pipeline page can target a destination', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [{ id: 'storefront', type: 'logs' }]);
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-web', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'web' },
  ]);

  await goto(admin, leader, '#/build');
  await admin.getByRole('button', { name: 'Add sample' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Capture' }).click();
  const target = dialog.getByRole('combobox', { name: /^Capture target/ });
  await expect(target).toHaveValue('source:storefront');
  await target.selectOption({ label: 'es-web' });
  await expect(admin.getByRole('dialog', { name: /capture at es-web/ })).toBeVisible();
  await admin.screenshot({ path: test.info().outputPath('capture-target-destination.png') });

  const sent = admin.waitForRequest(
    (r) => r.url().endsWith('/api/captures') && r.method() === 'POST',
  );
  await dialog.getByRole('button', { name: 'Start', exact: true }).click();
  const body = (await sent).postDataJSON();
  expect(body.destination).toBe('es-web');
  expect(body.source).toBeUndefined();
});

/** A pipeline of one JSON parser over `_raw` that declares `declared`, and a sample of `records` standing for
 * source `source`. */
async function jsonPipeline(
  api: Api,
  id: string,
  declared: { name: string; type: string }[],
  sample: { id: string; name: string; source?: string; records: string[] },
) {
  await api.put(`/api/pipelines/${id}${ON}`, {
    id,
    name: id,
    steps: [{ type: 'parser', id: 'body', input: '_raw', kind: { type: 'json' }, declared }],
  });
  await api.put(`/api/samples${ON}`, {
    id: sample.id,
    name: sample.name,
    source: sample.source,
    records: sample.records.map((raw) => ({ _raw: raw })),
  });
}

const ORDERS = [
  '{"id":1,"total":20,"coupon":"SPRING"}',
  '{"id":2,"total":"35.50"}',
  '{"id":3,"total":12,"coupon":"SPRING"}',
];

// A1, a saved sample cut down to the records that matter, and a record fixed or added.
test('A1: a saved sample is edited where its row is, and keeps where each record was read from', async ({
  admin,
  leader,
  api,
}) => {
  const read = (partition: number, offset: number, raw: string) => ({
    name: 'orders-west',
    type: 'kafka',
    topic: 'orders',
    partition,
    offset,
    bytes: btoa(raw),
  });
  await api.put(`/api/samples${ON}`, {
    id: 'busy-hour',
    name: 'orders, a busy hour',
    source: 'orders-west',
    columns: [
      { name: '_raw', type: 'str' },
      { name: '__source.topic', type: 'str' },
      { name: '__source.partition', type: 'i64' },
      { name: '__source.offset', type: 'i64' },
    ],
    records: ORDERS.map((raw, i) => ({ _raw: raw, __source: read(i, 100 + i, raw) })),
  });
  await api.put(`/api/tests${ON}`, {
    id: 'orders-parse',
    name: 'orders parse',
    pipeline: 'orders',
    sample: 'busy-hour',
    lockedAt: 0,
    rowsIn: 3,
    expected: [null, null, null],
  });

  await goto(admin, leader, '#/build');
  await admin
    .getByRole('row', { name: /orders, a busy hour/ })
    .getByRole('button', { name: 'Edit' })
    .click();
  const records = admin.getByRole('group', { name: 'Records' });
  await expect(records.getByRole('cell', { name: /Read from/ })).toContainText(
    'partition · offset',
  );
  await expect(records.getByRole('row', { name: /coupon.*SPRING.*0 · 100/ })).toBeVisible();
  await expect(admin.getByText('Re-lock after saving: orders parse')).toBeVisible();

  // Keep the first and last, fix the last, and paste one more.
  await records.getByRole('checkbox', { name: 'Select record 1' }).check();
  await records.getByRole('checkbox', { name: 'Select record 3' }).check();
  await expect(records.getByText('2 of 3 selected')).toBeVisible();
  // A tick chooses a record without opening it.
  await expect(records.getByRole('textbox')).toHaveCount(0);
  await records.getByRole('button', { name: 'Keep only selected' }).click();
  await expect(records.getByRole('checkbox', { name: 'Select record 2' })).toHaveCount(0);
  await records.getByRole('cell', { name: /"id":3/ }).click();
  await records.getByRole('textbox', { name: '_raw, record 3' }).fill('{"id":3,"total":13}');
  // Enter settles the cell; it does not save the sample.
  await records.getByRole('textbox', { name: '_raw, record 3' }).press('Enter');
  await expect(records).toBeVisible();
  await admin.getByLabel('Add records').fill('{"id":4,"total":1}');
  await admin.screenshot({ path: 'results/sample-edit-kafka.png', fullPage: true });
  await admin.getByRole('button', { name: 'Save sample' }).click();
  await expect(records).toBeHidden();
  await expect(admin.getByRole('row', { name: /^orders, a busy hour orders-west/ })).toContainText(
    '3',
  );

  const saved = (await api.get(`/api/samples/busy-hour${ON}`)).sample;
  expect(saved.records).toEqual([
    { _raw: ORDERS[0], __source: read(0, 100, ORDERS[0]) },
    // The bytes a byte record is read from are the edited text.
    { _raw: '{"id":3,"total":13}', __source: read(2, 102, '{"id":3,"total":13}') },
    { _raw: '{"id":4,"total":1}' },
  ]);
});

// A1, a captured value that failed its type is shown failing, and fixed in place.
test('A1: a SQL sample is edited a column at a time, and a value fixed to its type stops failing', async ({
  admin,
  leader,
  api,
}) => {
  const db = { name: 'billing-db', type: 'postgres', database: 'billing' };
  await api.put(`/api/samples${ON}`, {
    id: 'invoices',
    name: 'invoices',
    source: 'billing-db',
    columns: [
      { name: 'invoice_id', type: 'i64' },
      { name: 'amount', type: 'f64' },
      { name: '__source.database', type: 'str' },
    ],
    records: [
      { invoice_id: 1, amount: 'n/a', smeltr: { mistyped: ['amount'] }, __source: db },
      { invoice_id: 2, amount: 3.5, __source: db },
    ],
  });

  await goto(admin, leader, '#/build');
  await admin
    .getByRole('row', { name: /invoices/ })
    .getByRole('button', { name: 'Edit' })
    .click();
  const records = admin.getByRole('group', { name: 'Records' });
  await expect(records.getByRole('cell', { name: /^amount/ })).toContainText('f64');
  await expect(records.getByRole('cell', { name: /Read from/ })).toHaveCount(0);
  await records.getByRole('cell', { name: 'n/a' }).click();
  const amount = records.getByRole('textbox', { name: 'amount, record 1' });
  await expect(amount).toHaveAttribute('aria-invalid', 'true');
  await admin.screenshot({ path: 'results/sample-edit-sql.png', fullPage: true });
  await amount.fill('20');
  await amount.press('Tab');
  await expect(amount).toHaveAttribute('aria-invalid', 'false');
  await admin.getByRole('button', { name: 'Save sample' }).click();
  await expect(records).toBeHidden();

  const saved = (await api.get(`/api/samples/invoices${ON}`)).sample;
  expect(saved.records[0]).toEqual({ invoice_id: 1, amount: 20, __source: db });
});

// A2, schema from a sample.
test('A2: the parser proposes its declared fields from a sample, and the analyst accepts them', async ({
  admin,
  leader,
  api,
}) => {
  await jsonPipeline(api, 'orders', [], { id: 'orders-sample', name: 'orders', records: ORDERS });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  // The sample in the URL is chosen once the samples have loaded; Suggest fields before then has none to read.
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();
  await admin.getByRole('button', { name: /_raw as JSON/ }).click();
  await admin.getByRole('button', { name: 'Suggest fields' }).click();

  // Suggested in the step's own card: the sample stays where it was.
  const suggested = admin.getByRole('group', { name: 'Suggested fields' });
  await expect(suggested).toContainText('Suggested from orders, 3 rows');
  await expect(admin.getByRole('button', { name: 'Choose a sample' })).toContainText('orders');
  await expect(suggested.getByRole('group', { name: 'id' }).getByRole('combobox')).toHaveValue(
    'i64',
  );
  await expect(suggested.getByRole('group', { name: 'coupon' }).getByRole('combobox')).toHaveValue(
    'str',
  );
  await admin.screenshot({ path: 'results/suggest-fields.png', fullPage: true });

  // A suggestion renamed before it is added is declared under its new name, and leaves the list.
  const coupon = suggested.getByRole('group', { name: 'coupon' });
  await coupon.getByRole('textbox', { name: 'suggested field name' }).fill('discount');
  await coupon.getByRole('button', { name: 'Add' }).click();
  await expect(suggested.getByRole('group')).toHaveCount(2);

  await suggested.getByRole('button', { name: 'Add all' }).click();
  await expect(suggested.getByRole('group')).toHaveCount(0);
  await suggested.getByRole('button', { name: 'Done' }).click();
  await expect(
    admin.getByRole('button', { name: /^Parser _raw as JSON \(3 declared\)/ }),
  ).toBeVisible();

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Saved.')).toBeVisible();
  const saved = await api.get(`/api/pipelines/orders${ON}`);
  const names = saved.pipeline.steps[0].declared.map((f: { name: string }) => f.name);
  expect(names.sort()).toEqual(['discount', 'id', 'total']);
});

// A3, where the sample is thin.
test('A3: the proposal says which fields some rows lack and where rows disagree about a type', async ({
  admin,
  leader,
  api,
}) => {
  await jsonPipeline(api, 'orders', [], { id: 'orders-sample', name: 'orders', records: ORDERS });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();
  await admin.getByRole('button', { name: /_raw as JSON/ }).click();
  await admin.getByRole('button', { name: 'Suggest fields' }).click();

  const suggested = admin.getByRole('group', { name: 'Suggested fields' });
  await expect(suggested.getByRole('group', { name: 'coupon' })).toContainText(
    'absent from 1 of 3 rows',
  );
  await expect(suggested.getByRole('group', { name: 'total' })).toContainText('rows disagree');
  await expect(suggested.getByRole('group', { name: 'id' })).not.toContainText('absent');
});

// A2, choosing what to run over. The editor opens on no sample, still compiling the steps, and the samples a test
// of this pipeline locks come first in the Sample menu, marked. The output is searched by keyword, or filtered by an
// expression.
test('A2: a sample is picked from the Sample menu, its output searched by keyword or expression', async ({
  admin,
  leader,
  api,
}) => {
  await jsonPipeline(api, 'orders', [{ name: 'total', type: 'f64' }], {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS,
  });
  await api.put(`/api/samples${ON}`, { id: 'other', name: 'other', records: [{ _raw: '{}' }] });
  await api.put(`/api/tests${ON}`, {
    id: 'orders-parse',
    name: 'orders parse',
    pipeline: 'orders',
    sample: 'orders-sample',
    lockedAt: 0,
    rowsIn: 3,
    expected: [null, null, null],
  });

  await goto(admin, leader, '#/build/pipelines/orders');
  const sampleMenu = admin.getByRole('button', { name: 'Choose a sample' });
  // No sample, but the steps compiled: the Fields menu lists what they declare.
  await admin.getByRole('button', { name: 'Choose fields' }).click();
  const fields = admin.getByRole('group', { name: 'Fields' });
  await expect(fields.getByText('total', { exact: true })).toBeVisible();
  await admin.keyboard.press('Escape');

  // The locked sample first, though `other` sorts before it by name.
  await sampleMenu.click();
  const saved = admin.getByRole('listbox', { name: 'Saved samples' });
  await expect(saved.getByRole('option').first()).toContainText('orders');
  await expect(saved.getByTitle('Locked by the test orders parse')).toBeVisible();
  await saved.getByRole('option', { name: /\borders 3 records$/ }).click();
  await expect(sampleMenu).toContainText('3 records');
  const search = admin.getByRole('combobox', { name: 'Search the output' });
  await search.fill('SPRING');
  await expect(admin.getByText('2 of 3 rows')).toBeVisible();
  await expect(admin.getByText('keyword', { exact: true })).toBeVisible();
  await search.fill('total > 15');
  await expect(admin.getByText('ƒ expression')).toBeVisible();
  await expect(admin.getByText('2 of 3 rows')).toBeVisible();
  // One that does not compile to a Bool is a keyword.
  await search.fill('total +');
  await expect(admin.getByText('keyword', { exact: true })).toBeVisible();
  // Left with an expression in it, the box offers it again, under the kinds of row a chip keeps.
  await search.fill('total > 15');
  await expect(admin.getByText('ƒ expression')).toBeVisible();
  await search.blur();
  await search.fill('');
  const offered = admin.getByRole('listbox', { name: 'Suggestions' });
  await expect(offered.getByRole('option', { name: 'total > 15' })).toBeVisible();
  await offered.getByRole('option', { name: /Changed by a step/ }).click();
  await expect(admin.getByRole('button', { name: 'remove changed' })).toBeVisible();
  await expect(admin.getByText('3 of 3 rows')).toBeVisible();
  await admin.screenshot({ path: 'results/sample-pane-search.png', fullPage: true });

  // Another sample, from the menu: the locked one first, and the search starts over.
  await sampleMenu.click();
  await saved.getByRole('option', { name: /^other/ }).click();
  await expect(sampleMenu).toContainText('1 record');
  await expect(search).toHaveValue('');
});

// A2, records the workspace has no sample for yet: pasted, or captured from a source the pipeline is fed by, from the
// Sample menu. Either is saved and is what the workspace runs over.
test('A2: a sample pasted or captured from the Sample menu is saved and run', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  await jsonPipeline(api, 'orders', [{ name: 'total', type: 'f64' }], {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS,
  });
  await api.put(`/api/sources${ON}`, [{ id: 'own-metrics', type: 'metrics', bucketSeconds: 1 }]);
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
  ]);
  await api.put(`/api/routes${ON}`, [
    {
      id: 'r1',
      source: 'own-metrics',
      condition: null,
      pipeline: 'orders',
      deliver: [{ to: ['es-a'] }],
    },
  ]);
  await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build/pipelines/orders');
  const sampleMenu = admin.getByRole('button', { name: 'Choose a sample' });
  await sampleMenu.click();
  await admin.getByRole('button', { name: 'Paste or file' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Paste' }).click();
  await dialog.getByLabel('Name').fill('two orders');
  await dialog.getByLabel(/^Records/).fill('{"id":7,"total":1}\n{"id":8,"total":2}\n');
  await dialog.getByRole('button', { name: 'Save sample' }).click();
  await expect(dialog).toBeHidden();
  await expect(sampleMenu).toContainText('two orders');
  await expect(sampleMenu).toContainText('2 records');

  await sampleMenu.click();
  await admin.getByRole('tab', { name: /^Capture/ }).click();
  await admin
    .getByRole('listbox', { name: 'Sources' })
    .getByRole('option', { name: /own-metrics/ })
    .click();
  await captureThree(admin, 'metrics now');
  await expect(sampleMenu).toContainText('metrics now');
  await expect(sampleMenu).toContainText('3 records');
});

/** A source, destinations and a `web` pipeline routed from one to them: every record to `es-web`, a server error
 * to `kafka-alerts` too. `es-dlq` is there for an exception answer to name. */
async function webRoutes(api: Api, sampleRecords: string[]) {
  await api.put(`/api/sources${ON}`, [{ id: 'storefront', type: 'logs' }]);
  await api.put(`/api/destinations${ON}`, [
    { id: 'es-web', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'web' },
    { id: 'kafka-alerts', type: 'kafka', brokers: '127.0.0.1:9', defaultTopic: 'alerts' },
    { id: 'es-dlq', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'dlq' },
  ]);
  await jsonPipeline(api, 'web', [{ name: 'status', type: 'i64' }], {
    id: 'web-sample',
    name: 'storefront hour',
    source: 'storefront',
    records: sampleRecords,
  });
  await api.put(`/api/routes${ON}`, [
    {
      id: 'web',
      source: 'storefront',
      condition: null,
      pipeline: 'web',
      deliver: [{ to: ['es-web'] }, { when: 'status >= 500', to: ['kafka-alerts'] }],
    },
  ]);
}

// A4, the whole trip.
// The default shows the fields most records hold, so it moves with the sample. A field the reader
// chose stays as they chose it.
test('A2: a field shown from the Fields menu stays shown on a sample that would have hidden it', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/wide${ON}`, {
    id: 'wide',
    name: 'wide',
    steps: [{ type: 'parser', id: 'body', input: '_raw', kind: { type: 'json' } }],
  });
  // 55 fields. In `few`, f50 to f54 are in one record of three, so the 50 shown by default leave
  // them out; in `many`, f0 to f4 are, so f52 is in the default there.
  const record = (rare: (i: number) => boolean, i: number) =>
    Object.fromEntries(
      Array.from({ length: 55 }, (_, f) => [`f${f}`, f]).filter(
        ([, f]) => !rare(f as number) || i === 0,
      ),
    );
  for (const [id, rare] of [
    ['few', (f: number) => f >= 50],
    ['many', (f: number) => f < 5],
  ] as const)
    await api.put(`/api/samples${ON}`, {
      id,
      name: id,
      records: [0, 1, 2].map((i) => ({ _raw: JSON.stringify(record(rare, i)) })),
    });

  const grid = admin.getByRole('table');
  const head = grid.getByRole('button', { name: /^f52\b/ });
  await goto(admin, leader, '#/build/pipelines/wide/for/few');
  await expect(grid.getByRole('row', { name: /^row 0 / })).toBeVisible();
  await expect(head).toHaveCount(0);
  await admin.getByRole('button', { name: 'Choose fields' }).click();
  await admin.getByRole('searchbox', { name: 'Find a field' }).fill('f52');
  await admin.getByRole('group', { name: 'Fields' }).getByRole('checkbox').check();
  await admin.keyboard.press('Escape');
  await expect(head).toHaveCount(1);

  const sampleMenu = admin.getByRole('button', { name: 'Choose a sample' });
  await sampleMenu.click();
  await admin
    .getByRole('listbox', { name: 'Saved samples' })
    .getByRole('option', { name: /^many/ })
    .click();
  await expect(sampleMenu).toContainText('many');
  await expect(grid.getByRole('row', { name: /^row 0 / })).toBeVisible();
  await expect(head).toHaveCount(1);
});

test('A4: a route preview shows each output row, the route it matched and the destinations it reaches', async ({
  admin,
  leader,
  api,
}) => {
  await webRoutes(api, ['{"status":200}', '{"status":503}']);

  await goto(admin, leader, '#/build/routes/sample');
  await admin.getByRole('link', { name: 'Where these records would go' }).click();
  await expect(admin.getByRole('heading', { name: 'Where these records would go' })).toBeVisible();
  await expect(admin.getByRole('combobox', { name: 'Sample' })).toHaveValue('web-sample');

  // The page runs on arrival; each tile counts what one destination would be sent.
  await expect(admin.getByRole('button', { name: '2 es-web' })).toBeVisible();
  await expect(admin.getByRole('button', { name: '1 kafka-alerts' })).toBeVisible();

  const table = admin.getByRole('table');
  const ok = table.getByRole('row', { name: /^0 web/ });
  await expect(ok).toContainText('es-web');
  await expect(ok).not.toContainText('kafka-alerts');
  await expect(ok).toContainText('"status":200');
  const failed = table.getByRole('row', { name: /^1 web/ });
  await expect(failed).toContainText('es-web, kafka-alerts');
  await expect(failed).toContainText('"status":503');
});

// A4, the whole trip. A step that does not compile says why once, on the step; the error chip above the records
// lists it and takes the author there, and the records stand after the last step that compiles.
test('A4: a compile error is written once, on its step, and the error chip jumps to it', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/orders${ON}`, {
    id: 'orders',
    name: 'orders',
    steps: [
      { type: 'parser', id: 'body', input: '_raw', kind: { type: 'json' }, declared: [] },
      { type: 'code', id: 'code-1', filter: 'true', disabled: false, body: 'x = 1;' },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS.map((raw) => ({ _raw: raw })),
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();
  await admin.getByRole('button', { name: /line\(s\) of code/ }).click();
  const body = admin.locator('textarea').last();
  await body.fill('x = ipVersion();');

  const message = '`ipVersion` takes 1 argument, found 0';
  const failed = admin.getByRole('button', { name: /Compile failed · 1/ });
  await expect(failed).toBeVisible();
  // Focus is where the field's popup used to repeat it.
  await body.focus();
  await expect(admin.getByText(message, { exact: true })).toHaveCount(1);
  // The records stand where the pipeline still compiles: after the parser.
  await expect(admin.getByLabel('Paused')).toHaveText('◉ After step 1 · PARSER');
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();

  await failed.click();
  await admin.getByRole('button', { name: /takes 1 argument, found 0 Jump/ }).click();
  await expect(body).toBeInViewport();
});

// ◎ pauses the records after a step: the steps after it are dimmed, a record a later step drops is back, and ✕
// shows the whole pipeline again.
test('◎ pauses the records after a step', async ({ admin, leader, api }) => {
  await api.put(`/api/pipelines/orders${ON}`, {
    id: 'orders',
    name: 'orders',
    steps: [
      { type: 'parser', id: 'body', input: '_raw', kind: { type: 'json' }, declared: [] },
      {
        type: 'drop',
        id: 'no-coupon',
        filter: 'true',
        disabled: false,
        condition: 'coupon == null',
      },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS.map((raw) => ({ _raw: raw })),
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  const grid = admin.getByRole('table');
  await expect(grid.getByRole('row', { name: /^row 0 / })).toBeVisible();
  const dropped = grid.getByRole('row', { name: /^row 1 / });
  await expect(dropped).toHaveClass(/dropped/);

  await admin.getByRole('button', { name: 'Pause the records after this step' }).first().click();
  const paused = admin.getByLabel('Paused');
  await expect(paused).toHaveText(/◉ After step 1 · PARSER/);
  await expect(dropped).not.toHaveClass(/dropped/);
  await expect(admin.locator('#step-no-coupon')).toHaveClass(/not-run/);

  await paused.getByRole('button', { name: 'Show the whole pipeline' }).click();
  await expect(paused).toBeHidden();
  await expect(dropped).toHaveClass(/dropped/);
});

// A3, which step did it. Once typing settles, a field carries the sign of the step that last touched it, held it
// lists every step that did and jumps to one, an exception names its step, and a step's dropped and failed counts
// filter the records to those it dropped or failed.
test('A3: a field names the steps that touched it, a step card counts what it dropped, a column its type', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/web${ON}`, {
    id: 'web',
    name: 'web',
    steps: [
      {
        type: 'parser',
        id: 'body',
        input: '_raw',
        kind: { type: 'json' },
        declared: [
          { name: 'status', type: 'i64' },
          { name: 'path', type: 'str' },
        ],
      },
      { type: 'drop', id: 'health', condition: 'path == "/health"' },
      { type: 'code', id: 'calc', body: 'path = "web" + path;' },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'web',
    name: 'web hour',
    records: [
      { _raw: '{"status": 200, "path": "/a"}' },
      { _raw: '{"status": "oops", "path": "/b"}' },
      { _raw: '{"status": 200, "path": "/health"}' },
    ],
  });

  await goto(admin, leader, '#/build/pipelines/web/for/web');
  const grid = admin.getByRole('table');
  await expect(grid.getByRole('row', { name: /^row 0 / })).toBeVisible();
  // The failing row opens first, and its exception names the parser.
  await admin.getByRole('button', { name: 'Step 1 · Parser · _raw as JSON (2 declared)' }).click();
  await expect(admin.locator('#step-body')).toBeInViewport();

  // `path` was set by the parser and then by the code step: the sign is the last's, and the list has both.
  const record = grid.getByRole('row', { name: /^record 1/ });
  const last = record.getByRole('button', { name: /^Step 3 · Code/ });
  await expect(last).toBeVisible();
  await last.hover();
  const list = last.locator('xpath=following-sibling::span[1]');
  await expect(list.getByRole('button', { name: /^Step 1 · Parser/ })).toBeVisible();
  await expect(list.getByRole('button', { name: /^Step 3 · Code/ })).toBeVisible();

  await admin.locator('#step-health').getByRole('button', { name: '−1 dropped' }).click();
  await expect(admin.getByRole('button', { name: 'remove dropped by health' })).toBeVisible();
  await expect(admin.getByText('1 of 3 rows')).toBeVisible();
  await expect(grid.getByRole('row', { name: /^row 2 / })).toBeVisible();

  // A column's head names its type, until types are hidden.
  const head = grid.getByRole('button', { name: /^status\b/ });
  await expect(head).toHaveText(/^status: i64/);
  await admin.getByRole('button', { name: 'Show types' }).click();
  await expect(head).not.toContainText('i64');
});

// A4, the whole trip. A refused save's errors stay on the steps until the next save; a preview error after it is
// still written somewhere, in the error chip above the records.
test('A4: after a refused save, a new compile error is still shown', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/orders${ON}`, {
    id: 'orders',
    name: 'orders',
    steps: [{ type: 'code', id: 'code-1', filter: 'true', disabled: false, body: 'x = 1;' }],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS.map((raw) => ({ _raw: raw })),
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();
  await admin.getByRole('button', { name: /line\(s\) of code/ }).click();
  const body = admin.locator('textarea').last();
  await body.fill('x = ipVersion();');
  await expect(admin.getByRole('button', { name: /Compile failed · 1/ })).toBeVisible();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(
    admin.locator('.error-bar').getByRole('link', { name: /line\(s\) of code/ }),
  ).toBeVisible();

  await body.fill('x = ipVersion(1, 2);');
  await admin.getByRole('button', { name: /Compile failed · 1/ }).click();
  await expect(
    admin.getByText('`ipVersion` takes 1 argument, found 2', { exact: true }),
  ).toBeVisible();
});

// A4. Leaving a code field lays its code out, and what compiles but is likely a mistake is said on its step.
test('A4: a code field is laid out when it is left, and a warning is written on its step', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/orders${ON}`, {
    id: 'orders',
    name: 'orders',
    steps: [{ type: 'code', id: 'code-1', filter: 'true', disabled: false, body: 'x = 1;' }],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS.map((raw) => ({ _raw: raw })),
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  await expect(admin.getByRole('table').getByRole('row', { name: /^row 0 / })).toBeVisible();
  await admin.getByRole('button', { name: /line\(s\) of code/ }).click();
  const body = admin.locator('textarea').last();
  await body.fill('let n=1\nif(_raw!=""){x=1}else{x=2}');
  await body.blur();

  await expect(body).toHaveValue('let n = 1;\nif (_raw != "") {\n  x = 1;\n} else {\n  x = 2;\n}');
  await expect(admin.getByText('the local `n` is never read', { exact: true })).toBeVisible();
});

// A4, the whole trip. Save pressed straight from a code field saves the field as it is laid out, however slowly the
// layout comes back.
test('A4: a save pressed from a code field saves its layout', async ({ admin, leader, api }) => {
  await api.put(`/api/pipelines/orders${ON}`, {
    id: 'orders',
    name: 'orders',
    steps: [{ type: 'code', id: 'code-1', filter: 'true', disabled: false, body: 'x = 1;' }],
  });

  await goto(admin, leader, '#/build/pipelines/orders');
  await admin.getByRole('button', { name: /line\(s\) of code/ }).click();
  const body = admin.locator('textarea').last();
  await body.fill('x=2');
  await admin.route('**/api/format', async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await route.continue();
  });
  await admin.getByRole('button', { name: 'Save', exact: true }).click();

  await expect(admin.getByText('Saved.')).toBeVisible();
  await expect(body).toHaveValue('x = 2;');
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden();
  const saved = (await api.get(`/api/pipelines/orders${ON}`)).pipeline;
  expect(saved.steps[0].body).toBe('x = 2;');
});

// A6, samples become tests.
test('A6: a sample locked as a test passes, and a save that breaks it is allowed and shows it failing', async ({
  admin,
  leader,
  api,
}) => {
  await jsonPipeline(api, 'orders', [{ name: 'id', type: 'i64' }], {
    id: 'orders-sample',
    name: 'orders',
    records: ORDERS,
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  const lock = admin.getByRole('button', { name: 'Lock this as a test' });
  await expect(lock).toBeEnabled({ timeout: 15_000 });
  await lock.click();

  await expect(admin.getByRole('heading', { name: 'Lock this as a test' })).toBeVisible();
  await expect(admin.getByText('3 rows, 0 dropped')).toBeVisible();
  await admin.getByRole('textbox', { name: 'Name' }).fill('orders keep every order');
  await admin.getByRole('button', { name: 'Lock the test' }).click();

  const testRow = admin.getByRole('row', { name: /orders keep every order/ });
  await expect(testRow).toContainText('3 in / 3 out');
  await expect(testRow).toContainText('Passing', { timeout: 15_000 });

  // Break it: a step that drops the second order.
  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  await admin.getByRole('button', { name: 'Add step' }).click();
  await admin.getByRole('dialog').getByRole('button', { name: 'Drop', exact: true }).click();
  await admin.getByLabel(/^drop when this is true/).fill('id == 2');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Saved.')).toBeVisible();

  await goto(admin, leader, '#/build');
  await expect(admin.getByRole('row', { name: /orders keep every order/ })).toContainText(
    'Failing',
    { timeout: 15_000 },
  );
});

// A8, bad rows go somewhere.
test('A8: while authoring, a row that fails to parse is shown failing, with why', async ({
  admin,
  leader,
  api,
}) => {
  await jsonPipeline(api, 'orders', [{ name: 'id', type: 'i64' }], {
    id: 'orders-sample',
    name: 'orders',
    records: ['{"id":1}', '{"id":"two"}', 'not json'],
  });

  await goto(admin, leader, '#/build/pipelines/orders/for/orders-sample');
  const grid = admin.getByRole('table').filter({ hasText: '#' }).last();
  // Three rows, and the first that failed opened.
  await expect(grid.getByRole('row')).toHaveCount(5, { timeout: 15_000 });
  await expect(admin.getByRole('button', { name: 'row 1', exact: true })).toHaveAttribute(
    'aria-expanded',
    'true',
  );

  // The grid carries each exception in the row itself, and an opened row unfolds it.
  await expect(grid.getByRole('cell', { name: /"kind":"UnparsableField"/ })).toBeVisible();
  await expect(grid.getByRole('cell', { name: /"kind":"Unparsable"/ })).toBeVisible();
  // Under the row's head, after the step that raised it, and as the unfolded exception's message.
  const why = (message: string) => async () => {
    await expect(admin.getByText(message, { exact: true })).toHaveCount(1);
    await expect(grid.locator('.exception', { hasText: message })).toHaveText(
      /^\s*Step \d+ · Parser · .*: /,
    );
  };
  await why('_raw.id is "two", which could not parse as i64 in id')();
  await admin.getByRole('button', { name: 'row 2', exact: true }).click();
  await why('_raw is "not json", which json could not parse')();
});

// A8, bad rows go somewhere.
test('A8: a route sends its unparseable rows where the analyst says, and the preview shows it', async ({
  admin,
  leader,
  api,
}) => {
  await webRoutes(api, ['{"status":200}', 'not json']);

  await goto(admin, leader, '#/build/routes');
  await admin.getByRole('button', { name: /^storefront/ }).click();
  // The route's own Exceptions, in its card, come before the group's at the foot of the page.
  await admin
    .getByRole('button', { name: /^Exceptions/ })
    .first()
    .click();
  await admin.getByRole('radio', { name: 'parse: deliver' }).click();
  await admin.getByRole('combobox', { name: 'parse: add destination' }).selectOption('es-dlq');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.routes[0].exceptions).toEqual({
    categories: { parse: { deliver: ['es-dlq'] } },
  });

  await admin.getByRole('link', { name: 'Route a sample' }).click();
  await admin.getByRole('link', { name: 'Where these records would go' }).click();
  const bad = admin.getByRole('table').getByRole('row', { name: /^1 web/ });
  await expect(bad).toContainText('Unparsable');
  await expect(bad).toContainText('deliver');
  await expect(bad).toContainText('es-dlq');
  await expect(bad).not.toContainText('es-web');
  await expect(admin.getByRole('table').getByRole('row', { name: /^0 web/ })).toContainText(
    'es-web',
  );
});

/** A pipeline that parses `store` and looks it up in `table` by `keyColumn`, writing `valueColumn` as `region`, and
 * a sample of three stores. */
async function storesPipeline(api: Api, table: string, keyColumn: string, valueColumn: string) {
  await api.put(`/api/pipelines/stores${ON}`, {
    id: 'stores',
    name: 'stores',
    steps: [
      {
        type: 'parser',
        id: 'body',
        input: '_raw',
        kind: { type: 'json' },
        declared: [{ name: 'store', type: 'str' }],
      },
      {
        type: 'lookup',
        id: 'region',
        input: 'store',
        table,
        matchKind: 'exact',
        multiplicity: 'first',
        keyColumn,
        outputs: [{ source: valueColumn, field: 'region' }],
      },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'stores-sample',
    name: 'sales',
    records: ['{"store":"s1"}', '{"store":"s2"}', '{"store":"s9"}'].map((raw) => ({ _raw: raw })),
  });
}

/** Opens the `stores` pipeline over its sample and expects each store's region, `—` where none matched. */
async function expectRegions(page: Page, leader: Leader) {
  await goto(page, leader, '#/build/pipelines/stores/for/stores-sample');
  const grid = page.getByRole('table').filter({ hasText: 'region' }).last();
  await expect(grid.getByRole('row', { name: /^row 0 / })).toContainText('west', {
    timeout: 15_000,
  });
  await expect(grid.getByRole('row', { name: /^row 1 / })).toContainText('east');
  await expect(grid.getByRole('row', { name: /^row 2 / })).not.toContainText(/west|east/);
}

// A9, reference data.
test('A9: a lookup table typed in by hand enriches the records of a pipeline that names it', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/lookups');
  await expect(admin.getByRole('heading', { name: 'Lookup Tables' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add table' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Built by hand' }).click();
  await dialog.getByLabel('id').fill('regions');
  await dialog.getByRole('button', { name: 'Add' }).click();
  await expect(dialog).toBeHidden();

  await admin.getByRole('button', { name: '+ row' }).click();
  await admin.getByRole('button', { name: '+ row' }).click();
  await admin.getByLabel('row 1 key').fill('s1');
  await admin.getByLabel('row 1 value').fill('west');
  await admin.getByLabel('row 2 key').fill('s2');
  await admin.getByLabel('row 2 value').fill('east');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.lookupTables).toEqual([
    {
      id: 'regions',
      type: 'inline',
      headers: ['key', 'value'],
      rows: [
        ['s1', 'west'],
        ['s2', 'east'],
      ],
    },
  ]);

  await storesPipeline(api, 'regions', 'key', 'value');
  await expectRegions(admin, leader);
});

// A grok pattern declared once on the Grok patterns page is named by a pipeline's grok step, whose preview reads
// the field the shared pattern captures.
test('a grok pattern added on its page is named by a grok step in a pipeline', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/grok');
  await expect(admin.getByRole('heading', { name: 'Grok patterns' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add pattern' }).click();
  await admin.getByRole('textbox', { name: 'name', exact: true }).fill('WORD');
  await expect(admin.getByText('WORD is a built-in pattern')).toBeVisible();
  await admin.getByRole('textbox', { name: 'name', exact: true }).fill('STORE');
  await admin.getByRole('textbox', { name: 'STORE pattern' }).fill('store-%{INT:store}');
  // A row the search shows stays while its name is typed away from the search.
  const search = admin.getByRole('searchbox', { name: 'Search patterns' });
  await search.fill('STOR');
  await expect(admin.getByRole('cell', { name: 'WORD', exact: true })).toBeHidden();
  await admin.getByRole('textbox', { name: 'name', exact: true }).fill('SHOP');
  await expect(admin.getByRole('textbox', { name: 'SHOP pattern' })).toBeVisible();
  await admin.getByRole('textbox', { name: 'name', exact: true }).fill('STORE');
  await search.fill('');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.grokPatterns).toEqual([{ id: 'STORE', pattern: 'store-%{INT:store}' }]);

  await api.put(`/api/pipelines/stores${ON}`, {
    id: 'stores',
    name: 'stores',
    steps: [
      {
        type: 'parser',
        id: 'body',
        input: '_raw',
        kind: { type: 'grok', patterns: ['%{STORE}'] },
        declared: [{ name: 'store', type: 'i64' }],
      },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'stores-sample',
    name: 'sales',
    records: [{ _raw: 'store-7' }],
  });
  await goto(admin, leader, '#/build/pipelines/stores/for/stores-sample');
  const grid = admin.getByRole('table').filter({ hasText: 'store' }).last();
  await expect(grid.getByRole('row', { name: /^row 0 / })).toContainText('7', { timeout: 15_000 });
});

// A9, reference data. Choosing the file commits it; the Save after it must not read that commit as somebody
// else's.
test('A9: an uploaded CSV lookup table enriches the records of a pipeline that names it', async ({
  admin,
  leader,
  api,
}, testInfo) => {
  const file = testInfo.outputPath('stores.csv');
  fs.writeFileSync(file, 'store,region\ns1,west\ns2,east\n');

  await goto(admin, leader, '#/build/lookups');
  await admin.getByRole('button', { name: '+ Add table' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'CSV file' }).click();
  await dialog.getByLabel('id').fill('store-regions');
  await dialog.getByRole('button', { name: 'Add' }).click();
  await expect(dialog).toBeHidden();

  await admin.getByRole('tab', { name: 'File' }).click();
  await admin.getByLabel(/^CSV file/).setInputFiles(file);
  await expect(admin.getByText('already committed to the branch')).toBeVisible();
  await admin.getByRole('tab', { name: 'Rows' }).click();
  await expect(admin.getByLabel('row 2 region')).toHaveValue('east');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.lookupTables).toMatchObject([
    { id: 'store-regions', type: 'csv', headers: ['store', 'region'] },
  ]);

  await storesPipeline(api, 'store-regions', 'store', 'region');
  await expectRegions(admin, leader);
});

// A10, sources and destinations from a form.
test('A10: adding a source asks first how the group reads it, then shows that kind with the others a chip away', async ({
  admin,
  leader,
}) => {
  for (const scheme of ['light', 'dark'] as const) {
    await admin.emulateMedia({ colorScheme: scheme });
    await goto(admin, leader, '#/build/sources');
    await admin.getByRole('button', { name: '+ Add source' }).click();
    const dialog = admin.getByRole('dialog');
    await expect(dialog.getByRole('button', { name: /^Pull, all workers/ })).toBeVisible();
    await expect(dialog.getByRole('button', { name: /^Local, each worker/ })).toBeVisible();
    await dialog.screenshot({ path: test.info().outputPath(`source-kinds-${scheme}.png`) });

    await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
    const kinds = dialog.getByRole('group', { name: 'Kinds' });
    await expect(
      kinds.getByRole('button', { name: 'Pull, one worker', exact: true }),
    ).toHaveAttribute('aria-pressed', 'true');
    await expect(dialog.getByRole('button', { name: 'S3 listing', exact: true })).toBeVisible();
    await expect(dialog.getByRole('button', { name: 'Kafka', exact: true })).toBeHidden();

    // The chip's own `?` explains it without changing the filter.
    await kinds.getByRole('button', { name: 'About Pull, one worker' }).click();
    await expect(admin.getByText('More workers do not read it faster.')).toBeVisible();
    await expect(
      kinds.getByRole('button', { name: 'Pull, one worker', exact: true }),
    ).toHaveAttribute('aria-pressed', 'true');
    await dialog.screenshot({ path: test.info().outputPath(`source-kind-catalog-${scheme}.png`) });
    await kinds.getByRole('button', { name: 'About Pull, one worker' }).click();

    await kinds.getByRole('button', { name: 'All', exact: true }).click();
    await expect(dialog.getByRole('button', { name: 'Kafka', exact: true })).toBeVisible();

    // Back from a type's form returns to the kind chosen last, and back again to the question.
    await kinds.getByRole('button', { name: 'Pull, all workers', exact: true }).click();
    await dialog.getByRole('button', { name: 'Kafka', exact: true }).click();
    await dialog.getByRole('button', { name: 'Back', exact: true }).click();
    await expect(
      kinds.getByRole('button', { name: 'Pull, all workers', exact: true }),
    ).toHaveAttribute('aria-pressed', 'true');
    await dialog.getByRole('button', { name: 'Back', exact: true }).click();
    await expect(dialog.getByRole('button', { name: /^Pull, all workers/ })).toBeVisible();
    await dialog.getByRole('button', { name: 'Cancel' }).click();
  }
});

test("A10: a Kafka source's group id is blank under Advanced, reading as its worker group's", async ({
  admin,
  leader,
}) => {
  const groupId = async (scope: string) => {
    await admin.evaluate((s) => localStorage.setItem('smeltr-scope', s), scope);
    await goto(admin, leader, '#/build/sources');
    await admin.reload();
    await admin.getByRole('button', { name: '+ Add source' }).click();
    const dialog = admin.getByRole('dialog');
    await dialog.getByRole('button', { name: /^Pull, all workers/ }).click();
    await dialog.getByRole('button', { name: 'Kafka', exact: true }).click();
    const field = dialog.getByRole('textbox', { name: 'group id' });
    await expect(field).toBeHidden();
    await dialog.getByRole('button', { name: /^Advanced/ }).click();
    await expect(field).toHaveValue('');
    await dialog.getByRole('button', { name: 'About group id' }).click();
    await admin.screenshot({ path: test.info().outputPath(`kafka-group-id-${scope}.png`) });
    return field.getAttribute('placeholder');
  };
  expect(await groupId('shared')).toBe('smeltr.<worker group>');
  expect(await groupId('default')).toBe('smeltr.default');
});

// E8, read again from a point. Nothing reads the source here, so this checks what is saved, not the move.
test('E8: a Kafka source is moved to now by changing its start, and the page says saving moves it', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/sources${ON}`, [
    { id: 'orders', type: 'kafka', brokers: '127.0.0.1:9', topics: ['orders'] },
  ]);
  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: /^orders\b/ }).click();
  await expect(
    admin.getByText(
      'Deploying this moves the source to the new position, so records are skipped or read again.',
    ),
  ).toHaveCount(0);
  const asked = Date.now();
  await admin.getByRole('combobox', { name: /^start at/ }).selectOption('timestamp');
  await expect(
    admin.getByText(
      'Deploying this moves the source to the new position, so records are skipped or read again.',
    ),
  ).toBeVisible();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  const [source] = saved.config.sources;
  expect(source.start.type).toBe('timestamp');
  expect(source.start.millis).toBeGreaterThanOrEqual(asked);
  await expect(
    admin.getByText(
      'Deploying this moves the source to the new position, so records are skipped or read again.',
    ),
  ).toHaveCount(0);
});

test('A10: an S3 source that lists its bucket is added from its form and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'S3 listing', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new S3 listing source' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('landing');
  await dialog.getByRole('textbox', { name: /^bucket/ }).fill('landing-zone');
  await dialog.getByRole('textbox', { name: /^prefix/ }).fill('incoming/');
  await dialog.getByRole('combobox', { name: /^after commit/ }).selectOption('move');
  await dialog.getByRole('textbox', { name: /^move prefix/ }).fill('done/');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'landing',
      type: 's3List',
      bucket: 'landing-zone',
      prefix: 'incoming/',
      afterCommit: 'move',
      movePrefix: 'done/',
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^landing\b/ })).toContainText(
    'landing-zone/incoming/',
  );
});

// A10, sources and destinations from a form. Signing in as a service app is the form's default; a token is the
// other choice, and switching keeps nothing of the other way.
test('A10: an Okta source is added from its form as a service app and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'okta-key',
    custody: 'leader-stored',
    description: 'the Okta service app key',
    value: '-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n',
  });

  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'Okta System Log', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new Okta System Log source' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('okta-audit');
  await dialog.getByRole('textbox', { name: /^org/ }).fill('https://acme.okta.com');
  await dialog.getByRole('combobox', { name: /^sign in/ }).selectOption('token');
  await expect(dialog.getByRole('textbox', { name: /^client id/ })).toBeHidden();
  await dialog.getByRole('combobox', { name: /^sign in/ }).selectOption('serviceApp');
  await dialog.getByRole('textbox', { name: /^client id/ }).fill('0oa1b2c3d4e5f6g7h8i9');
  await dialog.getByRole('combobox', { name: 'private key source' }).selectOption('okta-key');
  await dialog.screenshot({ path: test.info().outputPath('okta-source-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'okta-audit',
      type: 'oktaLogs',
      org: 'https://acme.okta.com',
      clientId: '0oa1b2c3d4e5f6g7h8i9',
      privateKey: { secret: 'okta-key', version: 1 },
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^okta-audit\b/ })).toContainText('acme.okta.com');
});

// A10, sources and destinations from a form. Nothing answers at the database: the statement is the leader's to
// build, and Run comes back from a worker with why it could not read.
test('A10: a PostgreSQL source shows the statement it runs, and a Run that cannot connect says why', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build/sources');
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'PostgreSQL', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('auth-events');
  await dialog.getByRole('textbox', { name: /^host/ }).fill('127.0.0.1');
  await dialog.getByRole('spinbutton', { name: /^port/ }).fill('1');
  await dialog.getByRole('textbox', { name: /^database/ }).fill('auth');
  await dialog.getByRole('textbox', { name: /^user/ }).fill('reader');
  await dialog.getByRole('combobox', { name: /^TLS/ }).selectOption('disable');
  await dialog
    .getByRole('textbox', { name: /^query/ })
    .fill('SELECT event_id, occurred_at FROM auth.events');

  // Before Run it shows what Run runs: the query, unchanged, a few rows. What follows from Run is not shown yet,
  // and with no columns there is no record to add.
  const statement = dialog.locator('pre.statement');
  await expect(statement).toContainText('SELECT event_id, occurred_at FROM auth.events');
  await expect(statement).toContainText('LIMIT 20');
  await expect(dialog.getByRole('radiogroup')).toBeHidden();
  await expect(dialog.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();

  await dialog.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(dialog.getByRole('alert')).toContainText(/connect|refused/i, { timeout: 90_000 });
  await expect(dialog.getByText(/failed after/)).toBeVisible();
  await expect(dialog.getByRole('radiogroup')).toBeHidden();
  await expect(dialog.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();
});

// A10, sources and destinations from a form. MySQL and MariaDB have the PostgreSQL form, on their own port and with
// MySQL's placeholders.
test('A10: a MySQL source takes its own port, and a Run that cannot connect says why', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build/sources');
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'MySQL / MariaDB', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new MySQL / MariaDB source' })).toBeVisible();
  await expect(dialog.getByRole('spinbutton', { name: /^port/ })).toHaveValue('3306');
  await dialog.getByRole('textbox', { name: /^id/ }).fill('orders');
  await dialog.getByRole('textbox', { name: /^host/ }).fill('127.0.0.1');
  await dialog.getByRole('spinbutton', { name: /^port/ }).fill('1');
  await dialog.getByRole('textbox', { name: /^database/ }).fill('shop');
  await dialog.getByRole('textbox', { name: /^user/ }).fill('reader');
  await dialog.getByRole('combobox', { name: /^TLS/ }).selectOption('disable');
  await dialog.getByRole('textbox', { name: /^query/ }).fill('SELECT id, placed_at FROM orders');

  const statement = dialog.locator('pre.statement');
  await expect(statement).toContainText('SELECT id, placed_at FROM orders');
  await expect(statement).toContainText('LIMIT 20');
  await expect(dialog.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();
  await dialog.screenshot({ path: test.info().outputPath('mysql-source-form.png') });

  await dialog.getByRole('button', { name: 'Run', exact: true }).click();
  await expect(dialog.getByRole('alert')).toContainText(/connect|refused/i, { timeout: 90_000 });
  await expect(dialog.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();
});

// A10, sources and destinations from a form. Receivers share one form: a port and tokens, and under Advanced where
// they listen and how large a request they read. HTTP adds its path and records; a HEC token its acknowledgement.
test('A10: HTTP, HEC, OTLP, Prometheus and Elasticsearch receivers are added from their forms and saved as the leader accepts them', async ({
  admin,
  leader,
  api,
}) => {
  for (const name of ['otel-token', 'hec-token', 'hook-token', 'prom-token', 'es-token'])
    await api.post('/api/secrets', {
      name,
      custody: 'leader-stored',
      description: name,
      value: `${name}-value`,
    });

  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  const dialog = admin.getByRole('dialog');
  const shot = (name: string) => dialog.screenshot({ path: test.info().outputPath(`${name}.png`) });
  /** Opens the add dialog on `label`'s form, under the push group. */
  async function adding(label: string, id: string) {
    await admin.getByRole('button', { name: '+ Add source' }).click();
    await dialog.getByRole('button', { name: /^Push, all workers/ }).click();
    if (label === 'OTLP receiver') await shot('receiver-catalog');
    await dialog.getByRole('button', { name: label, exact: true }).click();
    await expect(dialog.getByRole('heading', { name: `new ${label} source` })).toBeVisible();
    await dialog.getByRole('textbox', { name: /^id/ }).fill(id);
  }
  async function token(name: string, secret: string) {
    await dialog.getByRole('button', { name: '+ token' }).click();
    await dialog.getByRole('textbox', { name: 'token name' }).last().fill(name);
    await dialog.getByRole('combobox', { name: 'token source' }).last().selectOption(secret);
  }
  async function add() {
    await dialog.getByRole('button', { name: 'Add', exact: true }).click();
    await expect(dialog).toBeHidden();
  }

  await adding('OTLP receiver', 'otel-logs');
  await expect(dialog.getByRole('spinbutton', { name: /^port/ })).toHaveValue('4318');
  await token('collectors', 'otel-token');
  await shot('otlp-receiver-form');
  await add();

  await adding('Prometheus receiver', 'prom');
  await expect(dialog.getByRole('spinbutton', { name: /^port/ })).toHaveValue('9090');
  await token('prometheus', 'prom-token');
  await shot('prometheus-receiver-form');
  await add();

  await adding('Elasticsearch receiver', 'beats');
  await expect(dialog.getByRole('spinbutton', { name: /^port/ })).toHaveValue('9200');
  await token('beats', 'es-token');
  await shot('elastic-receiver-form');
  await add();

  await adding('HEC receiver', 'hec');
  await expect(dialog.getByRole('spinbutton', { name: /^port/ })).toHaveValue('8088');
  await token('vector', 'hec-token');
  await dialog.getByRole('checkbox', { name: 'acknowledgement' }).check();
  await shot('hec-receiver-form');
  await add();

  await adding('HTTP receiver', 'ci-hooks');
  await dialog.getByRole('spinbutton', { name: /^port/ }).fill('8090');
  await dialog.getByRole('textbox', { name: /^path/ }).fill('/hooks/ci');
  await dialog.getByRole('combobox', { name: /^records/ }).selectOption('lines');
  await token('ci', 'hook-token');
  await shot('http-receiver-form');
  await add();

  await admin.screenshot({ path: test.info().outputPath('receiver-rows.png') });
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'otel-logs',
      type: 'otlpReceiver',
      port: 4318,
      tokens: { collectors: { secret: 'otel-token', version: 1 } },
    },
    {
      id: 'prom',
      type: 'prometheusReceiver',
      port: 9090,
      tokens: { prometheus: { secret: 'prom-token', version: 1 } },
    },
    {
      id: 'beats',
      type: 'elasticReceiver',
      port: 9200,
      tokens: { beats: { secret: 'es-token', version: 1 } },
    },
    {
      id: 'hec',
      type: 'hecReceiver',
      port: 8088,
      tokens: { vector: { token: { secret: 'hec-token', version: 1 }, acknowledgement: true } },
    },
    {
      id: 'ci-hooks',
      type: 'httpReceiver',
      port: 8090,
      path: '/hooks/ci',
      tokens: { ci: { secret: 'hook-token', version: 1 } },
      records: 'lines',
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^otel-logs\b/ })).toContainText(':4318/v1/logs');
  await expect(admin.getByRole('button', { name: /^prom\b/ })).toContainText(':9090/api/v1/write');
});

// A10, sources and destinations from a form. S2S and syslog receivers are not reached over HTTP: their rows say a
// port alone. A syslog sender carries no token, so its form edits the networks it admits. Over UDP it serves no TLS,
// so the form offers no identity.
test('A10: S2S and syslog receivers show as receivers, and a syslog receiver edits the networks it admits and its transport', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'forwarder-token',
    custody: 'leader-stored',
    description: 'forwarder-token',
    value: 'forwarder-token-value',
  });
  const s2s = {
    id: 'forwarders',
    type: 's2sReceiver',
    port: 9997,
    tokens: { uf: { secret: 'forwarder-token', version: 1 } },
    sourcetypes: { java_app: { lineBreaker: '([\\r\\n]+)\\d{4}-' } },
  };
  await api.put(`/api/sources${ON}`, [
    s2s,
    { id: 'firewalls', type: 'syslogReceiver', port: 6514, allowFrom: ['10.0.0.0/8'] },
  ]);

  await goto(admin, leader, '#/build/sources');
  const forwarders = admin.getByRole('button', { name: /^forwarders\b/ });
  await expect(forwarders).toContainText('S2S receiver');
  await expect(forwarders).toContainText(':9997');
  const firewalls = admin.getByRole('button', { name: /^firewalls\b/ });
  await expect(firewalls).toContainText('Syslog receiver');
  await expect(firewalls).toContainText(':6514');

  await forwarders.click();
  await expect(admin.getByRole('textbox', { name: 'sourcetype', exact: true })).toHaveValue(
    'java_app',
  );
  await expect(admin.getByRole('textbox', { name: 'line breaker' })).toHaveValue(
    '([\\r\\n]+)\\d{4}-',
  );
  await admin.getByRole('button', { name: 'About sourcetypes' }).click();
  await expect(admin.getByText("Where each sourcetype's records end.")).toBeVisible();
  await admin.screenshot({ path: test.info().outputPath('s2s-sourcetypes.png'), fullPage: true });
  await admin.keyboard.press('Escape');
  await forwarders.click();

  await firewalls.click();
  await expect(admin.getByRole('button', { name: 'rename 10.0.0.0/8' })).toBeVisible();
  await expect(admin.getByRole('combobox', { name: /^min level/ })).toBeHidden();
  const draft = admin.locator('.chip-draft');
  await draft.fill('192.168.0.0/16');
  await draft.press('Enter');
  await admin.getByRole('button', { name: /^Advanced/ }).click();
  const identity = admin.getByRole('combobox', { name: /^identity/ });
  await expect(identity).toBeVisible();
  const lineBreaker = admin.getByRole('textbox', { name: 'line breaker' });
  await lineBreaker.fill('(\\x00)');
  await admin.getByRole('button', { name: 'About line breaker' }).click();
  await expect(admin.getByText('Where a record ends.')).toBeVisible();
  await admin.screenshot({
    path: test.info().outputPath('syslog-line-breaker.png'),
    fullPage: true,
  });
  await admin.keyboard.press('Escape');
  // UDP frames nothing, so choosing it drops the line breaker with the identity.
  await admin.getByRole('combobox', { name: /^transport/ }).selectOption('udp');
  await expect(identity).toBeHidden();
  await expect(lineBreaker).toBeHidden();
  await expect(admin.getByRole('textbox', { name: /^request size/ })).toBeVisible();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    s2s,
    {
      id: 'firewalls',
      type: 'syslogReceiver',
      port: 6514,
      allowFrom: ['10.0.0.0/8', '192.168.0.0/16'],
      transport: 'udp',
    },
  ]);
  await admin.reload();
  await expect(admin.getByRole('button', { name: /^firewalls\b/ })).toContainText(':6514/udp');
});

// A10, sources and destinations from a form.
test('A10: an HTTP API source is added from its form, reading an array and asking since its last poll', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'events-token',
    custody: 'leader-stored',
    description: 'the events API bearer token',
    value: 'Bearer abc',
  });

  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'HTTP API', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new HTTP API source' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('events');
  await dialog.getByRole('textbox', { name: /^url/ }).fill('https://api.example.com/v1/events');
  await dialog.getByRole('button', { name: '+ credential' }).click();
  await dialog.getByRole('textbox', { name: 'header name' }).fill('Authorization');
  await dialog.getByRole('combobox', { name: /source$/ }).selectOption('events-token');
  await dialog.getByRole('combobox', { name: /^records(?! at)/ }).selectOption('array');
  await dialog.getByRole('textbox', { name: /^records at/ }).fill('/data');
  await dialog.getByRole('combobox', { name: /^pages/ }).selectOption('follow');
  await dialog.getByRole('textbox', { name: /^since parameter/ }).fill('since');
  await dialog.screenshot({ path: test.info().outputPath('http-api-source-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'events',
      type: 'httpApi',
      url: 'https://api.example.com/v1/events',
      credentials: { Authorization: { secret: 'events-token', version: 1 } },
      records: 'array',
      recordsAt: '/data',
      followLinks: true,
      sinceParam: 'since',
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^events\b/ })).toContainText(
    'api.example.com/v1/events',
  );
});

// A10, sources and destinations from a form. Signing in as an app shows only the fields its proof and its token's
// origin take, and switching keeps nothing the other way takes.
test('A10: an HTTP API source signs in as an app through its issuer and is saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'events-client',
    custody: 'leader-stored',
    description: 'the events API client secret',
    value: 's3cr3t',
  });

  await goto(admin, leader, '#/build/sources');
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'HTTP API', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('events');
  await dialog.getByRole('textbox', { name: /^url/ }).fill('https://api.example.com/v1/events');
  await expect(dialog.getByRole('textbox', { name: /^client id/ })).toBeHidden();
  await dialog.getByRole('combobox', { name: /^sign in/ }).selectOption('key');
  await expect(dialog.getByRole('combobox', { name: 'private key source' })).toBeVisible();
  await dialog.getByRole('combobox', { name: /^sign in/ }).selectOption('secret');
  await expect(dialog.getByRole('combobox', { name: 'private key source' })).toBeHidden();
  await dialog.getByRole('textbox', { name: /^client id/ }).fill('smeltr');
  await dialog
    .getByRole('combobox', { name: 'client secret source' })
    .selectOption('events-client');
  await expect(dialog.getByRole('textbox', { name: /^token URL/ })).toBeVisible();
  await dialog.getByRole('combobox', { name: /^token from/ }).selectOption('issuer');
  await expect(dialog.getByRole('textbox', { name: /^token URL/ })).toBeHidden();
  await dialog
    .getByRole('textbox', { name: /^issuer/ })
    .fill('https://login.example.com/realms/acme');
  await dialog.getByRole('button', { name: /^Advanced/ }).click();
  await dialog.getByRole('textbox', { name: /^scope/ }).fill('read:events');
  await dialog.screenshot({ path: test.info().outputPath('http-api-sign-in-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'events',
      type: 'httpApi',
      url: 'https://api.example.com/v1/events',
      clientId: 'smeltr',
      clientSecret: { secret: 'events-client', version: 1 },
      issuer: 'https://login.example.com/realms/acme',
      scope: 'read:events',
    },
  ]);
});

// A10, sources and destinations from a form. A POST's body is JSON: text that is not shows why and is not kept.
test('A10: an HTTP API source POSTs a JSON body and is saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/sources');
  await admin.getByRole('button', { name: '+ Add source' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'HTTP API', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('telemetry');
  await dialog
    .getByRole('textbox', { name: /^url/ })
    .fill('https://telemetry.example.com/api/v2/stream');
  await expect(dialog.getByRole('textbox', { name: /^body/ })).toBeHidden();
  await dialog.getByRole('combobox', { name: /^method/ }).selectOption('POST');
  const body = dialog.getByRole('textbox', { name: /^body/ });
  await body.fill('{"batchSize": 1000,');
  await expect(dialog.getByText(/This is not JSON/)).toBeVisible();
  await body.fill('{"batchSize": 1000, "maxLingerMs": 15000}');
  await expect(dialog.getByText(/This is not JSON/)).toBeHidden();
  await dialog.screenshot({ path: test.info().outputPath('http-api-post-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.sources).toEqual([
    {
      id: 'telemetry',
      type: 'httpApi',
      url: 'https://telemetry.example.com/api/v2/stream',
      method: 'POST',
      body: { batchSize: 1000, maxLingerMs: 15000 },
    },
  ]);
});

// A10, sources and destinations from a form.
test('A10: an SFTP destination is added from its form and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'drop-password',
    custody: 'leader-stored',
    description: 'the drop box user',
    value: 'drop-box-password',
  });
  const hostKey =
    'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAID3o82ivVxzYDbjz/HQTXSvCvoEb7fBpGzXPVbqK+q63';

  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'SFTP', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new SFTP destination' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('drop-box');
  await dialog.getByRole('textbox', { name: /^host(?! key)/ }).fill('sftp.example.com');
  await dialog.getByRole('textbox', { name: /^username/ }).fill('ingest');
  await expect(dialog.getByRole('combobox', { name: /^credential/ })).toHaveValue('password');
  await dialog.getByRole('combobox', { name: 'password source' }).selectOption('drop-password');
  await dialog.getByRole('textbox', { name: /^host key/ }).fill(hostKey);
  await dialog.getByRole('textbox', { name: /^directory/ }).fill('/upload');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'drop-box',
      type: 'sftp',
      host: 'sftp.example.com',
      username: 'ingest',
      password: { secret: 'drop-password', version: 1 },
      hostKey,
      directory: '/upload',
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^drop-box\b/ })).toContainText(
    'sftp.example.com',
  );
});

// A10, sources and destinations from a form. An Elasticsearch destination signs in with a username and password in
// place of an API key, and choosing it clears the other.
test('A10: an Elasticsearch destination signs in with a username and password', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'es-password',
    custody: 'leader-stored',
    description: 'the writer',
    value: 'changeme',
  });

  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('search');
  await dialog.getByRole('textbox', { name: 'url' }).fill('https://es.example.com:9200');
  await dialog.getByRole('textbox', { name: 'default index' }).fill('logs');
  const credential = dialog.getByRole('combobox', { name: /^credential/ });
  await expect(credential).toHaveValue('none');
  await credential.selectOption('apiKey');
  await expect(dialog.getByRole('combobox', { name: 'API key source' })).toBeVisible();
  await credential.selectOption('basic');
  await expect(dialog.getByRole('combobox', { name: 'API key source' })).toBeHidden();
  await dialog.getByRole('textbox', { name: /^username/ }).fill('smeltr_writer');
  await dialog.getByRole('combobox', { name: 'password source' }).selectOption('es-password');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'search',
      type: 'elasticsearch',
      url: 'https://es.example.com:9200',
      defaultIndex: 'logs',
      username: 'smeltr_writer',
      password: { secret: 'es-password', version: 1 },
    },
  ]);
});

// A10, sources and destinations from a form. A Delta table's root is checked as it is typed; its partition columns
// are chips.
test('A10: a Delta destination is added from its form and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Delta', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new Delta destination' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('lake-web');
  const location = dialog.getByRole('textbox', { name: /^location/ });
  await location.fill('lake/web');
  await expect(dialog.getByText("Give the table's root in S3, as s3://bucket/path.")).toBeVisible();
  await location.fill('s3://lake/tables/web');
  await expect(dialog.getByText("Give the table's root in S3, as s3://bucket/path.")).toBeHidden();
  const partitions = dialog.getByRole('group', { name: /^partition by/ }).getByRole('textbox');
  await partitions.fill('host');
  await partitions.press('Enter');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    { id: 'lake-web', type: 'delta', location: 's3://lake/tables/web', partitionBy: ['host'] },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^lake-web\b/ })).toContainText(
    's3://lake/tables/web',
  );
});

// A10, sources and destinations from a form. A Delta table named in Unity Catalog takes its root and storage
// credentials from the catalog, so the form asks for the catalog's sign-in and the bucket's region instead.
test('A10: a Unity Catalog Delta destination is added from its form and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'databricks-secret',
    custody: 'leader-stored',
    description: 'the smeltr-writer service principal secret',
    value: 'dose0123456789',
  });

  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Delta', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new Delta destination' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('lake-uc');
  await dialog.getByRole('textbox', { name: /^location/ }).fill('s3://lake/tables/web');
  await dialog.getByRole('combobox', { name: /^table named by/ }).selectOption('unityCatalog');
  await expect(dialog.getByRole('textbox', { name: /^location/ })).toBeHidden();
  await expect(dialog.getByRole('combobox', { name: /^credential/ })).toBeHidden();
  await expect(dialog.getByText("Name the bucket's region")).toBeVisible();

  const workspace = dialog.getByRole('textbox', { name: /^workspace/ });
  await workspace.fill('https://dbc-1234.cloud.databricks.com/browse?o=1234');
  await expect(
    dialog.getByText("Give the workspace's https:// URL and nothing after its host"),
  ).toBeVisible();
  await workspace.fill('https://dbc-1234.cloud.databricks.com');
  await expect(
    dialog.getByText("Give the workspace's https:// URL and nothing after its host"),
  ).toBeHidden();
  const table = dialog.getByRole('textbox', { name: /^table/ });
  await table.fill('access_log');
  await expect(dialog.getByText('as catalog.schema.table')).toBeVisible();
  await table.fill('main.web.access_log');
  await expect(dialog.getByText('as catalog.schema.table')).toBeHidden();
  await dialog.getByRole('textbox', { name: /^region/ }).fill('us-west-2');
  await expect(dialog.getByText("Name the bucket's region")).toBeHidden();
  await dialog.getByRole('textbox', { name: /^client id/ }).fill('smeltr-writer');
  await dialog
    .getByRole('combobox', { name: 'client secret source' })
    .selectOption('databricks-secret');
  await dialog.screenshot({ path: test.info().outputPath('delta-unity-catalog-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'lake-uc',
      type: 'delta',
      unityCatalog: {
        workspace: 'https://dbc-1234.cloud.databricks.com',
        table: 'main.web.access_log',
        clientId: 'smeltr-writer',
        clientSecret: { secret: 'databricks-secret', version: 1 },
      },
      region: 'us-west-2',
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^lake-uc\b/ })).toContainText(
    'main.web.access_log',
  );
});

// A10, sources and destinations from a form. An Event Hubs destination asks for its namespace, its default event hub
// and where its connection string lives, and offers only the codecs Event Hubs takes.
test('A10: an Event Hubs destination is added from its form and saved as the leader accepts it', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'event-hubs-send',
    custody: 'leader-stored',
    description: 'the acme-logs namespace, Send',
    value:
      'Endpoint=sb://acme-logs.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=k',
  });

  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Azure Event Hubs', exact: true }).click();
  await expect(
    dialog.getByRole('heading', { name: 'new Azure Event Hubs destination' }),
  ).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('alerts-hub');
  await dialog.getByRole('textbox', { name: /^namespace/ }).fill('acme-logs');
  await dialog.getByRole('textbox', { name: /^default event hub/ }).fill('alerts');
  await dialog
    .getByRole('combobox', { name: 'connection string source' })
    .selectOption('event-hubs-send');
  await dialog.getByText('Delivery', { exact: true }).click();
  const compression = dialog.getByRole('combobox', { name: /^compression/ });
  await expect(compression.locator('option')).toHaveText(['none', 'gzip']);
  await compression.selectOption('gzip');
  await dialog.screenshot({ path: test.info().outputPath('event-hubs-destination-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'alerts-hub',
      type: 'eventHubs',
      namespace: 'acme-logs',
      defaultEventHub: 'alerts',
      connectionString: { secret: 'event-hubs-send', version: 1 },
      compression: 'gzip',
    },
  ]);

  await admin.reload();
  const row = admin.getByRole('button', { name: /^alerts-hub\b/ });
  await expect(row).toContainText('acme-logs');
  await expect(row).toContainText('alerts');
});

// A10, sources and destinations from a form. An Event Hubs destination signs in with a connection string, a service
// principal, a user-assigned managed identity or the host's own, one at a time; the principal's auth endpoint waits
// under Delivery.
test('A10: an Event Hubs destination signs in as a service principal or the host', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'event-hubs-principal',
    custody: 'leader-stored',
    description: 'the smeltr-writer app',
    value: 'secret',
  });

  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Azure Event Hubs', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('alerts-hub');
  await dialog.getByRole('textbox', { name: /^namespace/ }).fill('acme-logs');
  await dialog.getByRole('textbox', { name: /^default event hub/ }).fill('alerts');
  const credential = dialog.getByRole('combobox', { name: /^credential/ });
  await expect(credential).toHaveValue('connectionString');
  await expect(credential.locator('option')).toHaveText([
    "the host's managed identity",
    'a user-assigned managed identity',
    'a connection string',
    'a service principal',
  ]);
  await dialog.screenshot({ path: test.info().outputPath('event-hubs-connection-string.png') });

  await credential.selectOption('servicePrincipal');
  await expect(dialog.getByRole('combobox', { name: 'connection string source' })).toBeHidden();
  await dialog.getByRole('textbox', { name: /^tenant id/ }).fill('acme.onmicrosoft.us');
  await dialog.getByRole('textbox', { name: /^client id/ }).fill('smeltr-writer');
  await dialog
    .getByRole('combobox', { name: 'client secret source' })
    .selectOption('event-hubs-principal');
  await dialog.getByText('Delivery', { exact: true }).click();
  await dialog
    .getByRole('textbox', { name: /^auth endpoint/ })
    .fill('https://login.microsoftonline.us');
  await dialog.screenshot({ path: test.info().outputPath('event-hubs-service-principal.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  let saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'alerts-hub',
      type: 'eventHubs',
      namespace: 'acme-logs',
      defaultEventHub: 'alerts',
      tenantId: 'acme.onmicrosoft.us',
      clientId: 'smeltr-writer',
      clientSecret: { secret: 'event-hubs-principal', version: 1 },
      authEndpoint: 'https://login.microsoftonline.us',
    },
  ]);

  // A user-assigned identity is named by its client id alone, so the principal and the endpoint that served it go.
  await admin.reload();
  await admin.getByRole('button', { name: /^alerts-hub\b/ }).click();
  await expect(admin.getByRole('combobox', { name: /^credential/ })).toHaveValue(
    'servicePrincipal',
  );
  await admin.getByRole('combobox', { name: /^credential/ }).selectOption('managedIdentity');
  await expect(admin.getByRole('textbox', { name: /^tenant id/ })).toBeHidden();
  await admin
    .getByRole('textbox', { name: /^client id/ })
    .fill('72f988bf-86f1-41af-91ab-2d7cd011db47');
  await admin.screenshot({ path: test.info().outputPath('event-hubs-user-assigned-identity.png') });
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'alerts-hub',
      type: 'eventHubs',
      namespace: 'acme-logs',
      defaultEventHub: 'alerts',
      managedIdentityClientId: '72f988bf-86f1-41af-91ab-2d7cd011db47',
    },
  ]);

  // The host's own identity names nothing.
  await admin.getByRole('combobox', { name: /^credential/ }).selectOption('host');
  await expect(admin.getByRole('textbox', { name: /^client id/ })).toBeHidden();
  await admin.screenshot({ path: test.info().outputPath('event-hubs-managed-identity.png') });
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    { id: 'alerts-hub', type: 'eventHubs', namespace: 'acme-logs', defaultEventHub: 'alerts' },
  ]);
});

// A10, sources and destinations from a form. A header whose value is a secret is refused among the committed ones,
// in its own row, and taken as a credential.
test('A10: an HTTP destination is added from its form, its secret header as a credential', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'intake-token',
    custody: 'leader-stored',
    description: 'the intake bearer token',
    value: 'Bearer abc',
  });

  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'HTTP', exact: true }).click();
  await expect(dialog.getByRole('heading', { name: 'new HTTP destination' })).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('intake');
  await dialog.getByRole('textbox', { name: /^url/ }).fill('https://intake.example.com/v1/logs');
  await expect(dialog.getByRole('combobox', { name: /^requests/ })).toHaveValue('batched');
  await dialog.getByRole('combobox', { name: /^format/ }).selectOption('jsonArray');

  await dialog.getByRole('button', { name: '+ header' }).click();
  await dialog.getByRole('textbox', { name: 'header name' }).fill('X-Source');
  await dialog.getByRole('textbox', { name: 'header value' }).fill('smeltr');

  await dialog.getByRole('button', { name: '+ credential' }).click();
  await dialog.getByRole('textbox', { name: 'header name' }).last().fill('Authorization');
  await dialog
    .getByRole('combobox', { name: /source$/ })
    .last()
    .selectOption('intake-token');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'intake',
      type: 'http',
      url: 'https://intake.example.com/v1/logs',
      body: 'jsonArray',
      headers: { 'X-Source': 'smeltr' },
      credentials: { Authorization: { secret: 'intake-token', version: 1 } },
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^intake\b/ })).toContainText(
    'intake.example.com',
  );
});

// A10, sources and destinations from a form. Text and bytes in a batch go as a JSON array of strings, bytes as base64.
test('A10: an HTTP destination batches text or bytes as a JSON array of strings', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'HTTP', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('archive');
  await dialog.getByRole('textbox', { name: /^url/ }).fill('https://archive.example.com/v1/raw');
  await dialog.getByRole('combobox', { name: /^format/ }).selectOption('textArray');
  await expect(dialog.getByRole('textbox', { name: /^text field/ })).toHaveAttribute(
    'placeholder',
    '_raw',
  );
  await dialog.getByRole('combobox', { name: /^format/ }).selectOption('bytesArray');
  await expect(dialog.getByRole('textbox', { name: /^bytes field/ })).toHaveAttribute(
    'placeholder',
    '__source.bytes',
  );
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'archive',
      type: 'http',
      url: 'https://archive.example.com/v1/raw',
      body: 'jsonArray',
      format: 'bytes',
    },
  ]);
});

// A10, sources and destinations from a form. Prometheus remote_write takes the HTTP form's URL, headers and
// credentials; remote_write fixes the body, so there is no format to pick.
test('A10: a Prometheus remote_write destination is added from its form, its Authorization as a credential', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'prom-write-token',
    custody: 'leader-stored',
    description: 'the remote_write bearer token',
    value: 'Bearer abc',
  });

  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Prometheus remote_write', exact: true }).click();
  await expect(
    dialog.getByRole('heading', { name: 'new Prometheus remote_write destination' }),
  ).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('prom-out');
  await dialog
    .getByRole('textbox', { name: /^url/ })
    .fill('https://prometheus.example.com/api/v1/write');
  await expect(dialog.getByRole('combobox', { name: /^format/ })).toHaveCount(0);
  await dialog.getByRole('combobox', { name: /^remote_write version/ }).selectOption('2.0');

  await dialog.getByRole('button', { name: '+ credential' }).click();
  await dialog.getByRole('textbox', { name: 'header name' }).last().fill('Authorization');
  await dialog
    .getByRole('combobox', { name: /source$/ })
    .last()
    .selectOption('prom-write-token');
  await dialog.getByRole('button', { name: /^Advanced/ }).click();
  // One request at a time keeps each series in order, so there is no concurrency to set.
  await expect(dialog.getByRole('spinbutton', { name: /^starting concurrency/ })).toHaveCount(0);
  await dialog.screenshot({ path: test.info().outputPath('prometheus-destination-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'prom-out',
      type: 'prometheus',
      url: 'https://prometheus.example.com/api/v1/write',
      version: '2.0',
      credentials: { Authorization: { secret: 'prom-write-token', version: 1 } },
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^prom-out\b/ })).toContainText(
    'prometheus.example.com',
  );
});

// A10, sources and destinations from a form. A Log Analytics workspace takes rows through a data collection rule,
// signed in with Entra as Event Hubs is, less the connection string.
test('A10: a Log Analytics workspace destination is added from its form, signed in as a service principal', async ({
  admin,
  leader,
  api,
}) => {
  await api.post('/api/secrets', {
    name: 'law-principal',
    custody: 'leader-stored',
    description: "the data collection rule's service principal",
    value: 'principal-secret',
  });

  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Log Analytics workspace', exact: true }).click();
  await expect(
    dialog.getByRole('heading', { name: 'new Log Analytics workspace destination' }),
  ).toBeVisible();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('law');
  await dialog
    .getByRole('textbox', { name: /^endpoint/ })
    .fill('https://acme-dce.eastus-1.ingest.monitor.azure.com');
  await dialog
    .getByRole('textbox', { name: /^rule id/ })
    .fill('dcr-0123456789abcdef0123456789abcdef');
  await dialog.getByRole('textbox', { name: /^stream/ }).fill('Custom-Events_CL');
  const credential = dialog.getByRole('combobox', { name: /^credential/ });
  await expect(credential).toHaveValue('host');
  await expect(credential.locator('option')).toHaveText([
    "the host's managed identity",
    'a user-assigned managed identity',
    'a service principal',
  ]);
  await credential.selectOption('servicePrincipal');
  await dialog.getByRole('textbox', { name: /^tenant id/ }).fill('acme.onmicrosoft.com');
  await dialog.getByRole('textbox', { name: /^client id/ }).fill('smeltr-writer');
  await dialog
    .getByRole('combobox', { name: 'client secret source' })
    .selectOption('law-principal');
  await dialog.getByRole('button', { name: /^Advanced/ }).click();
  await expect(dialog.getByRole('textbox', { name: /^auth endpoint/ })).toBeVisible();
  await dialog.screenshot({ path: test.info().outputPath('log-analytics-destination-form.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'law',
      type: 'logAnalytics',
      endpoint: 'https://acme-dce.eastus-1.ingest.monitor.azure.com',
      ruleId: 'dcr-0123456789abcdef0123456789abcdef',
      stream: 'Custom-Events_CL',
      tenantId: 'acme.onmicrosoft.com',
      clientId: 'smeltr-writer',
      clientSecret: { secret: 'law-principal', version: 1 },
    },
  ]);

  await admin.reload();
  await expect(admin.getByRole('button', { name: /^law\b/ })).toContainText(
    'acme-dce.eastus-1.ingest.monitor.azure.com',
  );
});

// A10, sources and destinations from a form.
test('A10: an Elasticsearch destination sends from each zone to its own address, and a zone row says what the save would refuse', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('logs');
  await dialog.getByRole('textbox', { name: /^url/ }).fill('https://es.internal:9200');
  await dialog.getByRole('textbox', { name: /^default index/ }).fill('logs');
  await dialog.getByRole('button', { name: /^Advanced/ }).click();

  const zones = dialog.getByRole('textbox', { name: 'zone', exact: true });
  const urls = dialog.getByRole('textbox', { name: 'zone URL', exact: true });
  await dialog.getByRole('button', { name: '+ zone' }).click();
  await zones.last().fill('us-east-1a');
  await expect(dialog.getByText('Give this zone a URL.')).toBeVisible();
  await urls.last().fill('https://es-a.internal:9200');
  await expect(dialog.getByText('Give this zone a URL.')).toBeHidden();
  await dialog.getByRole('button', { name: '+ zone' }).click();
  await urls.last().fill('https://es-b.internal:9200');
  await expect(dialog.getByText('Name the zone this URL is for.')).toBeVisible();
  await zones.last().fill('us-east-1a');
  await expect(dialog.getByText(/^This zone is listed twice/)).toBeVisible();
  await zones.last().fill('us-east-1b');
  await expect(dialog.getByText(/^This zone is listed twice/)).toBeHidden();
  await expect(dialog.getByRole('textbox', { name: /^zone attribute/ })).toHaveCount(0);
  await dialog.getByRole('switch', { name: 'sniff nodes' }).click();
  await dialog.getByRole('textbox', { name: /^zone attribute/ }).fill('rack');
  await dialog.getByRole('textbox', { name: /^zone attribute/ }).scrollIntoViewIfNeeded();
  await dialog.screenshot({ path: test.info().outputPath('elasticsearch-zone-urls.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toEqual([
    {
      id: 'logs',
      type: 'elasticsearch',
      url: 'https://es.internal:9200',
      zoneUrls: {
        'us-east-1a': 'https://es-a.internal:9200',
        'us-east-1b': 'https://es-b.internal:9200',
      },
      defaultIndex: 'logs',
      sniff: true,
      zoneAttribute: 'rack',
    },
  ]);
});

// A10, sources and destinations from a form.
test('A10: an S3 destination writes unmodified source bytes, which an S3 source reads back as escaped lines', async ({
  admin,
  leader,
  api,
}) => {
  await goto(admin, leader, '#/build/destinations');
  await expect(admin.getByRole('heading', { name: 'Destinations' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'S3', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('archive');
  await dialog.getByRole('textbox', { name: /^bucket/ }).fill('archive-zone');
  await dialog.getByRole('combobox', { name: /^format/ }).selectOption('bytes');
  await expect(dialog.getByRole('textbox', { name: /^bytes field/ })).toHaveAttribute(
    'placeholder',
    '__source.bytes',
  );
  await dialog.getByRole('combobox', { name: /^newlines/ }).selectOption('refuse');
  await dialog.screenshot({ path: test.info().outputPath('destination-source-bytes.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  // The bar goes once the save has landed; the button alone hides as soon as it reads "Saving…".
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  await goto(admin, leader, '#/build/sources');
  await expect(admin.getByRole('heading', { name: 'Sources' })).toBeVisible();
  await admin.getByRole('button', { name: '+ Add source' }).click();
  await dialog.getByRole('button', { name: /^Pull, one worker/ }).click();
  await dialog.getByRole('button', { name: 'S3 listing', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('replay');
  await dialog.getByRole('textbox', { name: /^bucket/ }).fill('archive-zone');
  await dialog.getByRole('combobox', { name: /^framing/ }).selectOption('escapedLines');
  await dialog.screenshot({ path: test.info().outputPath('source-escaped-lines.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations).toMatchObject([
    { id: 'archive', type: 's3', format: 'bytes', newlines: 'refuse' },
  ]);
  expect(saved.config.sources).toMatchObject([
    { id: 'replay', type: 's3List', framing: 'escapedLines' },
  ]);
});

// A10, sources and destinations from a form. Nothing answers at the source's endpoint: holding a listing source
// is the leader's choice of worker, whether or not its listing succeeds.
test('A10: the Sources page names the worker that holds a listing source', async ({
  admin,
  leader,
  api,
  fleet,
}) => {
  const branch = 'listing';
  await api.put(`/api/sources?branch=${branch}&scope=shared`, [
    {
      id: 'landing',
      type: 's3List',
      bucket: 'landing-zone',
      region: 'us-east-1',
      endpoint: 'http://127.0.0.1:1',
      afterCommit: 'delete',
    },
  ]);
  await api.openChange(branch, 'Read the landing bucket');
  await api.deploy(branch);
  const w1 = await fleet.startWorker(leader, api, 'default', 'w1');

  await goto(admin, leader, '#/build/sources');
  const row = admin.getByRole('button', { name: /^landing\b/ });
  await expect(row).toContainText('held by w1', { timeout: 60_000 });
  await expect(row).not.toContainText('last heard');

  // A holder gone quiet keeps the source, and the row says how long since it was heard.
  w1.child.kill('SIGSTOP');
  await expect(row).toContainText(/held by w1, last heard \d+s ago/, { timeout: 60_000 });
});
