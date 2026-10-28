// The schema catalog: the built-ins browsed by OCSF category and searched by field, a bundle's own schema added from
// a JSON Schema, a pipeline's Produces and a destination's accepts, and what review finds where rows do not fit.
import { test, expect, goto, type Api } from '../fleet';

const ON = '?branch=work&scope=shared';

test('OCSF is browsed by category a page at a time, and a class opens an object at a time', async ({
  admin,
  leader,
}) => {
  await goto(admin, leader, '#/build/schemas');
  await expect(admin.getByRole('heading', { name: 'Schemas' })).toBeVisible();

  const discovery = admin.getByRole('button', { name: /Discovery$/ });
  await expect(discovery).toHaveAttribute('aria-expanded', 'false');
  await discovery.click();
  await expect(discovery).toHaveAttribute('aria-expanded', 'true');
  await expect(admin.getByText('1–10 of 23 in Discovery')).toBeVisible();
  await admin.getByRole('button', { name: '3', exact: true }).click();
  await expect(admin.getByText('21–23 of 23 in Discovery')).toBeVisible();
  await admin.getByRole('button', { name: 'Collapse all' }).click();
  await expect(discovery).toHaveAttribute('aria-expanded', 'false');

  await admin.getByRole('button', { name: /Identity & Access Management$/ }).click();
  await admin.getByRole('button', { name: /ocsf\/authentication$/ }).click();
  await expect(admin.getByText('Built in, read-only.')).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Close user' })).toHaveCount(0);
  await admin.getByRole('button', { name: 'Open user', exact: true }).click();
  await expect(admin.getByRole('button', { name: 'Close user', exact: true })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Open user.account', exact: true })).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-ocsf.png' });
});

test('a search finds a schema by a field it holds', async ({ admin, leader }) => {
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('src_endpoint.port');
  const auth = admin.getByRole('row').filter({ hasText: 'ocsf/authentication' });
  await expect(auth).toContainText('src_endpoint.port');
  await expect(admin.getByRole('button', { name: 'Expand all' })).toHaveCount(0);
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('no-field-is-called-this');
  await expect(admin.getByText(/No schema has a field or a name holding/)).toBeVisible();
});

test("a JSON Schema added on the Schemas page is saved as the bundle's own", async ({
  admin,
  leader,
  api,
}) => {
  const schema = {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    title: 'Orders',
    type: 'object',
    required: ['id'],
    properties: { id: { type: 'integer' }, total: { type: 'number' } },
  };
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: '+ Add schema' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'JSON Schema', exact: true }).click();
  await dialog.getByLabel('id', { exact: true }).fill('orders');
  await dialog.getByRole('textbox', { name: 'JSON Schema' }).fill(JSON.stringify(schema));
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('cell', { name: /^id\*$/ })).toBeVisible();

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.schemas).toEqual([{ id: 'orders', schema }]);
});

test("a pipeline's Produces is saved with it, and the editor says what its rows lack", async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`/api/pipelines/logins${ON}`, { id: 'logins', name: 'logins', steps: [] });
  await goto(admin, leader, '#/build/pipelines/logins');
  const produces = admin.locator('.produces');
  await produces
    .getByRole('combobox', { name: 'Add a schema it produces' })
    .selectOption('ocsf/authentication');
  await expect(produces.getByRole('link', { name: 'ocsf/authentication' })).toBeVisible();
  await expect(produces.getByRole('status')).toHaveText('1 schema finding', { timeout: 15_000 });
  await expect(
    admin.getByText(/^OCSF Authentication requires \d+ fields no step writes/),
  ).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-produces.png' });

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`/api/pipelines/logins${ON}`);
  expect(saved.pipeline.schemas).toEqual(['ocsf/authentication']);
});

test("a destination's accepts is saved with it", async ({ admin, leader, api }) => {
  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('search');
  await dialog.getByRole('textbox', { name: 'url' }).fill('https://es.example.com:9200');
  await dialog.getByRole('textbox', { name: 'default index' }).fill('logs');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('combobox', { name: 'Add a schema it accepts' }).selectOption('ecs');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations[0].accepts).toEqual(['ecs']);
});

/** On `branch`, in group `default`: a pipeline producing OCSF Authentication that writes nothing, routed from a
 * metrics source to a destination that accepts only ECS. */
async function arrangeMisfit(api: Api, branch: string) {
  const on = `?branch=${branch}&scope=default`;
  await api.put(`/api/sources${on}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/logins${on}`, {
    id: 'logins',
    name: 'logins',
    steps: [],
    schemas: ['ocsf/authentication'],
  });
  await api.put(`/api/destinations${on}`, [
    {
      id: 'search',
      type: 'elasticsearch',
      url: 'http://127.0.0.1:9',
      defaultIndex: 'logs',
      accepts: ['ecs'],
    },
  ]);
  await api.put(`/api/routes${on}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'logins', deliver: [{ to: ['search'] }] },
  ]);
}

test('review lists where the rows do not fit a schema, and the dock counts them without blocking', async ({
  admin,
  leader,
  api,
}) => {
  await arrangeMisfit(api, 'logins');
  await api.openChange('logins', 'Logins as OCSF');
  await goto(admin, leader, '#/changes/review/logins');

  await expect(admin.getByText('2 schema findings')).toBeVisible({ timeout: 15_000 });
  await admin.getByRole('tab', { name: /^Schemas/ }).click();
  const panel = admin.getByRole('tabpanel');
  await expect(panel.getByRole('link', { name: 'logins' })).toBeVisible();
  await expect(
    panel.getByText(/^OCSF Authentication requires \d+ fields no step writes/),
  ).toBeVisible();
  await expect(panel.getByText(/destination "search" accepts ECS/)).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeEnabled();
  await admin.screenshot({ path: 'results/schemas-review.png' });
});
