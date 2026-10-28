// Schemas: a bundle's schemas opened and searched by field, one added from a JSON Schema, a pipeline's Produces and
// a destination's accepts, and what review finds where rows do not fit.
import { test, expect, goto, type Api } from '../fleet';

const ON = '?branch=work&scope=shared';

/** A sign-in, as a bundle declares it: an id, a time in epoch milliseconds and a user are required. */
const LOGIN = {
  id: 'login',
  title: 'Login',
  fields: [
    { field: 'activity_id', type: 'i64', required: true },
    { field: 'time', type: 'i64', required: true, description: 'Milliseconds since the epoch.' },
    { field: 'user', type: '{...}', required: true },
    { field: 'user.name', type: 'str' },
    { field: 'user.manager', type: 'variant' },
    { field: 'src_endpoint', type: '{...}' },
    { field: 'src_endpoint.ip', type: 'str' },
    { field: 'src_endpoint.port', type: 'i64' },
  ],
};

const AUDIT = { id: 'audit', title: 'Audit', fields: [{ field: 'action', type: 'str' }] };

/** `shared/` declaring `login` and `audit`. */
async function declare(api: Api) {
  await api.put(`/api/schemas${ON}`, [LOGIN, AUDIT]);
}

test('a schema opens to its fields, and the search narrows them', async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await expect(admin.getByRole('heading', { name: 'Schemas' })).toBeVisible();
  await admin.getByRole('button', { name: /Login$/ }).click();
  await expect(admin.getByText('Edit it with Edit JSON.')).toBeVisible();
  await expect(admin.getByRole('cell', { name: /^time\*$/ })).toBeVisible();
  await expect(
    admin.getByRole('cell', { name: 'Milliseconds since the epoch.', exact: true }),
  ).toBeVisible();
  await expect(admin.getByRole('cell', { name: 'user.manager', exact: true })).toBeVisible();
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('src_endpoint');
  await expect(admin.getByRole('cell', { name: 'user.manager', exact: true })).toBeHidden();
  await expect(admin.getByRole('cell', { name: 'src_endpoint.port', exact: true })).toBeVisible();
  await expect(admin.getByText('3 fields of 8 hold “src_endpoint”.')).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-fields.png' });
});

test('a search finds a schema by a field it holds', async ({ admin, leader, api }) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('src_endpoint.port');
  await expect(admin.getByRole('cell').filter({ hasText: 'src_endpoint.port' })).toBeVisible();
  await expect(admin.getByRole('button', { name: /Audit$/ })).toBeHidden();
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('no-field-is-called-this');
  await expect(admin.getByText(/No schema or field matches/)).toBeVisible();
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
  expect(saved.config.schemas).toEqual([
    {
      id: 'orders',
      title: 'Orders',
      fields: [
        { field: 'id', type: 'i64', required: true },
        { field: 'total', type: 'f64' },
      ],
    },
  ]);
});

test("a pipeline's Produces is saved with it, and the editor says what its rows lack", async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await api.put(`/api/pipelines/logins${ON}`, { id: 'logins', name: 'logins', steps: [] });
  await goto(admin, leader, '#/build/pipelines/logins');
  const produces = admin.locator('.produces');
  await produces.getByRole('combobox', { name: 'Add a schema it produces' }).selectOption('login');
  await expect(produces.getByRole('link', { name: 'login' })).toBeVisible();
  await expect(produces.getByRole('status')).toHaveText('1 schema finding', { timeout: 15_000 });
  await expect(
    admin.getByText('Login requires `activity_id`, `time`, `user`; the rows lack them.'),
  ).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-produces.png' });

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`/api/pipelines/logins${ON}`);
  expect(saved.pipeline.produces).toEqual(['login']);
});

test("a destination's accepts is saved with it", async ({ admin, leader, api }) => {
  await declare(api);
  await goto(admin, leader, '#/build/destinations');
  await admin.getByRole('button', { name: '+ Add destination' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await dialog.getByRole('textbox', { name: /^id/ }).fill('search');
  await dialog.getByRole('textbox', { name: 'url' }).fill('https://es.example.com:9200');
  await dialog.getByRole('textbox', { name: 'default index' }).fill('logs');
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await admin.getByRole('combobox', { name: 'Add a schema it accepts' }).selectOption('audit');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });

  const saved = await api.get(`/api/config${ON}`);
  expect(saved.config.destinations[0].accepts).toEqual(['audit']);
});

/** On `branch`, in group `default`: a pipeline producing `login` that writes nothing, routed from a metrics source
 * to a destination that accepts only `audit`. */
async function arrangeMisfit(api: Api, branch: string) {
  const on = `?branch=${branch}&scope=default`;
  await api.put(`/api/schemas${on}`, [LOGIN, AUDIT]);
  await api.put(`/api/sources${on}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/logins${on}`, {
    id: 'logins',
    name: 'logins',
    steps: [],
    produces: ['login'],
  });
  await api.put(`/api/destinations${on}`, [
    {
      id: 'search',
      type: 'elasticsearch',
      url: 'http://127.0.0.1:9',
      defaultIndex: 'logs',
      accepts: ['audit'],
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
  await api.openChange('logins', 'Logins as Login');
  await goto(admin, leader, '#/changes/review/logins');

  await expect(admin.getByText('2 schema findings')).toBeVisible({ timeout: 15_000 });
  await admin.getByRole('tab', { name: /^Schemas/ }).click();
  const panel = admin.getByRole('tabpanel');
  await expect(panel.getByRole('link', { name: 'logins' })).toBeVisible();
  await expect(
    panel.getByText('Login requires `activity_id`, `time`, `user`; the rows lack them.'),
  ).toBeVisible();
  await expect(panel.getByText(/destination "search" accepts Audit/)).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeEnabled();
  await admin.screenshot({ path: 'results/schemas-review.png' });
});

test('a field the schema requires and the rows lack is a column of its own, with its type', async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await api.put(`/api/pipelines/logins${ON}`, {
    id: 'logins',
    name: 'logins',
    produces: ['login'],
    steps: [
      {
        type: 'parser',
        id: 'login',
        input: '_raw',
        kind: { type: 'json' },
        declared: [{ name: 'when', type: 'datetime' }],
      },
      {
        type: 'code',
        id: 'map',
        disabled: false,
        body: 'time = when.epochMillis();\ndelete(when, _raw);',
      },
    ],
  });
  await api.put(`/api/samples${ON}`, {
    id: 'logins-sample',
    name: 'logins',
    records: [{ _raw: '{"when":"2026-10-27T10:00:00Z"}' }],
  });
  await goto(admin, leader, '#/build/pipelines/logins/for/logins-sample');

  const grid = admin.locator('table.grid');
  const head = grid.locator('thead th', { hasText: 'activity_id' });
  await expect(head).toContainText('activity_id: i64', { timeout: 15_000 });
  await expect(head).toHaveAttribute('title', /^Login requires activity_id/);
  await expect(grid.locator('thead th.missing-head', { hasText: /^time/ })).toHaveCount(0);
  await expect(grid.getByRole('row', { name: /^row 0 / })).toContainText('missing');

  // A sample of one record opens on it.
  await expect(admin.getByRole('button', { name: 'row 0' })).toHaveAttribute(
    'aria-expanded',
    'true',
  );
  await expect(admin.getByText('Required by Login:')).toBeVisible();
  await expect(
    admin.getByText('Login requires `activity_id`, `user`; the rows lack them.'),
  ).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-missing.png' });
});
