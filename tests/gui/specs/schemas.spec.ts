// Schemas: a bundle's schemas opened and searched by field, added from a standard's file (ECS, OCSF, a CSV of fields,
// JSON Schema) or by an importer pipeline, a pipeline's Produces and a destination's accepts, and what review finds
// where rows do not fit.
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Page } from '@playwright/test';
import { test, expect, goto, type Api, type Leader } from '../fleet';

const ON = '?branch=work';
const SHARED = '/api/areas/shared';

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
  await api.put(`${SHARED}/schemas${ON}`, [LOGIN, AUDIT]);
}

test('a schema opens to its fields, and the search narrows them', async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await expect(admin.getByRole('heading', { name: 'Schemas' })).toBeVisible();
  await admin.getByRole('button', { name: 'login fields' }).click();
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

test('a schema exports as JSON Schema and as an Elasticsearch mapping', async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: 'login fields' }).click();
  const exported = async (button: string) => {
    const download = admin.waitForEvent('download');
    await admin.getByRole('button', { name: button }).click();
    const file = await download;
    return {
      name: file.suggestedFilename(),
      body: JSON.parse(await readFile(await file.path(), 'utf8')),
    };
  };
  const schema = await exported('Export JSON Schema');
  expect(schema.name).toBe('login.schema.json');
  expect(schema.body.required).toEqual(['activity_id', 'time', 'user']);
  const mapping = await exported('Export Elasticsearch mapping');
  expect(mapping.name).toBe('login.mapping.json');
  expect(mapping.body.mappings.properties.src_endpoint).toEqual({
    properties: {
      ip: { type: 'keyword', ignore_above: 1024 },
      port: { type: 'long' },
    },
  });
});

test('a search finds a schema by a field it holds', async ({ admin, leader, api }) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('src_endpoint.port');
  await expect(admin.getByRole('cell').filter({ hasText: 'src_endpoint.port' })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'audit fields' })).toBeHidden();
  await admin.getByRole('searchbox', { name: 'Search schemas' }).fill('no-field-is-called-this');
  await expect(admin.getByText(/No schema or field matches/)).toBeVisible();
});

test('the Elasticsearch view chooses the type a field maps to, saved with the schema', async ({
  admin,
  leader,
  api,
}) => {
  await declare(api);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: 'login fields' }).click();
  await admin.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await admin.getByRole('button', { name: 'About Elasticsearch' }).click();
  await expect(admin.getByText(/Each field is indexed as this Elasticsearch type/)).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-elasticsearch-help.png' });
  await admin.keyboard.press('Escape');
  const ip = admin.getByRole('combobox', { name: 'Elasticsearch type of src_endpoint.ip' });
  await expect(ip).toHaveValue('keyword');
  await expect(ip.getByRole('option')).toHaveCount(8);
  await expect(
    admin
      .getByRole('combobox', { name: 'Elasticsearch type of user.manager' })
      .getByRole('option', {
        name: 'dynamic',
        exact: true,
      }),
  ).toBeAttached();
  await ip.selectOption('ip');
  await admin.screenshot({ path: 'results/schemas-elasticsearch.png' });

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`${SHARED}${ON}`);
  /** `src_endpoint.ip` of `login`, as the leader keeps it. */
  type Kept = { schemas: { id: string; fields: { field: string; elasticsearch?: object }[] }[] };
  const kept = (config: Kept) =>
    config.schemas.find((s) => s.id === 'login')!.fields.find((f) => f.field === 'src_endpoint.ip');
  expect(kept(saved.config)?.elasticsearch).toEqual({ type: 'ip' });

  await admin
    .getByRole('combobox', { name: 'Elasticsearch type of src_endpoint.ip' })
    .selectOption('keyword');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const cleared = await api.get(`${SHARED}${ON}`);
  expect(kept(cleared.config)).toEqual({ field: 'src_endpoint.ip', type: 'str' });
});

test("a field's own Elasticsearch settings show beside its type, and shared/'s schemas only show it", async ({
  admin,
  leader,
  api,
}) => {
  const name = {
    field: 'user.name',
    type: 'str',
    elasticsearch: { type: 'keyword', ignore_above: 1024 },
  };
  await api.put(`${SHARED}/schemas${ON}`, [
    {
      ...LOGIN,
      fields: [
        ...LOGIN.fields.map((f) => (f.field === 'user.name' ? name : f)),
        { field: 'tags', type: 'array<variant>' },
      ],
    },
  ]);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: 'login fields' }).click();
  await admin.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await expect(
    admin.getByRole('combobox', { name: 'Elasticsearch type of user.name' }),
  ).toHaveValue('keyword');
  await expect(admin.getByText('+ 1 more setting: ignore_above')).toBeVisible();
  // An array's elements map as Elasticsearch finds them, as a variant does.
  await expect(
    admin
      .getByRole('combobox', { name: 'Elasticsearch type of tags' })
      .getByRole('option', { name: 'dynamic', exact: true }),
  ).toBeAttached();
  await admin.screenshot({ path: 'results/schemas-elasticsearch-settings.png' });

  await admin.getByRole('button', { name: 'shared', exact: true }).click();
  await admin.getByRole('button', { name: /^default/ }).click();
  await admin.getByRole('button', { name: '▸ login' }).click();
  await admin.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await expect(admin.getByRole('combobox')).toHaveCount(0);
  await expect(
    admin.getByRole('cell', { name: 'keyword + 1 more setting: ignore_above', exact: true }),
  ).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-elasticsearch-shared.png' });
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
  await expect(dialog.getByRole('heading', { name: /new schema from JSON Schema$/ })).toBeVisible();
  await dialog.getByLabel('id', { exact: true }).fill('orders');
  await dialog.getByRole('textbox', { name: 'JSON Schema' }).fill(JSON.stringify(schema));
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('cell', { name: /^id\*$/ })).toBeVisible();

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`${SHARED}${ON}`);
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
  await api.put(`${SHARED}/pipelines/logins${ON}`, { id: 'logins', name: 'logins', steps: [] });
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
  const saved = await api.get(`${SHARED}/pipelines/logins${ON}`);
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

  const saved = await api.get(`${SHARED}${ON}`);
  expect(saved.config.destinations[0].accepts).toEqual(['audit']);
});

/** The ECS importer: one row per field of ECS's `generated/csv/fields.csv` (Elastic Common Schema, Apache 2.0). The
 * header row is dropped, and so is each `.text` multi-field: the field above it indexed as text, not a key of its own.
 * Each other row's column 4 is the field, 5 its ECS type, 7 whether it is an array, 9 its
 * description. Its quoted cells hold line ends, so a row is cut where the next starts, with its version.
 * `tests/gui/fixtures/ecs-fields-excerpt.csv` is that file's first rows, one whose example spans lines, and a field
 * with its multi-field. */
const ECS_LINE_BREAKER = String.raw`([\r\n]+)\d+\.\d+\.\d+`;
const ECS_IMPORTER = {
  id: 'ecs-importer',
  name: 'ECS importer',
  steps: [
    {
      type: 'drop',
      id: 'not-fields',
      condition: '_raw.startsWith("ECS_Version,") || _raw.includes(".text,match_only_text,")',
    },
    {
      type: 'code',
      id: 'row',
      body: [
        'Object.assign($, csv(_raw, [null, null, null, "field", "ecs", null, "normalization", null, "description"]))',
        'type = ecs == "date" ? "datetime"',
        '  : ecs == "long" ? "i64"',
        '  : ecs == "float" || ecs == "scaled_float" || ecs == "double" ? "f64"',
        '  : ecs == "boolean" ? "bool"',
        '  : ecs == "object" || ecs == "geo_point" ? "{...}"',
        '  : ecs == "nested" ? "array<{...}>"',
        '  : ecs == "flattened" ? "variant"',
        '  : "str";',
        'if (normalization == "array") { type = "array<" + type + ">"; }',
        'required = field == "@timestamp";',
        'if (ecs == "wildcard" || ecs == "text" || ecs == "match_only_text" || ecs == "ip" || ecs == "constant_keyword"',
        '    || ecs == "float" || ecs == "geo_point" || ecs == "nested" || ecs == "flattened") {',
        '  elasticsearch = {type: ecs};',
        '}',
        'delete(_raw, ecs, normalization);',
      ].join('\n'),
    },
  ],
};

test('a schema is imported by a pipeline of the bundle run over an uploaded file', async ({
  admin,
  leader,
  api,
}) => {
  await api.put(`${SHARED}/pipelines/ecs-importer${ON}`, ECS_IMPORTER);
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: '+ Add schema' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Import', exact: true }).click();
  await dialog.getByLabel('id', { exact: true }).fill('ecs');
  await dialog.getByRole('combobox', { name: 'Importer' }).selectOption('ecs-importer');
  await dialog
    .getByLabel(/^File/)
    .setInputFiles(fileURLToPath(new URL('../fixtures/ecs-fields-excerpt.csv', import.meta.url)));
  await dialog.getByRole('textbox', { name: 'line breaker' }).fill(ECS_LINE_BREAKER);
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('cell', { name: '@timestamp*', exact: true })).toBeVisible();
  await expect(
    admin.getByRole('cell', {
      name: 'Unique number allocated to the autonomous system.',
      exact: true,
    }),
  ).toBeVisible();
  await admin.screenshot({ path: 'results/schemas-imported.png' });

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const saved = await api.get(`${SHARED}${ON}`);
  const fields = saved.config.schemas[0].fields;
  expect(saved.config.schemas[0].id).toBe('ecs');
  expect(fields).toHaveLength(14);
  expect(fields[0]).toEqual({
    field: '@timestamp',
    type: 'datetime',
    required: true,
    description: 'Date/time when the event originated.',
  });
  expect(fields.find((f: { field: string }) => f.field === 'tags').type).toBe('array<str>');
  expect(fields.find((f: { field: string }) => f.field === 'labels').type).toBe('{...}');
  expect(fields.find((f: { field: string }) => f.field === 'client.as.number').type).toBe('i64');
  expect(fields.map((f: { field: string }) => f.field)).not.toContain(
    'client.as.organization.name.text',
  );
  expect(fields.find((f: { field: string }) => f.field === 'gen_ai.input.messages').type).toBe(
    'variant',
  );
  expect(fields.find((f: { field: string }) => f.field === 'message').elasticsearch).toEqual({
    type: 'match_only_text',
  });
  expect(
    fields.find((f: { field: string }) => f.field === 'gen_ai.input.messages').elasticsearch,
  ).toEqual({
    type: 'flattened',
  });
  expect(
    fields.find((f: { field: string }) => f.field === 'agent.id').elasticsearch,
  ).toBeUndefined();
});

/** On `branch`, in group `default`: a pipeline producing `login` that writes nothing, routed from a metrics source
 * to a destination that accepts only `audit`. */
async function arrangeMisfit(api: Api, branch: string) {
  const on = `?branch=${branch}`;
  const DEFAULT = '/api/areas/default';
  await api.put(`${DEFAULT}/schemas${on}`, [LOGIN, AUDIT]);
  await api.put(`${DEFAULT}/sources${on}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`${DEFAULT}/pipelines/logins${on}`, {
    id: 'logins',
    name: 'logins',
    steps: [],
    produces: ['login'],
  });
  await api.put(`${DEFAULT}/destinations${on}`, [
    {
      id: 'search',
      type: 'elasticsearch',
      url: 'http://127.0.0.1:9',
      defaultIndex: 'logs',
      accepts: ['audit'],
    },
  ]);
  await api.put(`${DEFAULT}/routes${on}`, [
    { id: 'r1', source: 'm1', condition: null, pipeline: 'logins', deliver: [{ to: ['search'] }] },
  ]);
}

/** A fixture's path, for an upload. */
const fixture = (path: string) => fileURLToPath(new URL(path, import.meta.url));

/** Opens Add schema at `tile`, names the schema `id` and uploads `file`. Answers the dialog. */
async function addFrom(admin: Page, leader: Leader, tile: string, id: string, file: string) {
  await goto(admin, leader, '#/build/schemas');
  await admin.getByRole('button', { name: '+ Add schema' }).click();
  const dialog = admin.getByRole('dialog');
  await dialog.getByRole('button', { name: tile, exact: true }).click();
  await dialog.getByLabel('id', { exact: true }).fill(id);
  await dialog.getByLabel(/^File/).setInputFiles(file);
  return dialog;
}

/** Saves the page, and answers `id`'s fields as the leader keeps them. */
async function saved(admin: Page, api: Api, id: string) {
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('Unsaved', { exact: true })).toBeHidden({ timeout: 15_000 });
  const config = (await api.get(`${SHARED}${ON}`)).config;
  type Kept = {
    field: string;
    type: string;
    required?: boolean;
    example?: string;
    elasticsearch?: object;
  };
  const fields: Kept[] = config.schemas.find((s: { id: string }) => s.id === id).fields;
  return (name: string) => fields.find((f) => f.field === name);
}

test('ECS comes in from ecs_flat.yml with its types, settings and examples', async ({
  admin,
  leader,
  api,
}) => {
  const dialog = await addFrom(
    admin,
    leader,
    'ECS',
    'ecs',
    fixture('../../fixtures/ecs-flat-excerpt.yml'),
  );
  await expect(dialog.getByRole('heading', { name: /new schema from ECS$/ })).toBeVisible();
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('cell', { name: '@timestamp*', exact: true })).toBeVisible();
  await expect(admin.getByRole('cell', { name: 'Albert Einstein', exact: true })).toBeVisible();
  const field = await saved(admin, api, 'ecs');
  expect(field('ecs.version')?.required).toBe(true);
  expect(field('agent.id')).toMatchObject({ type: 'str', example: '8a4f500d' });
  expect(field('agent.id')?.elasticsearch).toBeUndefined();
  expect(field('user.full_name')?.elasticsearch).toEqual({
    type: 'keyword',
    fields: { text: { type: 'match_only_text' } },
  });
  expect(field('tags')?.type).toBe('array<str>');
});

test('ECS comes in from fields.csv, with no line breaker to set', async ({
  admin,
  leader,
  api,
}) => {
  const dialog = await addFrom(
    admin,
    leader,
    'ECS',
    'ecs',
    fixture('../fixtures/ecs-fields-excerpt.csv'),
  );
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  const field = await saved(admin, api, 'ecs');
  expect(field('@timestamp')).toMatchObject({ type: 'datetime', required: true });
  expect(field('message')?.elasticsearch).toEqual({ type: 'match_only_text' });
  expect(field('gen_ai.input.messages')).toMatchObject({
    type: 'variant',
    elasticsearch: { type: 'flattened' },
  });
});

test('an OCSF class comes in as deep and with the requirements chosen', async ({
  admin,
  leader,
  api,
}) => {
  const dialog = await addFrom(
    admin,
    leader,
    'OCSF',
    'auth',
    fixture('../../fixtures/ocsf-authentication.json'),
  );
  await expect(dialog.getByRole('combobox', { name: 'Class' })).toHaveValue('authentication');
  await expect(dialog.getByRole('spinbutton', { name: 'Depth' })).toHaveValue('2');
  await expect(dialog.getByRole('checkbox', { name: 'required' })).toBeChecked();
  await expect(dialog.getByRole('checkbox', { name: 'recommended' })).toBeChecked();
  await expect(dialog.getByRole('checkbox', { name: 'optional' })).not.toBeChecked();
  await dialog.getByRole('spinbutton', { name: 'Depth' }).fill('1');
  await dialog.getByRole('checkbox', { name: 'recommended' }).uncheck();
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  const field = await saved(admin, api, 'auth');
  expect(field('user')).toMatchObject({ type: '{...}', required: true });
  expect(field('user.name')).toBeUndefined();
  expect(field('src_endpoint')).toBeUndefined();
});

test('a CSV of fields comes in by its header, example and Elasticsearch type among them', async ({
  admin,
  leader,
  api,
}) => {
  const csv = path.join(test.info().outputDir, 'fields.csv');
  await mkdir(test.info().outputDir, { recursive: true });
  await writeFile(
    csv,
    'Field,Type,Required,Description,Example,Elasticsearch,Owner\n' +
      'client_ip,str,yes,Where the request came from.,10.0.0.1,ip,web team\n' +
      'status,i64,,The HTTP status answered.,200,,web team\n',
  );
  const dialog = await addFrom(admin, leader, 'CSV of fields', 'web', csv);
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('cell', { name: 'Example', exact: true })).toBeVisible();
  const field = await saved(admin, api, 'web');
  expect(field('client_ip')).toEqual({
    field: 'client_ip',
    type: 'str',
    required: true,
    description: 'Where the request came from.',
    example: '10.0.0.1',
    elasticsearch: { type: 'ip' },
  });
  expect(field('status')).toMatchObject({ type: 'i64', example: '200' });
});

test("a CSV's columns and types are picked from its first rows when its header names neither", async ({
  admin,
  leader,
  api,
}) => {
  const csv = path.join(test.info().outputDir, 'columns.csv');
  await mkdir(test.info().outputDir, { recursive: true });
  await writeFile(
    csv,
    'Attribute,Kind,Meaning\n' +
      'host,VARCHAR,The machine it ran on.\n' +
      'port,int,The port it listened on.\n' +
      'tags,widget,What the team calls it.\n',
  );
  const dialog = await addFrom(admin, leader, 'CSV of fields', 'inventory', csv);
  await expect(dialog.getByText('Choose the column that holds')).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Add', exact: true })).toBeDisabled();
  await dialog.getByRole('combobox', { name: 'Column Attribute holds' }).selectOption('field');
  await dialog.getByRole('combobox', { name: 'Column Kind holds' }).selectOption('type');
  await dialog.getByRole('combobox', { name: 'Column Meaning holds' }).selectOption('description');
  await expect(dialog.getByRole('combobox', { name: 'Type of VARCHAR' })).toHaveValue('str');
  await expect(dialog.getByRole('combobox', { name: 'Type of int' })).toHaveValue('i64');
  const widget = dialog.getByRole('combobox', { name: 'Type of widget' });
  await expect(widget).toHaveValue('str');
  await expect(dialog.getByText('not a type smeltr knows')).toBeVisible();
  await widget.selectOption('{...}');
  await dialog.screenshot({ path: 'results/schemas-csv-picker.png' });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  const field = await saved(admin, api, 'inventory');
  expect(field('host')).toMatchObject({ type: 'str', description: 'The machine it ran on.' });
  expect(field('port')?.type).toBe('i64');
  expect(field('tags')?.type).toBe('{...}');
});

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
  await api.put(`${SHARED}/pipelines/logins${ON}`, {
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
  await api.put(`${SHARED}/samples/logins-sample${ON}`, {
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
