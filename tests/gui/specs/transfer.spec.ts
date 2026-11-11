// Moving a pipeline between leaders: exporting one with what it reads, as a bundle in a zip. Each test drives the
// real binary through the GUI from an empty data directory; `api` only arranges what a test is not about.
//
// A fresh browser edits branch `work` in the `shared` scope, which is where `api` arranges things too.
import { test, expect, goto } from '../fleet';

const ON = '?branch=work';
const SHARED = '/api/areas/shared';

test('a pipeline is exported with the pipeline it calls, and its routes when asked', async ({
  admin,
  api,
  leader,
}) => {
  await api.put(`${SHARED}/grok-patterns${ON}`, [
    { id: 'FW_LINE', pattern: '%{IP:ip} %{WORD:action}' },
  ]);
  await api.put(`${SHARED}/pipelines/common${ON}`, {
    id: 'common',
    name: 'Common',
    steps: [
      {
        id: 'line',
        type: 'parser',
        input: '_raw',
        kind: { type: 'grok', patterns: ['%{FW_LINE}'] },
      },
    ],
  });
  await api.put(`${SHARED}/pipelines/firewall${ON}`, {
    id: 'firewall',
    name: 'Firewall',
    steps: [{ type: 'call', id: 'common', pipeline: 'common' }],
  });
  await api.put(`${SHARED}/pipelines/other${ON}`, { id: 'other', name: 'Other', steps: [] });
  await api.put(`${SHARED}/sources${ON}`, [{ id: 'fw', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`${SHARED}/destinations${ON}`, [
    { id: 'out', type: 'kafka', brokers: 'b:9092', defaultTopic: 'out' },
  ]);
  await api.put(`${SHARED}/routes${ON}`, [
    { id: 'fw-out', source: 'fw', pipeline: 'firewall', deliver: [{ to: ['out'] }] },
  ]);
  await api.put(`${SHARED}/samples/lines${ON}`, {
    id: 'lines',
    name: 'lines',
    columns: [{ name: '_raw', type: 'str' }],
    records: [{ _raw: '10.0.0.1 allow' }],
  });
  await api.put(`${SHARED}/tests/firewall-lines${ON}`, {
    name: 'firewall lines',
    pipeline: 'firewall',
    sample: 'lines',
    lockedAt: 0,
    fields: [],
    rows: [0],
  });

  await goto(admin, leader, '#/build/pipelines');
  await admin
    .getByRole('row', { name: /Firewall/ })
    .getByRole('button', { name: 'Export' })
    .click();
  const dialog = admin.getByRole('dialog', { name: /export firewall/ });
  const row = (kind: string, id: string) =>
    dialog.getByRole('row', { name: new RegExp(`^${kind} ${id}\\b`) });
  await expect(row('Pipeline', 'firewall')).toBeVisible();
  await expect(row('Pipeline', 'common')).toBeVisible();
  await expect(row('Grok pattern', 'FW_LINE')).toBeVisible();
  await expect(row('Test', 'firewall-lines')).toBeVisible();
  await expect(row('Sample', 'lines')).toContainText('With its tests');
  await expect(row('Route', 'fw-out')).toBeHidden();

  // A sample leaves with its last test.
  await dialog.getByRole('checkbox', { name: 'Export Test firewall-lines' }).uncheck();
  await expect(row('Sample', 'lines')).toBeHidden();
  const untested = await dialog.getByRole('link', { name: 'Download' }).getAttribute('href');
  const lean = await (await admin.request.get(new URL(untested!, leader.url).toString())).body();
  expect(lean.includes('tests/firewall-lines.json')).toBe(false);
  expect(lean.includes('samples/lines.json')).toBe(false);
  expect(lean.includes('pipelines/common.json')).toBe(true);
  await dialog.getByRole('checkbox', { name: 'Export Test firewall-lines' }).check();

  // Without the pipeline itself, the export is only what it reads.
  await dialog.getByRole('checkbox', { name: 'Export Pipeline firewall' }).uncheck();
  const readsOnly = await dialog.getByRole('link', { name: 'Download' }).getAttribute('href');
  const reads = await (await admin.request.get(new URL(readsOnly!, leader.url).toString())).body();
  expect(reads.includes('pipelines/firewall.json')).toBe(false);
  expect(reads.includes('pipelines/common.json')).toBe(true);
  await dialog.getByRole('checkbox', { name: 'Export Pipeline firewall' }).check();

  await dialog.getByRole('switch', { name: 'Include routes, sources and destinations' }).check();
  await expect(row('Route', 'fw-out')).toBeVisible();
  await expect(row('Source', 'fw')).toBeVisible();
  await expect(row('Destination', 'out')).toBeVisible();

  const href = await dialog.getByRole('link', { name: 'Download' }).getAttribute('href');
  const res = await admin.request.get(new URL(href!, leader.url).toString());
  expect(res.headers()['content-disposition']).toContain('firewall.zip');
  const zip = await res.body();
  expect(zip.subarray(0, 2).toString()).toBe('PK');
  // A zip names each file in the clear, so its names can be found in the bytes.
  for (const name of ['pipelines/firewall.json', 'pipelines/common.json', 'routes.json']) {
    expect(zip.includes(name), name).toBe(true);
  }
  expect(zip.includes('pipelines/other.json')).toBe(false);
});

test('an export is imported, a taken id renamed and a secret matched to one here', async ({
  admin,
  api,
  leader,
}) => {
  await api.put(`${SHARED}/pipelines/common${ON}`, { id: 'common', name: 'Common', steps: [] });
  await api.put(`${SHARED}/pipelines/firewall${ON}`, {
    id: 'firewall',
    name: 'Firewall',
    steps: [{ type: 'call', id: 'common', pipeline: 'common' }],
  });
  await api.put(`${SHARED}/sources${ON}`, [
    {
      id: 'fw',
      type: 'kafka',
      brokers: 'b:9092',
      topics: ['fw'],
      properties: { 'security.protocol': 'SASL_SSL' },
      credentials: { 'sasl.password': 'FW_PASSWORD' },
    },
  ]);
  await api.put(`${SHARED}/routes${ON}`, [
    { id: 'fw-out', source: 'fw', pipeline: 'firewall', deliver: [] },
  ]);
  const zip = await admin.request.get(
    `${leader.url}${SHARED}/pipelines/firewall/export${ON}&routes=true`,
  );
  // What the importing leader holds instead: no firewall or its route and source, and a different common.
  await api.put(`${SHARED}/routes${ON}`, []);
  await api.put(`${SHARED}/sources${ON}`, []);
  await api.delete(`${SHARED}/pipelines/firewall${ON}`);
  await api.put(`${SHARED}/pipelines/common${ON}`, { id: 'common', name: 'Ours', steps: [] });
  await api.post('/api/secrets', { name: 'kafka-fw', custody: 'worker-local', description: '' });

  await goto(admin, leader, '#/build/pipelines');
  await admin.getByRole('button', { name: 'Import' }).click();
  const dialog = admin.getByRole('dialog', { name: /import pipeline/ });
  await dialog.getByLabel('Import into').selectOption('Shared');
  await dialog.getByLabel('Export file').setInputFiles({
    name: 'firewall.zip',
    mimeType: 'application/zip',
    buffer: await zip.body(),
  });
  const row = (kind: string, id: string) =>
    dialog.getByRole('row', { name: new RegExp(`^${kind} ${id} `) });
  await expect(row('Pipeline', 'firewall')).toContainText('New');
  await expect(row('Pipeline', 'common')).toContainText('Taken');
  await expect(row('Route', 'fw-out')).toContainText('New');
  const importButton = dialog.getByRole('button', { name: 'Import' });
  await expect(importButton).toBeDisabled();

  const common = dialog.getByRole('region', { name: 'Pipeline common' });
  // Only the name differs, from the one here to the imported one.
  await expect(common.getByRole('list', { name: /^What differs/ })).toHaveText(
    /^\s*name:\s*Ours\s*→\s*Common\s*$/,
  );
  await common.getByLabel('Keep').selectOption('Both, rename imported');
  await common.getByLabel('New id').fill('common-fw');
  await dialog.getByLabel('Match for FW_PASSWORD').selectOption('kafka-fw');
  await importButton.click();
  await expect(dialog).toBeHidden();
  await expect(admin.getByRole('row', { name: /Firewall/ })).toBeVisible();

  const saved = await api.get(`${SHARED}${ON}`);
  const pipelines = saved.config.pipelines.map((p: { id: string }) => p.id).sort();
  expect(pipelines).toEqual(['common', 'common-fw', 'firewall']);
  const firewall = saved.config.pipelines.find((p: { id: string }) => p.id === 'firewall');
  expect(firewall.steps[0].pipeline).toBe('common-fw');
  expect(saved.config.sources[0].credentials['sasl.password']).toBe('kafka-fw');
});

test('only the ticked part of an export is imported, and leaving out what the rest needs is refused', async ({
  admin,
  api,
  leader,
}) => {
  await api.put(`${SHARED}/pipelines/common${ON}`, { id: 'common', name: 'Common', steps: [] });
  await api.put(`${SHARED}/pipelines/firewall${ON}`, {
    id: 'firewall',
    name: 'Firewall',
    steps: [{ type: 'call', id: 'common', pipeline: 'common' }],
  });
  const zip = await admin.request.get(`${leader.url}${SHARED}/pipelines/firewall/export${ON}`);
  await api.delete(`${SHARED}/pipelines/firewall${ON}`);
  await api.put(`${SHARED}/pipelines/common${ON}`, { id: 'common', name: 'Ours', steps: [] });

  await goto(admin, leader, '#/build/pipelines');
  await admin.getByRole('button', { name: 'Import' }).click();
  const dialog = admin.getByRole('dialog', { name: /import pipeline/ });
  await dialog.getByLabel('Import into').selectOption('Shared');
  await dialog.getByLabel('Export file').setInputFiles({
    name: 'firewall.zip',
    mimeType: 'application/zip',
    buffer: await zip.body(),
  });
  const common = dialog.getByRole('region', { name: 'Pipeline common' });
  await expect(common).toBeVisible();
  // Left out, the taken common needs no answer: the firewall calls the one here.
  await dialog.getByRole('checkbox', { name: 'Import Pipeline common' }).uncheck();
  await expect(common).toBeHidden();
  await dialog.getByRole('button', { name: 'Import' }).click();
  await expect(dialog).toBeHidden();
  const saved = await api.get(`${SHARED}${ON}`);
  const names = saved.config.pipelines.map((p: { name: string }) => p.name).sort();
  expect(names).toEqual(['Firewall', 'Ours']);

  // With nothing here under that id, the firewall would call a pipeline that does not exist.
  await api.delete(`${SHARED}/pipelines/firewall${ON}`);
  await api.delete(`${SHARED}/pipelines/common${ON}`);
  await admin.getByRole('button', { name: 'Import' }).click();
  await dialog.getByLabel('Import into').selectOption('Shared');
  await dialog.getByLabel('Export file').setInputFiles({
    name: 'firewall.zip',
    mimeType: 'application/zip',
    buffer: await zip.body(),
  });
  await dialog.getByRole('checkbox', { name: 'Import Pipeline common' }).uncheck();
  await dialog.getByRole('button', { name: 'Import' }).click();
  await expect(
    admin
      .getByText(/common/)
      .filter({ hasText: /pipeline/i })
      .first(),
  ).toBeVisible();
  await expect(dialog).toBeVisible();
  expect((await api.get(`${SHARED}${ON}`)).config.pipelines).toEqual([]);
});
