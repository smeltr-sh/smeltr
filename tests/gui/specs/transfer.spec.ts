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

  await goto(admin, leader, '#/build/pipelines');
  await admin
    .getByRole('row', { name: /Firewall/ })
    .getByRole('button', { name: 'Export' })
    .click();
  const dialog = admin.getByRole('dialog', { name: /export firewall/ });
  const row = (kind: string) => dialog.getByRole('row', { name: new RegExp(`^${kind}`) });
  await expect(row('Pipelines')).toContainText('firewall, common');
  await expect(row('Grok patterns')).toContainText('FW_LINE');
  await expect(row('Routes')).toBeHidden();

  await dialog.getByRole('switch', { name: 'Include routes, sources and destinations' }).check();
  await expect(row('Routes')).toContainText('fw-out');
  await expect(row('Sources')).toContainText('fw');
  await expect(row('Destinations')).toContainText('out');

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
