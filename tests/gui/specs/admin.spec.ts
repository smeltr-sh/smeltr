// What a system administrator does to smeltr itself: bring workers in and take them out, see what each runs,
// configure the service, and hold its credentials. Each test drives the Admin and Status pages of a fleet of
// its own; the API only arranges what a test is not about.
import { test, expect, goto, type Api } from '../fleet';
import { FakeModel, MODEL_KEY, request, say } from '../model';
import {
  described,
  openAdmin,
  openWorkersTab,
  startWorkerFromLines,
  workerDetail,
  workerRow,
  workerRowOf,
} from '../moves/admin';
import { conversationLink, openAgent } from '../moves/agent';
import { fullChain } from '../moves/certificates';

test('an enrollment code minted under Enrollment codes brings a worker in', async ({
  admin,
  fleet,
  leader,
}) => {
  // S1
  await openAdmin(admin, 'Workers');
  await expect(admin.getByText('No worker has ever enrolled.')).toBeVisible();

  await openWorkersTab(admin, 'Enrollment codes');
  await admin.getByRole('button', { name: '+ Add enrollment code' }).click();
  const mint = admin.getByRole('dialog', { name: 'new enrollment code' });
  await expect(mint.getByLabel(/^Group/)).toHaveValue('default');
  await mint.getByRole('button', { name: 'Mint' }).click();

  await expect(admin.getByText('Copy this enrollment code now')).toBeVisible();
  await expect(admin.getByLabel('The address this worker will reach the leader at')).toHaveValue(
    leader.url,
  );
  const lines = await admin
    .locator('pre')
    .filter({ hasText: 'SMELTR_WORKER_ENROLLMENT_CODE=' })
    .innerText();
  expect(lines).toContain('SMELTR_WORKER_ENABLED=1');
  expect(lines).toContain('SMELTR_WORKER_GROUP=default');
  await admin.getByRole('button', { name: 'Done' }).click();

  const code = admin.getByRole('row').filter({ hasText: 'permanent' });
  await expect(code.getByRole('cell', { name: '0 of 1' })).toBeVisible();

  startWorkerFromLines(fleet, 'gui-enrolled', lines);

  await openWorkersTab(admin, 'Workers');
  const row = await workerRow(admin, 'gui-enrolled', 'Heartbeating');
  await expect(row.getByRole('cell', { name: 'default', exact: true }).first()).toBeVisible();
  await openWorkersTab(admin, 'Enrollment codes');
  await expect(code.getByRole('cell', { name: /1 of 1.*gui-enrolled/ })).toBeVisible();
  await expect(code.getByRole('cell', { name: 'spent out' })).toBeVisible();
});

test('an enrollment code withdrawn under Enrollment codes is no longer offered', async ({
  admin,
}) => {
  // S1
  await openAdmin(admin, 'Workers');
  await openWorkersTab(admin, 'Enrollment codes');
  await admin.getByRole('button', { name: '+ Add enrollment code' }).click();
  await admin
    .getByRole('dialog', { name: 'new enrollment code' })
    .getByRole('button', { name: 'Mint' })
    .click();
  await admin.getByRole('button', { name: 'Done' }).click();

  const code = admin.getByRole('row').filter({ hasText: 'permanent' });
  await code.getByRole('button', { name: 'Withdraw' }).click();
  await admin
    .getByRole('dialog', { name: 'withdraw enrollment code' })
    .getByRole('button', { name: 'Withdraw' })
    .click();
  // A code withdrawn before it brought anybody in has nothing left to show.
  await expect(code).toHaveCount(0);
  await expect(
    admin.getByText('No enrollment code is waiting to be used, and none has brought a worker in.'),
  ).toBeVisible();
});

test('a revoked worker is shown revoked, and once removed it leaves the list', async ({
  admin,
  fleet,
  leader,
  api,
}) => {
  // S2
  await fleet.startWorker(leader, api, 'default', 'w-revoke');
  await openAdmin(admin, 'Workers');
  const row = await workerRow(admin, 'w-revoke', 'Heartbeating');

  // A worker that is still heartbeating is not offered for removal.
  await admin.getByRole('button', { name: 'More actions for w-revoke' }).click();
  await expect(admin.getByRole('button', { name: 'Revoke', exact: true })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Remove', exact: true })).toHaveCount(0);
  await admin.getByRole('button', { name: 'Revoke', exact: true }).click();
  await admin
    .getByRole('dialog', { name: 'revoke worker' })
    .getByRole('button', { name: 'Revoke' })
    .click();
  await expect(row.getByRole('cell', { name: 'Revoked', exact: true })).toBeVisible();

  // A revoked worker is removed at once: its last heartbeat, still recent, came before the revocation.
  await admin.getByRole('button', { name: 'More actions for w-revoke' }).click();
  await expect(admin.getByRole('button', { name: 'Remove', exact: true })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Revoke', exact: true })).toHaveCount(0);
  await admin.getByRole('button', { name: 'Remove', exact: true }).click();
  await admin
    .getByRole('dialog', { name: 'remove worker' })
    .getByRole('button', { name: 'Remove' })
    .click();
  await expect(workerRowOf(admin, 'w-revoke')).toHaveCount(0);
  await admin.reload();
  await expect(workerRowOf(admin, 'w-revoke')).toHaveCount(0);
  // The enrollment code still says whom it brought in; the worker itself is in no list.
  await openWorkersTab(admin, 'Enrollment codes');
  await expect(admin.getByRole('cell', { name: /1 of 1.*w-revoke/ })).toBeVisible();
});

test("the Cluster page shows each worker's deployed commit and what it does offline", async ({
  admin,
  fleet,
  leader,
  api,
}) => {
  // S3
  // A deploy that moves the default group, so its workers have a commit to report.
  const branch = 'rename-default';
  await api.put(`/api/worker-groups/default?branch=${branch}`, { name: 'Default fleet' });
  await api.openChange(branch, 'Rename the default group');
  const head: string = (await api.deploy(branch)).head;

  await fleet.startWorker(leader, api, 'default', 'w-stops', {
    SMELTR_WORKER_STOP_AFTER_UNREACHABLE_SECS: '45',
  });
  await fleet.startWorker(leader, api, 'default', 'w-keeps');

  let detail = await workerDetail(admin, 'w-stops');
  await expect(described(detail, 'Offline policy')).toHaveText('45s');
  await expect(described(detail, 'Commit')).toHaveText(head.slice(0, 7), { timeout: 60_000 });
  await expect(described(detail, 'Group')).toHaveText('default');

  detail = await workerDetail(admin, 'w-keeps');
  await expect(described(detail, 'Offline policy')).toHaveText('never');
  await expect(described(detail, 'Commit')).toHaveText(head.slice(0, 7), { timeout: 60_000 });
});

test('the session deadlines are changed under System, and an impossible pair is refused', async ({
  admin,
}) => {
  // S6
  await openAdmin(admin, 'System');
  await admin.getByRole('link', { name: 'Sessions', exact: true }).click();
  const idle = admin.getByLabel('Idle timeout');
  const absolute = admin.getByLabel('Maximum session age');
  await expect(idle).toBeVisible();
  const maximum = await absolute.inputValue();

  // Longer than the maximum age, in whichever unit the form shows each.
  await idle.fill(String(Number(maximum) * 100_000));
  await expect(admin.getByRole('alert')).toHaveText(
    'An idle timeout longer than the maximum session age could never be reached.',
  );
  await expect(admin.getByRole('button', { name: 'Save sessions' })).toBeDisabled();
  await admin.getByRole('button', { name: 'Discard' }).click();

  await idle.fill('17');
  await admin.getByRole('button', { name: 'Save sessions' }).click();
  await expect(admin.getByRole('status')).toHaveText(
    'saved -- these apply to every session on the next request it makes',
  );

  await admin.reload();
  await expect(admin.getByLabel('Idle timeout')).toHaveValue('17');
});

test('the leader certificate is reissued under System', async ({ admin }) => {
  // S6
  await openAdmin(admin, 'System');
  await admin
    .getByRole('navigation', { name: 'System settings' })
    .getByRole('link', { name: 'Leader certificate', exact: true })
    .click();
  await expect(admin.getByRole('heading', { name: /Current authority/ })).toBeVisible();
  await expect(admin.getByRole('link', { name: 'Download ca.pem' })).toBeVisible();

  const served = admin.getByRole('region', { name: /^Leader certificate/ });
  // The page shortens a fingerprint to its two ends, which is what a person compares.
  const fingerprint = served.getByText(/^[0-9a-f]{16}…[0-9a-f]{8}$/);
  const before = await fingerprint.innerText();
  await served.getByRole('button', { name: 'Issue a new one' }).click();
  await admin.getByRole('dialog').getByRole('button', { name: 'Issue a new one' }).click();
  await expect(served.getByRole('status')).toHaveText('a new certificate is in force');
  await expect(fingerprint).not.toHaveText(before);
});

test('System and Cluster show the build each machine runs', async ({
  admin,
  fleet,
  leader,
  api,
}) => {
  // S7
  const info = await api.get('/api/system/info');
  const shown: string = info.release ?? info.smeltrVersion;
  expect(shown).toBeTruthy();

  await openAdmin(admin, 'System');
  const general = admin.getByRole('form', { name: 'General' });
  await expect(general.getByRole('heading', { name: 'Version' })).toBeVisible();
  await expect(general.getByText(shown, { exact: true })).toBeVisible();

  await fleet.startWorker(leader, api, 'default', 'w-build');
  const detail = await workerDetail(admin, 'w-build');
  await expect(described(detail, 'Version')).toContainText(shown);
  await expect(described(detail, 'Disk')).toContainText('free of');
});

test('a secret is set once, checked without being shown, and never read back', async ({
  admin,
  api,
}) => {
  // S8
  const value = 'gui-secret-value-6d1f0e';
  await openAdmin(admin, 'Secrets');
  await expect(admin.getByText('No secrets declared yet.')).toBeVisible();

  await admin.getByRole('button', { name: '+ Add secret' }).click();
  const add = admin.getByRole('dialog', { name: 'new secret' });
  await add.getByRole('textbox', { name: /^Name\b/ }).fill('es-ingest-key');
  await add.getByLabel('value', { exact: true }).fill(value);
  await add.getByRole('textbox', { name: 'Description' }).fill('the ingest key');
  await add.getByRole('button', { name: 'Add' }).click();

  const row = admin.getByRole('row').filter({ hasText: 'es-ingest-key' });
  await expect(row.getByRole('cell', { name: 'v1', exact: true })).toBeVisible();
  await expect(row.getByRole('cell', { name: /^nothing/ })).toBeVisible();

  await row.getByRole('button', { name: 'Check' }).click();
  const candidate = admin.getByLabel('the value you think is stored', { exact: true });
  await candidate.fill('a-different-value');
  await admin.getByRole('button', { name: 'Check', exact: true }).last().click();
  await expect(admin.getByText('That does not match.')).toBeVisible();
  await candidate.fill(value);
  await admin.getByRole('button', { name: 'Check', exact: true }).last().click();
  await expect(admin.getByText('That matches what is stored.')).toBeVisible();

  // Neither the page nor anything the leader answers carries the value.
  await admin.reload();
  await expect(row).toBeVisible();
  expect(await admin.content()).not.toContain(value);
  expect(JSON.stringify(await api.get('/api/secrets'))).not.toContain(value);
});

test('rotating a secret points what uses it at the new version, and deploying it runs that', async ({
  admin,
  api,
}) => {
  // S8
  await api.post('/api/secrets', {
    name: 'es-key',
    custody: 'leader-stored',
    description: 'the ingest key',
    value: 'first-value',
  });
  const using = 'use-es-key';
  await api.put(`/api/destinations?branch=${using}&scope=default`, [
    {
      id: 'es-out',
      type: 'elasticsearch',
      url: 'http://127.0.0.1:9',
      defaultIndex: 'logs',
      auth: { secret: 'es-key', version: 1 },
    },
  ]);
  await api.openChange(using, 'Send to Elasticsearch');
  await api.deploy(using);

  await openAdmin(admin, 'Secrets');
  const row = admin.getByRole('row').filter({ hasText: 'es-key' });
  await expect(row.getByRole('cell', { name: 'v1', exact: true })).toHaveCount(2);

  await row.getByRole('button', { name: 'New value' }).click();
  await admin.getByLabel('the new value', { exact: true }).fill('second-value');
  await admin.getByRole('button', { name: 'Store', exact: true }).click();
  await expect(
    admin.getByText(
      'Stored, and pointed 1 reference across default at it. It deploys when the change does.',
    ),
  ).toBeVisible();
  // The change goes on the branch already in hand, which the page names.
  const note = await admin.getByText(/Current branch: /).innerText();
  const rotating = /Current branch:\s*(\S+)\.$/.exec(note.trim())![1];
  await expect(row.getByRole('cell', { name: 'v2', exact: true })).toBeVisible();
  await expect(row.getByText('stored, not deployed')).toBeVisible();

  // Storing the value opened the change; deploying it is the ordinary change flow, which other stories drive.
  await api.deploy(rotating);

  await admin.reload();
  await expect(row.getByRole('cell', { name: 'v2', exact: true })).toHaveCount(2);
  await expect(row.getByText('stored, not deployed')).toHaveCount(0);
});

test('a new value for a secret nothing names asks for no branch and drafts no change', async ({
  admin,
  api,
}) => {
  // S8
  await api.post('/api/secrets', {
    name: 'spare-key',
    custody: 'leader-stored',
    description: '',
    value: 'first-value',
  });
  // On main, where an edit would ask for a branch.
  await admin.evaluate(() => localStorage.setItem('smeltr-branch', 'main'));
  await openAdmin(admin, 'Secrets');
  await admin.reload();
  const row = admin.getByRole('row').filter({ hasText: 'spare-key' });

  await row.getByRole('button', { name: 'New value' }).click();
  await admin.getByLabel('the new value', { exact: true }).fill('second-value');
  await admin.getByRole('button', { name: 'Store', exact: true }).click();
  await expect(admin.getByText('Stored. Nothing references it yet.')).toBeVisible();
  await expect(row.getByRole('cell', { name: 'v2', exact: true })).toBeVisible();
  await expect(admin.getByRole('dialog', { name: 'branch required' })).toHaveCount(0);
  expect((await api.get('/api/branches')).branches).toEqual([]);
});

test('an identity presents a certificate from the list, and replacing it points the identity at the new version', async ({
  admin,
  api,
  fleet,
  leader,
}) => {
  const pki = await fleet.externalPki();
  await api.post('/api/certificates', {
    name: 'client-tls',
    description: '',
    certificate: fullChain(pki),
    privateKey: pki.privateKey,
  });
  const identities = async () =>
    (await api.get('/api/config?branch=work&scope=shared')).config.identities;

  await goto(admin, leader, '#/build/identities');
  await admin.getByRole('button', { name: '+ Add identity' }).click();
  const dialog = admin.getByRole('dialog', { name: 'new identity' });
  await dialog.getByRole('textbox', { name: 'id' }).fill('acme');
  const presents = dialog.getByRole('combobox', { name: /^presents/ });
  await presents.selectOption('client-tls');
  await expect(dialog.getByText(/^version 1/)).toBeVisible();
  await admin.screenshot({ path: test.info().outputPath('identity-presents.png') });
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect
    .poll(identities)
    .toEqual([{ id: 'acme', certificate: { secret: 'client-tls', version: 1 } }]);

  await openAdmin(admin, 'Certificates');
  const row = admin.getByRole('row').filter({ hasText: 'client-tls' });
  await row.getByRole('button', { name: 'Replace' }).click();
  const replace = admin.getByRole('dialog', { name: /^replace / });
  await replace.getByRole('textbox', { name: 'Certificate', exact: true }).fill(fullChain(pki));
  await replace.getByRole('textbox', { name: 'Private key' }).fill(pki.privateKey);
  await replace.getByRole('button', { name: 'Replace', exact: true }).click();
  await expect(
    admin.getByText(
      'Replaced. 1 identity in group shared will present the new version once you deploy the open change.',
    ),
  ).toBeVisible();
  await admin.screenshot({ path: test.info().outputPath('certificate-replaced.png') });
  expect(await identities()).toEqual([
    { id: 'acme', certificate: { secret: 'client-tls', version: 2 } },
  ]);
});

test('the git remote form takes an http or https URL, and warns about a credential in plain text', async ({
  admin,
}) => {
  // G5
  await openAdmin(admin, 'System');
  await admin.getByRole('link', { name: 'Git remote', exact: true }).click();
  const url = admin.getByRole('textbox', { name: /^URL\b/ });
  const save = admin.getByRole('button', { name: 'Save' });

  await url.fill('git@git.example.com:team/config.git');
  await save.click();
  await expect(admin.getByRole('alert')).toHaveText(
    '"git@git.example.com:team/config.git" is not a whole http:// or https:// URL. SSH is not supported',
  );

  await url.fill('http://bot:pw@git.example.com/team/config.git');
  await expect(admin.getByRole('status').filter({ hasText: 'unencrypted' })).toHaveText(
    'Its credentials will cross the network unencrypted. Put the user and password in the user and token fields instead of the URL.',
  );
  await expect(save).toBeEnabled();

  await url.fill('https://git.example.com/team/config.git');
  await expect(admin.getByText('unencrypted')).toHaveCount(0);
  await expect(save).toBeEnabled();
});

// S10
test.fixme('System → License shows the bytes this leader read this year', async () => {
  // The yearly total is not built yet.
});

/** Turn AI agents on over the API, for a test that is not about it, against `model`: one in this process the
 * test keeps running, or one that answers only this. */
async function agentsOn(api: Api, model?: FakeModel) {
  const endpoint = model ?? (await FakeModel.start());
  try {
    await api.put('/api/settings/agents', {
      enabled: true,
      provider: 'anthropic',
      endpoint: endpoint.base,
      model: 'claude-opus-5-5',
      apiKey: MODEL_KEY,
    });
  } finally {
    if (!model) await endpoint.stop();
  }
}

test('AI agents turned on under System offer the agent screens', async ({ admin }) => {
  // S12
  const model = await FakeModel.start();
  try {
    await admin.getByRole('button', { name: /^signed in as / }).click();
    await expect(admin.getByRole('button', { name: 'Sign out' })).toBeVisible();
    await expect(admin.getByRole('button', { name: 'Agent permissions' })).toHaveCount(0);
    await admin.keyboard.press('Escape');

    await openAdmin(admin, 'System');
    await admin.getByRole('link', { name: 'AI agents', exact: true }).click();
    await admin.getByRole('switch', { name: 'AI agents enabled' }).check();
    await expect(admin.getByRole('alert')).toHaveText('Turning AI agents on needs: API key.');
    await admin.getByRole('textbox', { name: /^Endpoint URL\b/ }).fill(model.base);
    await admin.getByRole('textbox', { name: 'API key', exact: true }).fill('sk-wrong');
    await expect(admin.getByText('Saving asks the endpoint for a short answer')).toBeVisible();
    await admin.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(admin.getByRole('alert')).toContainText('answered 401: invalid x-api-key');

    await admin.getByRole('textbox', { name: 'API key', exact: true }).fill(MODEL_KEY);
    await admin.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(admin.getByRole('button', { name: 'replace the API key' })).toBeVisible();

    await admin.getByRole('button', { name: /^signed in as / }).click();
    await expect(admin.getByRole('button', { name: 'Agent permissions' })).toBeVisible();
    await expect(admin.getByRole('button', { name: 'Agent', exact: true })).toBeVisible();
  } finally {
    await model.stop();
  }
});

test('a person sets what their agent may use from their name', async ({ admin, api }) => {
  // P8
  await agentsOn(api);
  await admin.reload();
  await admin.getByRole('button', { name: /^signed in as / }).click();
  await admin.getByRole('button', { name: 'Agent permissions' }).click();
  await expect(admin.getByRole('heading', { name: 'Agent permissions' })).toBeVisible();
  await expect(admin.getByRole('combobox', { name: /^Every area\b/ })).toHaveValue('viewer');

  await admin.getByRole('combobox', { name: 'shared level' }).selectOption('editor');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByRole('status')).toHaveText('agent permissions saved');
  expect(await api.get('/api/me/agent-permissions')).toEqual({
    kind: 'upto',
    default: 'viewer',
    areas: { shared: 'editor' },
  });

  await admin.getByRole('button', { name: 'Everything you hold' }).click();
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByRole('status')).toHaveText('agent permissions saved');
  expect(await api.get('/api/me/agent-permissions')).toEqual({ kind: 'everything' });
});

test('a person asks their agent, sees what it asked the leader, and deletes the conversation', async ({
  admin,
  api,
}) => {
  // P9
  const model = await FakeModel.start();
  try {
    await agentsOn(api, model);
    model.answer(
      [request('GET', '/api/pipelines', { scope: 'shared' })],
      [say('You have **no pipelines** yet.')],
    );
    await admin.reload();
    await openAgent(admin);
    await expect(admin.getByText('No conversations yet.')).toBeVisible();

    await admin.getByRole('textbox', { name: 'Message' }).fill('What pipelines do I have?');
    await admin.getByRole('button', { name: 'Send', exact: true }).click();
    await expect(admin.getByText('You have no pipelines yet.')).toBeVisible();
    // The answer's Markdown is rendered, not shown as typed.
    await expect(admin.locator('strong', { hasText: 'no pipelines' })).toBeVisible();
    await expect(conversationLink(admin, 'What pipelines do I have')).toBeVisible();

    const call = admin.getByRole('button', { name: /GET \/api\/pipelines\?scope=shared/ });
    await expect(call).toContainText('200');
    await call.click();
    await expect(admin.getByRole('heading', { name: 'Answered' })).toBeVisible();
    // The leader's answer went back to the model as the call's result.
    expect(JSON.stringify(model.asked.at(-1)?.messages.at(-1))).toContain('tool_result');

    await admin.getByRole('button', { name: 'Delete conversation' }).click();
    await admin
      .getByRole('dialog', { name: 'delete conversation' })
      .getByRole('button', { name: 'Delete', exact: true })
      .click();
    await expect(admin.getByText('No conversations yet.')).toBeVisible();
    expect((await api.get('/api/agent/conversations')).conversations).toEqual([]);
  } finally {
    await model.stop();
  }
});
