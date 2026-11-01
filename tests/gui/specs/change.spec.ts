// Change and deploy (W1-W11) and reading the history (S4): editing on a branch, opening a change, reviewing
// it, bringing it current, deploying it, closing it, rolling back, and the branch list.
import { test, expect, addAccount, goto, type Api, type Leader } from '../fleet';
import type { Page } from '@playwright/test';
import { historyRow } from '../moves/change';

type Table = { id: string; type: 'inline'; headers: string[]; rows: string[][] };

/** A hand-built lookup table with one row: the smallest object a branch can carry. */
function table(id: string, value = '1'): Table {
  return { id, type: 'inline', headers: ['key', 'value'], rows: [['red', value]] };
}

/** Saves `tables` as `shared/`'s whole set of lookup tables on `branch`, as one commit. */
async function putTables(api: Api, branch: string, tables: Table[]) {
  await api.put(`/api/areas/shared/lookups?branch=${encodeURIComponent(branch)}`, tables);
}

/** A branch carrying `tables`, opened for review as `title`. */
async function arrangeChange(api: Api, branch: string, title: string, tables: Table[]) {
  await putTables(api, branch, tables);
  await api.openChange(branch, title);
}

/** The branch chip in the nav, which names the branch every page reads and writes. */
function branchChip(page: Page) {
  return page.getByRole('button', { name: /^Branch: / });
}

/** Picks `name` from the branch chip, which reloads the page onto it. */
async function switchBranch(page: Page, name: string) {
  await branchChip(page).click();
  await page.getByRole('button', { name: new RegExp(`^${name} `) }).click();
  await expect(branchChip(page)).toHaveAccessibleName(new RegExp(`^Branch: ${name}\\b`));
}

/** Adds a hand-built lookup table named `id` on the Lookup tables page, without saving it. */
async function addTable(page: Page, leader: Leader, id: string) {
  await goto(page, leader, '#/build/lookups');
  await page.getByRole('button', { name: '+ Add table' }).click();
  await page.getByRole('button', { name: 'Built by hand' }).click();
  await page.getByLabel('id', { exact: true }).fill(id);
  await page.getByRole('button', { name: 'Add', exact: true }).click();
}

/** The table ids `shared/` holds on `branch`. */
async function tablesOn(api: Api, branch: string): Promise<string[]> {
  const res = await api.get(`/api/areas/shared?branch=${encodeURIComponent(branch)}`);
  return res.config.lookupTables.map((t: Table) => t.id);
}

async function branchNames(api: Api): Promise<string[]> {
  return (await api.get('/api/branches')).branches.map((b: { name: string }) => b.name);
}

test('an edit aimed at main asks for a branch, and saying no saves nothing', async ({
  admin,
  api,
  leader,
}) => {
  // W1
  await switchBranch(admin, 'main');
  await expect(branchChip(admin)).toHaveText(/main\s*· read-only/);

  await addTable(admin, leader, 'colors');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  const prompt = admin.getByRole('dialog', { name: 'branch required' });
  await expect(prompt).toBeVisible();
  // Who is editing and what: the admin, on the lookup tables.
  await expect(prompt.getByLabel('branch to edit on')).toHaveValue('admin-lookups');
  await prompt.getByRole('button', { name: "Don't save" }).click();
  await expect(prompt).toBeHidden();
  await expect(admin.getByText('not saved -- no branch was chosen to save it on')).toBeVisible();
  expect(await branchNames(api)).toEqual([]);
  expect(await tablesOn(api, 'main')).toEqual([]);

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(prompt).toBeVisible();
  await prompt.getByLabel('branch to edit on').fill('add-colors');
  await prompt.getByRole('button', { name: 'Edit on this branch' }).click();
  await expect(prompt).toBeHidden();

  await expect(branchChip(admin)).toHaveAccessibleName('Branch: add-colors');
  await expect(admin.getByText('Not deployed')).toBeVisible();
  await expect(admin.getByText('1 save, not open for review yet')).toBeVisible();
  expect(await tablesOn(api, 'add-colors')).toEqual(['colors']);
  expect(await tablesOn(api, 'main')).toEqual([]);
});

test('the branch bar opens a change from a form filled from what changed', async ({
  admin,
  api,
  leader,
}) => {
  // W2
  await putTables(api, 'add-colors', [table('colors')]);
  await goto(admin, leader, '#/changes');
  await switchBranch(admin, 'add-colors');
  await expect(admin.getByText('Not deployed')).toBeVisible();
  await expect(admin.getByText('1 save, not open for review yet')).toBeVisible();

  await admin.getByRole('button', { name: 'Open for review' }).click();
  const title = admin.getByLabel('Title');
  await expect(title).toBeEnabled();
  await expect(title).not.toHaveValue('');
  await expect(admin.getByText('no tests reach this change')).toBeVisible();
  await expect(admin.getByText(/^1 file ·/)).toBeVisible();
  await expect(admin.getByText('no other administrator')).toBeVisible();

  await title.fill('Add the colors table');
  await admin.getByLabel('Description').fill('Colors for the enrichment step.');
  await admin.getByRole('button', { name: 'Open for review' }).click();

  await expect(admin).toHaveURL(/#\/changes\/review\/add-colors$/);
  await expect(admin.getByRole('heading', { name: 'Add the colors table' })).toBeVisible();
  await expect(admin.getByText('Colors for the enrichment step.')).toBeVisible();
  await expect(admin.getByText(/open for review — 1 file/)).toBeVisible();
  // The author is the only administrator, so there is nobody else's verdict to give here.
  await expect(admin.getByRole('radiogroup', { name: 'Review' })).toHaveCount(0);

  await admin.getByRole('button', { name: 'Rename this change' }).click();
  await admin.getByLabel('change title').fill('Add colors for enrichment');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByRole('heading', { name: 'Add colors for enrichment' })).toBeVisible();
});

test('a reviewer sees the diff, comments, requests changes and approves', async ({
  admin,
  api,
  leader,
}) => {
  // W3
  const rita = await addAccount(leader, api, 'rita', 'Rita Author');
  await arrangeChange(rita, 'rita-colors', 'Add the colors table', [table('colors')]);

  await goto(admin, leader, '#/changes');
  await admin.getByRole('link', { name: /Add the colors table/ }).click();
  await expect(admin.getByRole('heading', { name: 'Add the colors table' })).toBeVisible();
  await expect(
    admin.getByText(/Rita Author\s+wants to deploy 1 save from rita-colors/),
  ).toBeVisible();
  await expect(admin.getByText('1 object')).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Comment on colors' })).toBeVisible();
  await expect(admin.getByText('No reviews yet')).toBeVisible();

  const comment = admin.getByRole('textbox', { name: 'Comment', exact: true });
  await comment.fill('Is red the only key we need?');
  await admin.getByRole('button', { name: 'Submit' }).click();
  await expect(admin.getByText('Is red the only key we need?')).toBeVisible();

  await comment.fill('Add blue before this ships.');
  await admin.getByRole('radio', { name: 'Request changes' }).check();
  await admin.getByRole('button', { name: 'Submit' }).click();
  await expect(admin.getByText('Administrator requested changes', { exact: true })).toBeVisible();
  // A request for changes is shown beside Approve & deploy, and does not block it.
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeEnabled();

  await comment.fill('Fine as it is.');
  await admin.getByRole('radio', { name: 'Approve' }).check();
  await admin.getByRole('button', { name: 'Submit' }).click();
  await expect(admin.getByText('Approved by Administrator', { exact: true })).toBeVisible();
});

test('a change behind main is brought current, and its conflict is the author’s to settle', async ({
  admin,
  api,
  leader,
}) => {
  // W4
  await arrangeChange(api, 'base', 'Seed colors', [table('colors', '0')]);
  await api.deploy('base');
  await arrangeChange(api, 'mine', 'Red is one', [table('colors', '1')]);
  await arrangeChange(api, 'theirs', 'Red is two', [table('colors', '2')]);
  await api.deploy('theirs');

  await goto(admin, leader, '#/changes/review/mine');
  await expect(admin.getByRole('heading', { name: 'Red is one' })).toBeVisible();
  await expect(admin.getByText('Behind main by 1 commit', { exact: true })).toBeVisible();
  await expect(
    admin.getByText('Behind main by 1 commit — bring the branch current first.'),
  ).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeDisabled();

  await admin.getByRole('button', { name: 'Bring current' }).click();
  await expect(admin.getByRole('heading', { name: 'Conflicts' })).toBeVisible();
  await expect(admin.getByRole('heading', { name: 'shared · Lookup table colors' })).toBeVisible();
  const version = (side: string) => admin.getByRole('group', { name: side, exact: true });
  await expect(version('Base')).toContainText('"0"');
  await expect(version('Main')).toContainText('"2"');
  await expect(version('This branch')).toContainText('"1"');
  await admin.getByLabel('Keep').selectOption('work');
  await admin.getByRole('button', { name: 'Apply' }).click();

  await expect(admin.getByText('Current with main', { exact: true })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeEnabled();
  const mine = (await api.get('/api/areas/shared?branch=mine')).config.lookupTables;
  expect(mine[0].rows).toEqual([['red', '1']]);
});

test('approving deploys the change as one commit on main and deletes its branch', async ({
  admin,
  api,
  leader,
}) => {
  // W5
  await arrangeChange(api, 'ship-it', 'Add the colors table', [table('colors')]);

  await goto(admin, leader, '#/changes/review/ship-it');
  await admin.getByRole('button', { name: 'Approve & deploy' }).click();
  const confirm = admin.getByRole('dialog', { name: 'approve & deploy' });
  await expect(confirm).toContainText('deletes the branch ship-it');
  await confirm.getByRole('button', { name: 'Approve & deploy', exact: true }).click();

  await expect(admin).not.toHaveURL(/#\/changes\/review/);
  expect(await branchNames(api)).toEqual([]);
  expect(await tablesOn(api, 'main')).toEqual(['colors']);

  await goto(admin, leader, '#/changes/history');
  const row = historyRow(admin, 'Add the colors table');
  await expect(row).toContainText('running now');
  await expect(row.getByRole('cell', { name: 'Author: Administrator', exact: true })).toBeVisible();
  await expect(
    row.getByRole('cell', { name: 'Deployed by: Administrator', exact: true }),
  ).toBeVisible();
});

test('closing a change with a reason keeps its branch', async ({ admin, api, leader }) => {
  // W6
  await arrangeChange(api, 'not-yet', 'Add the colors table', [table('colors')]);

  await goto(admin, leader, '#/changes/review/not-yet');
  await admin
    .getByRole('textbox', { name: 'Comment', exact: true })
    .fill('Waiting on the blue key.');
  await admin.getByRole('button', { name: 'Close change' }).click();

  await expect(admin.getByText('Closed', { exact: true }).first()).toBeVisible();
  await expect(admin.getByText('Waiting on the blue key.').first()).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Reopen change' })).toBeVisible();
  await expect(admin.getByRole('button', { name: 'Approve & deploy' })).toBeDisabled();

  await goto(admin, leader, '#/changes');
  const row = admin.getByRole('row').filter({ hasText: 'not-yet' });
  await expect(row).toContainText('Closed');
  expect(await tablesOn(api, 'not-yet')).toEqual(['colors']);
});

test('closing needs no reason', async ({ admin, api, leader }) => {
  // W6
  await arrangeChange(api, 'mine-alone', 'Add the colors table', [table('colors')]);

  await goto(admin, leader, '#/changes/review/mine-alone');
  await admin.getByRole('button', { name: 'Close change' }).click();
  await expect(admin.getByText(/Closed without a reason/).first()).toBeVisible();
  expect(await branchNames(api)).toEqual(['mine-alone']);
});

test('History rolls the fleet back to an earlier commit and holds it there', async ({
  admin,
  api,
  leader,
}) => {
  // W7
  await arrangeChange(api, 'ship-it', 'Add the colors table', [table('colors')]);
  await api.deploy('ship-it');

  await goto(admin, leader, '#/changes/history');
  await expect(historyRow(admin, 'Add the colors table')).toContainText('running now');
  const earlier = historyRow(admin, 'worker group default');
  await earlier.getByRole('button', { name: 'Roll back to here' }).click();
  const confirm = admin.getByRole('dialog', { name: 'deploy' });
  await confirm.getByRole('button', { name: 'Deploy' }).click();
  await expect(admin).toHaveURL(/#\/status\?deploying=/);

  await goto(admin, leader, '#/changes/history');
  await expect(earlier).toContainText('running now');
  await expect(earlier).toContainText('held');
  await expect(
    admin.getByText(/The fleet is held at \w+,\s+deployed there by\s+Administrator/),
  ).toBeVisible();
  await expect(
    historyRow(admin, 'Add the colors table').getByRole('button', { name: 'Deploy this' }),
  ).toBeVisible();

  await admin.getByRole('button', { name: 'Release' }).click();
  await admin
    .getByRole('dialog', { name: 'release' })
    .getByRole('button', { name: 'Release' })
    .click();
  await expect(admin).toHaveURL(/#\/status\?deploying=/);
  await goto(admin, leader, '#/changes/history');
  await expect(historyRow(admin, 'Add the colors table')).toContainText('running now');
  await expect(admin.getByText(/The fleet is held at/)).toHaveCount(0);
});

test('main is read-only and History says which commit is deployed and how', async ({
  admin,
  api,
  leader,
}) => {
  // W8
  await arrangeChange(api, 'ship-it', 'Add the colors table', [table('colors')]);
  await api.deploy('ship-it');

  await goto(admin, leader, '#/changes/history');
  await switchBranch(admin, 'main');
  await expect(branchChip(admin)).toHaveText(/main\s*· read-only/);
  await expect(admin.getByText('Not deployed')).toHaveCount(0);

  await branchChip(admin).click();
  await expect(admin.getByText('what the fleet runs — read-only')).toBeVisible();
  await branchChip(admin).click();

  const deployed = historyRow(admin, 'Add the colors table');
  await expect(deployed).toContainText('running now');
  await expect(
    deployed.getByRole('cell', { name: 'Deployed by: Administrator', exact: true }),
  ).toBeVisible();
  await expect(historyRow(admin, 'worker group default')).not.toContainText('running now');

  await goto(admin, leader, '#/changes');
  await expect(admin.getByRole('row').filter({ hasText: /^\s*main/ })).toContainText('open here');
});

test('a save onto a branch that moved is refused, showing both versions', async ({
  admin,
  api,
  leader,
}) => {
  // W9
  await putTables(api, 'work-here', [table('colors')]);
  await goto(admin, leader, '#/build/lookups');
  await switchBranch(admin, 'work-here');
  await addTable(admin, leader, 'sizes');

  // Somebody else saves onto the same branch while this edit is on screen.
  await putTables(api, 'work-here', [table('colors'), table('shapes')]);

  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(admin.getByText('This branch moved while you were editing.')).toBeVisible();
  await expect(
    admin.getByText('Your edit was not saved and is only on this screen.'),
  ).toBeVisible();
  await expect(admin.getByText('What they committed')).toBeVisible();
  await expect(admin.locator('pre').filter({ hasText: 'shapes' })).toBeVisible();
  await expect(admin.getByRole('button', { name: /^sizes/ })).toBeVisible();
  expect(await tablesOn(api, 'work-here')).toEqual(['colors', 'shapes']);
});

test('the branch list shows every branch with its state, and deletes one', async ({
  admin,
  api,
  leader,
}) => {
  // W10
  await putTables(api, 'parked', [table('colors')]);
  await arrangeChange(api, 'in-review', 'Add sizes', [table('sizes')]);
  await arrangeChange(api, 'turned-down', 'Add shapes', [table('shapes')]);
  await api.post('/api/branches/turned-down/close', { reason: '' });

  await goto(admin, leader, '#/changes');
  const row = (name: string) => admin.getByRole('row').filter({ hasText: name });
  await expect(row('parked')).toContainText('Not opened for review');
  await expect(row('in-review')).toContainText('In review');
  await expect(row('turned-down')).toContainText('Closed');
  await expect(row('in-review').getByRole('link', { name: 'Add sizes' })).toBeVisible();
  await expect(row('turned-down').getByRole('link', { name: 'Add shapes' })).toBeVisible();

  await admin.getByPlaceholder('a name for new work').fill('started-here');
  await admin.getByRole('button', { name: 'Start a branch' }).click();
  await expect(row('started-here')).toBeVisible();

  await row('parked').getByRole('button', { name: 'Delete' }).click();
  const confirm = admin.getByRole('dialog', { name: 'delete branch' });
  await expect(confirm).toContainText('It carries 1 commit main does not have.');
  await confirm.getByRole('button', { name: 'Delete' }).click();
  await expect(row('parked')).toHaveCount(0);
  expect(await branchNames(api)).not.toContain('parked');
});

test('a sample the branch deleted is put back from its card on the change page', async ({
  admin,
  api,
  leader,
}) => {
  // W3
  await api.put('/api/areas/shared/samples/keep?branch=seed', {
    id: 'keep',
    name: 'keep',
    records: [{ _raw: 'a line' }],
  });
  // A sample reaches main only with a test that uses it.
  await api.put('/api/areas/shared/pipelines/p?branch=seed', {
    id: 'p',
    name: 'p',
    steps: [{ type: 'drop', id: 'none', condition: 'false' }],
  });
  await api.put('/api/areas/shared/tests/keep?branch=seed', {
    id: 'keep',
    name: 'keep',
    pipeline: 'p',
    sample: 'keep',
    lockedAt: 0,
    rowsIn: 1,
    expected: [null],
  });
  await api.openChange('seed', 'Add a sample');
  await api.deploy('seed');
  await api.delete('/api/areas/shared/samples/keep?branch=tidy');
  await api.openChange('tidy', 'Tidy the samples');

  await goto(admin, leader, '#/changes');
  await admin.getByRole('link', { name: /Tidy the samples/ }).click();
  await expect(admin.getByText('sample · shared')).toBeVisible();
  await admin.getByRole('button', { name: 'Undo changes' }).click();
  await admin
    .getByRole('dialog', { name: 'undo changes' })
    .getByRole('button', { name: 'Undo changes' })
    .click();
  await expect(admin.getByText('No objects changed.')).toBeVisible();
  const { sample } = await api.get('/api/areas/shared/samples/keep?branch=tidy');
  expect(sample.records).toEqual([{ _raw: 'a line' }]);
});

test('a deployed commit that fails its own tests is deployed, and flagged on the Issues page', async ({
  admin,
  api,
  leader,
}) => {
  // W11
  const on = '?branch=broken-orders';
  await api.put(`/api/areas/shared/pipelines/orders${on}`, {
    id: 'orders',
    name: 'orders',
    steps: [
      {
        type: 'parser',
        id: 'body',
        input: '_raw',
        kind: { type: 'json' },
        declared: [{ name: 'id', type: 'i64' }],
      },
    ],
  });
  await api.put(`/api/areas/shared/samples/orders-sample${on}`, {
    id: 'orders-sample',
    name: 'orders',
    records: [{ _raw: '{"id":1}' }],
  });
  // Locks an output the pipeline does not make, so the test fails at every commit that carries it.
  await api.put(`/api/areas/shared/tests/orders-keep-id${on}`, {
    id: 'orders-keep-id',
    name: 'orders keep their id',
    pipeline: 'orders',
    sample: 'orders-sample',
    lockedAt: 0,
    rowsIn: 1,
    expected: [{ id: 2 }],
  });
  await api.openChange('broken-orders', 'Lock a test that fails');
  await api.deploy('broken-orders');

  await goto(admin, leader, '#/status/issues');
  const failing = admin
    .getByRole('list', { name: 'Test failing', exact: true })
    .getByRole('listitem')
    .filter({ has: admin.getByText('orders', { exact: true }) });
  await expect(failing).toContainText('a test fails at the deployed commit', { timeout: 30_000 });
  await expect(failing).toContainText('"orders keep their id"');
});

test('History shows who changed what, who deployed it, and when', async ({
  admin,
  api,
  leader,
}) => {
  // S4
  const rita = await addAccount(leader, api, 'rita', 'Rita Author');
  await arrangeChange(rita, 'rita-colors', 'Add the colors table', [table('colors')]);
  await api.deploy('rita-colors');

  await goto(admin, leader, '#/changes/history');
  const row = historyRow(admin, 'Add the colors table');
  await expect(row.getByRole('cell', { name: /lookup-tables/ })).toBeVisible();
  await expect(row.getByRole('cell', { name: 'Author: Rita Author', exact: true })).toBeVisible();
  await expect(
    row.getByRole('cell', { name: 'Deployed by: Administrator', exact: true }),
  ).toBeVisible();
  await expect(row.getByRole('cell', { name: /^\d+[smhd] ago$/ })).toBeVisible();
  await expect(
    historyRow(admin, 'worker group default').getByRole('cell', {
      name: 'Author: smeltr-leader',
      exact: true,
    }),
  ).toBeVisible();
});

test('a review sets apart the samples no test uses, which the deploy leaves out', async ({
  admin,
  api,
  leader,
}) => {
  const on = '?branch=scratch';
  await putTables(api, 'scratch', [table('colors')]);
  await api.put(`/api/areas/shared/samples/tried${on}`, {
    id: 'tried',
    name: 'capture 14:20',
    records: [{ _raw: 'a' }],
  });
  await api.openChange('scratch', 'Colors, and a capture');

  await goto(admin, leader, '#/changes/review/scratch');
  const staying = admin.locator('.left-out');
  await expect(staying.getByRole('heading', { name: 'Left out' })).toBeVisible();
  await expect(staying.getByText('capture 14:20', { exact: true })).toBeVisible();
  await expect(staying.getByRole('button', { name: 'Undo changes' })).toHaveCount(0);
  await expect(admin.getByText('1 sample left out', { exact: true })).toBeVisible();

  await api.deploy('scratch');
  const main = await api.get('/api/areas/shared/samples?branch=main');
  expect(main.samples).toEqual([]);
});
