// What a person sees when the leader refuses or fails: the error bar under the nav, a link from it to the step or
// field that holds an error, and the popup a failed deploy opens first (`docs/gui-design.md`, "Errors").
import { test, expect, goto } from '../fleet';
import type { Page } from '@playwright/test';

/** The error bar, which shows what every call refused or failed. */
function errorBar(page: Page) {
  return page.locator('.error-bar');
}

test('a refused call stays in the error bar until the same call succeeds', async ({
  admin,
  api,
  leader,
}) => {
  await api.put('/api/lookup-tables?branch=parked&scope=shared', [
    { id: 'colors', type: 'inline', headers: ['key', 'value'], rows: [['red', '1']] },
  ]);

  await goto(admin, leader, '#/changes');
  const name = admin.getByPlaceholder('a name for new work');
  await name.fill('parked');
  await admin.getByRole('button', { name: 'Start a branch' }).click();
  await expect(errorBar(admin)).toContainText('branch "parked" is already there');

  // Scrolled away, the bar is still at the top of the window.
  await admin.mouse.wheel(0, 2000);
  await expect(errorBar(admin)).toBeInViewport();

  await name.fill('started-here');
  await admin.getByRole('button', { name: 'Start a branch' }).click();
  await expect(admin.getByRole('row').filter({ hasText: 'started-here' })).toBeVisible();
  await expect(errorBar(admin)).toBeHidden();
});

test('a refused save links from the error bar to the step that holds its error', async ({
  admin,
  api,
  leader,
}) => {
  await api.put('/api/pipelines/orders?branch=work&scope=shared', {
    id: 'orders',
    name: 'orders',
    steps: [{ type: 'code', id: 'code-1', filter: 'true', disabled: false, body: 'x = 1;' }],
  });

  await goto(admin, leader, '#/build/pipelines/orders');
  await admin.getByRole('button', { name: /line\(s\) of code/ }).click();
  await admin.locator('textarea').last().fill('x = ipVersion();');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();

  const link = errorBar(admin).getByRole('link', { name: /line\(s\) of code/ });
  await expect(link).toBeVisible();
  await expect(admin.getByRole('dialog')).toHaveCount(0);
  await admin.mouse.wheel(0, 2000);
  await link.click();
  await expect(admin.locator('#step-code-1')).toBeInViewport();
});

test('a failed deploy opens a popup, which stays in the error bar once dismissed', async ({
  admin,
  api,
  leader,
}) => {
  await api.put('/api/lookup-tables?branch=ship-it&scope=shared', [
    { id: 'colors', type: 'inline', headers: ['key', 'value'], rows: [['red', '1']] },
  ]);
  await api.openChange('ship-it', 'Add the colors table');
  await admin.route('**/api/changes/ship-it/deploy', (r) =>
    r.fulfill({
      json: {
        ok: false,
        errors: [
          {
            pipeline: null,
            step: null,
            function: null,
            assignmentIndex: null,
            item: null,
            field: null,
            rule: null,
            message: 'the store could not be written',
          },
        ],
      },
    }),
  );

  await goto(admin, leader, '#/changes/review/ship-it');
  await admin.getByRole('button', { name: 'Approve & deploy' }).click();
  await admin
    .getByRole('dialog', { name: 'approve & deploy' })
    .getByRole('button', { name: 'Approve & deploy', exact: true })
    .click();

  const popup = admin.getByRole('dialog', { name: 'not deployed' });
  await expect(popup).toContainText('the store could not be written');
  await popup.getByRole('button', { name: 'Close' }).click();
  await expect(popup).toBeHidden();
  await expect(errorBar(admin)).toContainText('the store could not be written');
});

test('a refused route is marked on its condition, which the error bar opens its card to', async ({
  admin,
  api,
  leader,
}) => {
  const on = '?branch=work&scope=shared';
  await api.put(`/api/sources${on}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/p${on}`, { id: 'p', name: 'p', steps: [] });
  await api.put(`/api/destinations${on}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
  ]);
  await api.put(`/api/routes${on}`, [
    { id: 'r1', source: 'm1', condition: 'true', pipeline: 'p', deliver: [{ to: ['es-a'] }] },
  ]);

  await goto(admin, leader, '#/build/routes');
  const head = admin.getByRole('button', { name: /^m1/ }).first();
  await head.click();
  const card = admin.locator('.collapsible', { has: head });
  const condition = admin.getByRole('textbox', { name: 'takes rows where', exact: true });
  await condition.fill('ipVersion()');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();

  await expect(condition).toHaveAttribute('aria-invalid', 'true');
  await expect(card.locator(':scope > .head')).toContainText('1 error');
  const link = errorBar(admin).getByRole('link', { name: /r1/ });
  await expect(link).toContainText('ipVersion');

  // Closed, the card opens to the field the link names, in a window short enough that the field opens low.
  await admin.setViewportSize({ width: 1280, height: 480 });
  await head.click();
  await expect(condition).toBeHidden();
  await link.click();
  await expect(condition).toBeFocused();
  // Not under the save bar.
  const saveBar = await admin.locator('.save-bar').boundingBox();
  const field = await condition.boundingBox();
  expect(field!.y + field!.height).toBeLessThanOrEqual(saveBar!.y);

  await condition.fill('true');
  await expect(condition).toHaveAttribute('aria-invalid', 'false');
  await expect(card.locator(':scope > .head')).not.toContainText('error');
});

test('a refused delivery condition keeps its mark on its own rule as the rules change', async ({
  admin,
  api,
  leader,
}) => {
  const on = '?branch=work&scope=shared';
  await api.put(`/api/sources${on}`, [{ id: 'm1', type: 'metrics', bucketSeconds: 10 }]);
  await api.put(`/api/pipelines/p${on}`, { id: 'p', name: 'p', steps: [] });
  await api.put(`/api/destinations${on}`, [
    { id: 'es-a', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'a' },
    { id: 'es-b', type: 'elasticsearch', url: 'http://127.0.0.1:9', defaultIndex: 'b' },
  ]);
  await api.put(`/api/routes${on}`, [
    {
      id: 'r1',
      source: 'm1',
      condition: null,
      pipeline: 'p',
      deliver: [{ when: 'true', to: ['es-a'] }, { to: ['es-b'] }],
    },
  ]);

  await goto(admin, leader, '#/build/routes');
  await admin.getByRole('button', { name: /^m1/ }).first().click();
  const card = admin.locator('.flow[data-source="m1"]');
  const when = card.getByRole('textbox', { name: 'sends rows where', exact: true });
  await card.getByRole('button', { name: 'add a condition' }).last().click();
  await admin.keyboard.type('ipVersion()');
  await admin.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(when.last()).toHaveAttribute('aria-invalid', 'true');

  // es-a moves past it, and its rule comes first.
  await card.locator('.pill', { hasText: 'es-a' }).getByRole('button', { name: 'more' }).click();
  await admin
    .locator('.menu')
    .getByRole('button', { name: /move right/ })
    .click();
  await expect(card.locator('.pill .name')).toHaveText(['es-b', 'es-a']);
  await expect(when.first()).toHaveValue('ipVersion()');
  await expect(when.first()).toHaveAttribute('aria-invalid', 'true');
  await expect(when.last()).toHaveAttribute('aria-invalid', 'false');
});
