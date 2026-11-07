// The website demo: the GUI over the leader its page plays (`gui-frontend/src/demo`), with `smeltr-demo` answering
// what only the engine can. A visitor captures from a source, saves what it caught on a branch of their own, and
// previews a pipeline over it, a lookup table included.
import { test, expect, type Page } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import { REPO, startDemo, type Demo } from '../demo';

let demo: Demo;
let base: string;

test.beforeAll(async () => {
  demo = await startDemo();
  base = demo.base;
});

test.afterAll(() => demo?.stop());

async function open(page: Page, hash: string) {
  await page.addInitScript(() => localStorage.setItem('smeltr-scope', 'network'));
  await page.goto(`${base}${hash}`);
}

test('a visitor captures a sample, saves it on a branch and previews a pipeline over it', async ({
  page,
}) => {
  await open(page, '#/build');
  await page.getByRole('button', { name: 'Add sample' }).click();
  const dialog = page.locator('dialog[open]');
  await dialog.locator('button.tile', { hasText: 'Capture' }).click();
  await dialog.getByRole('combobox', { name: 'Worker group', exact: true }).selectOption('network');
  await dialog
    .getByRole('combobox', { name: 'Capture target', exact: true })
    .selectOption('panw-firewalls');
  await dialog
    .getByRole('textbox', { name: 'Keep records where', exact: true })
    .fill('_raw.includes("TRAFFIC")');
  await dialog.locator("input[aria-label='records']").fill('50');
  await dialog.locator('button[type=submit]', { hasText: 'Start' }).click();
  await expect(page.getByText('Captured from panw-firewalls')).toBeVisible();
  await expect(page.getByText('Finished. 50 records')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Use these records' })).toBeDisabled();

  await page.getByRole('button', { name: 'Save sample' }).click();
  // main is never edited, so the save asks for a branch and the demo's leader forks one.
  await page.getByRole('button', { name: 'Edit on this branch' }).click();
  await expect(page.locator('dialog[open]')).toHaveCount(0);
  // The branch the save forked, which the GUI is now on: a recorded branch may hold samples of its own.
  const forked = await page.evaluate(() => localStorage.getItem('smeltr-branch'));
  expect(forked, "the GUI's branch, under the key api.ts keeps it in").toBeTruthy();
  const sample = await page.evaluate((forked) => {
    const state = JSON.parse(sessionStorage.getItem('smeltr-demo')!);
    const branch = (state.branches as Record<string, { touches: string[] }>)[forked];
    return branch.touches
      .find((t) => t.includes('/samples/'))!
      .replace(/.*\/samples\/(.*)\.json$/, '$1');
  }, forked!);

  await page.goto(`${base}#/build/pipelines/panos-traffic/for/${sample}`);
  await expect(page.getByText('panw-firewalls, captured', { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Choose a sample' })).toContainText('50 records');
  // The directory table is a CSV under the service's bundle: a department in the output is a lookup that read it.
  const lookups = path.join(REPO, 'gui-frontend', 'src', 'demo', 'lookups');
  const directory = fs.readdirSync(lookups).find((f) => f.endsWith('directory.csv'))!;
  const departments = new Set(
    fs
      .readFileSync(path.join(lookups, directory), 'utf8')
      .trim()
      .split('\n')
      .slice(1)
      .map((line) => line.split(',')[1]),
  );
  await expect(
    page
      .getByRole('cell')
      .filter({ hasText: new RegExp(`^(${[...departments].join('|')})$`) })
      .first(),
  ).toBeVisible();
});

test('the map draws the fleet from the service', async ({ page }) => {
  await open(page, '#/map');
  // The map draws from what the in-browser service has computed, which a loaded machine takes seconds over.
  await expect(page.getByText('panw-firewalls').first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText('nginx-access').first()).toBeVisible();
  await expect(page.getByText('lookup key column')).toHaveCount(0);
});

test("the samples page runs the scope's locked tests in the service", async ({ page }) => {
  await open(page, '#/build');
  // The page reads the whole recording before it draws, which a loaded machine takes seconds over.
  await expect(page.getByText('panos-traffic maps sessions to ECS')).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByText('Passing')).toHaveCount(2);
});

test('the Schemas page reads a schema through the service', async ({ page }) => {
  await open(page, '#/build/schemas');
  await page.getByRole('button', { name: '+ Add schema' }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: 'JSON Schema', exact: true }).click();
  await dialog.getByLabel('id', { exact: true }).fill('login');
  const login = {
    type: 'object',
    properties: {
      user: { type: 'object', properties: { name: { type: 'string' } } },
      src_endpoint: { type: 'object', properties: { port: { type: 'integer' } } },
    },
  };
  await dialog.getByRole('textbox', { name: 'JSON Schema' }).fill(JSON.stringify(login));
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByRole('cell', { name: 'user.name', exact: true })).toBeVisible();
  await page.getByRole('searchbox', { name: 'Search schemas' }).fill('src_endpoint.port');
  await expect(page.getByRole('cell', { name: 'src_endpoint.port', exact: true })).toBeVisible();
});

test('the guide ticks a task when its work is there', async ({ page }) => {
  await open(page, '#/map');
  const guide = page.getByRole('button', { name: /^Guide · / });
  await expect(guide).toHaveText('Guide · 0/7');
  // Opening Status ticks its task.
  await page.evaluate(() => (location.hash = '#/status'));
  await expect(guide).toHaveText('Guide · 1/7');
  // A reload finds the task still done.
  await page.reload();
  await expect(guide).toHaveText('Guide · 1/7');
});

test('starting and opening a branch ticks the first task', async ({ page }) => {
  await open(page, '#/changes');
  const guide = page.getByRole('button', { name: /^Guide · / });
  await expect(guide).toHaveText('Guide · 0/7');
  await page.getByPlaceholder('a name for new work').fill('fix-network');
  await page.getByRole('button', { name: 'Start a branch' }).click();
  await page
    .getByRole('row', { name: /fix-network/ })
    .getByRole('button', { name: 'Open' })
    .click();
  await expect(guide).toHaveText('Guide · 1/7');
});

test("the guide's FortiGate task points at the parser that declares proto", async ({ page }) => {
  await open(page, '#/map');
  await page
    .locator('#demo-guide li', { hasText: 'Fix the proto field' })
    .getByText('Show me')
    .click();
  // The pipeline parses `_raw` with Regex/Grok first; proto is the key/value parser's.
  await expect(page.locator('[data-guide="parser"][data-guide-ring]')).toHaveAttribute(
    'data-parser',
    'keyValue',
  );
});

test('the guide folds on a click, but not on a click on its links', async ({ page }) => {
  await open(page, '#/map');
  const guide = page.getByRole('button', { name: /^Guide · / });
  await expect(guide).toHaveText('Guide · 0/7');

  // A click on the guide's text folds it.
  await page.locator('#demo-guide p').click();
  await expect(page.locator('#demo-guide')).toBeHidden();
  await guide.click();
  await expect(page.locator('#demo-guide')).toBeVisible();
  // Its links follow, and leave it open.
  await page.locator('#demo-guide').getByRole('link', { name: 'Show me' }).first().click();
  await expect(page).toHaveURL(/#\/changes/);
  await expect(page.locator('#demo-guide')).toBeVisible();
});
