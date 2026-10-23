// W12: from a fresh install to rows flowing, all of it through the GUI. The only things done outside the page are
// standing up a fake Elasticsearch for the rows to land in and starting the worker that sends them.
import { test as base, expect, goto, signInThroughGui, signedIn } from '../fleet';
import { FakeElasticsearch } from '../sinks';

// The sink is stopped after the fleet, so the worker never spends its shutdown failing to reach it.
const test = base.extend<{ es: FakeElasticsearch }>({
  es: async ({}, use) => {
    const es = await FakeElasticsearch.start();
    await use(es);
    await es.stop();
  },
  fleet: async ({ es, fleet }, use) => {
    void es;
    await use(fleet);
    await fleet.stop();
  },
});

/** A rate the GUI prints that is above zero. */
const MOVING = /(0\.\d*[1-9]|[1-9][\d.]*) rows\/s/;

test('W12: a fresh leader is set up, changed, reviewed and deployed from the GUI, and rows flow', async ({
  page,
  leader,
  fleet,
  es,
}) => {
  const password = 'end-to-end-password';
  const saved = () => expect(page.getByText('Unsaved', { exact: true })).toBeHidden();

  // The administrator signs in with the one-time password and chooses their own.
  await page.goto(leader.url);
  await signInThroughGui(page, leader.username, leader.oneTimePassword);
  await page.getByLabel('New password', { exact: true }).fill(password);
  await page.getByLabel('Confirm new password').fill(password);
  await page.getByRole('button', { name: 'Change password' }).click();
  await expect(page.getByRole('status')).toHaveText(
    'password changed -- sign in with your new one',
  );
  await signInThroughGui(page, leader.username, password);
  await expect(page.getByRole('navigation').first()).toBeVisible();

  // A source: the worker's own metrics, every second.
  await goto(page, leader, '#/build/sources');
  await page.getByRole('button', { name: '+ Add source' }).click();
  await page.getByRole('button', { name: /^Local, each worker/ }).click();
  await page.getByRole('button', { name: 'smeltr metrics', exact: true }).click();
  await page.getByRole('textbox', { name: /^id/ }).fill('own-metrics');
  await page.getByRole('spinbutton', { name: /^bucket seconds/ }).fill('1');
  await page.getByRole('button', { name: 'Add', exact: true }).click();
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  // A new browser opens on main, so the first save asks for a branch to edit on.
  const prompt = page.getByRole('dialog', { name: 'branch required' });
  await prompt.getByLabel('branch to edit on').fill('metrics-into-search');
  await prompt.getByRole('button', { name: 'Edit on this branch' }).click();
  await saved();

  // A destination: the fake Elasticsearch.
  await goto(page, leader, '#/build/destinations');
  await page.getByRole('button', { name: '+ Add destination' }).click();
  await page.getByRole('button', { name: 'Elasticsearch', exact: true }).click();
  await page.getByRole('textbox', { name: /^id/ }).fill('search');
  await page.getByRole('textbox', { name: 'url' }).fill(es.url);
  await page.getByRole('textbox', { name: 'default index' }).fill('metrics');
  await page.getByRole('button', { name: 'Add', exact: true }).click();
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await saved();

  // A pipeline that passes rows through as they are.
  await goto(page, leader, '#/build/pipelines');
  await page.getByRole('link', { name: '+ Add pipeline' }).click();
  await page.getByLabel('pipeline name').fill('pass through');
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(page.getByText('Saved.')).toBeVisible();

  // A route from the source, through the pipeline, to the destination.
  await goto(page, leader, '#/build/routes');
  await page.getByRole('button', { name: '+ Add route' }).click();
  await expect(page.getByRole('combobox', { name: 'from source' })).toHaveValue('own-metrics');
  await page.getByRole('combobox', { name: 'pipeline' }).selectOption({ label: 'pass through' });
  await page.getByRole('button', { name: 'Save', exact: true }).click();
  await saved();

  // The change is opened, commented on, approved and deployed.
  await page.getByRole('button', { name: 'Open for review' }).click();
  await page.getByLabel('Title').fill('Metrics into search');
  await page.getByRole('button', { name: 'Open for review' }).click();
  await expect(page.getByRole('heading', { name: 'Metrics into search' })).toBeVisible();
  await page
    .getByRole('textbox', { name: 'Comment', exact: true })
    .fill('Every worker reports itself into search.');
  await page.getByRole('button', { name: 'Submit' }).click();
  await expect(page.getByText('Every worker reports itself into search.')).toBeVisible();
  await page.getByRole('button', { name: 'Approve & deploy' }).click();
  const confirm = page.getByRole('dialog', { name: 'approve & deploy' });
  await confirm.getByRole('button', { name: 'Approve & deploy', exact: true }).click();
  await expect(page).not.toHaveURL(/#\/changes\/review/);

  // A worker joins the group, and rows reach the destination.
  const api = await signedIn(leader, leader.username, password);
  await fleet.startWorker(leader, api, 'default', 'w1');
  await api.ctx.dispose();

  await goto(page, leader, '#/map');
  const band = page.getByRole('region', { name: 'default', exact: true });
  await expect(band.getByRole('link', { name: /own-metrics/ })).toContainText(MOVING, {
    timeout: 40_000,
  });
  await expect(band.getByRole('link', { name: /search/ })).toContainText(MOVING, {
    timeout: 40_000,
  });
  await expect.poll(() => es.documents.length, { timeout: 40_000 }).toBeGreaterThan(0);
});
