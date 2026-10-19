// The sign-in screen shows in place of whatever page the address names: a session that ended there, or a
// bookmark to it. It must work the same on every one of them.
import type { Browser, Page } from '@playwright/test';
import {
  test,
  expect,
  goto,
  settled,
  signInThroughGui,
  watchRequests,
  ADMIN_PASSWORD,
  type Leader,
} from '../fleet';

/** Every page the GUI links to, found by following its links from each tab, with no query. */
async function everyPage(page: Page, leader: Leader): Promise<string[]> {
  watchRequests(page);
  const seen = new Set<string>();
  const queue = ['#/status', '#/map', '#/build', '#/changes', '#/admin'];
  while (queue.length > 0) {
    const hash = queue.shift()!;
    if (seen.has(hash)) continue;
    seen.add(hash);
    await goto(page, leader, hash);
    await expect(page.getByRole('navigation').first()).toBeVisible();
    // A page's links come with its data, so wait for what it asked to arrive.
    await settled(page);
    for (const href of await page
      .locator('a[href^="#/"]')
      .evaluateAll((links) => links.map((a) => a.getAttribute('href')!)))
      queue.push(href.split('?')[0]);
  }
  return [...seen].sort();
}

async function signedOut(browser: Browser): Promise<Page> {
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  return context.newPage();
}

test('the sign-in form takes typing on every page', async ({ admin, leader, browser }) => {
  const pages = await everyPage(admin, leader);
  // An authoring page is one only an editor may change, which nobody signed out is.
  expect(pages).toContain('#/build/sources');

  const page = await signedOut(browser);
  for (const hash of pages) {
    // From another document: a change of hash alone keeps the screen already drawn.
    await page.goto('about:blank');
    await goto(page, leader, hash);
    for (const [label, typed] of [
      ['Username', 'somebody'],
      ['Password', 'something'],
    ]) {
      const field = page.getByLabel(label, { exact: true });
      await expect(field, `${label} on ${hash}`).toBeEnabled();
      await field.click();
      await page.keyboard.type(typed);
      await expect(field, `${label} on ${hash}`).toHaveValue(typed);
      await field.clear();
    }
  }
});

test('signing in on an authoring page lands on it', async ({ api, leader, browser }) => {
  void api;
  const page = await signedOut(browser);
  await goto(page, leader, '#/build/sources');
  await signInThroughGui(page, leader.username, ADMIN_PASSWORD);
  await expect(page.getByRole('button', { name: 'Sign in' })).toHaveCount(0);
  await expect(page).toHaveURL(/#\/build\/sources$/);
});
