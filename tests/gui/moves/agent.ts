// The Agent page, as the person signed in reaches it from the agent panel.
import { expect } from '../fleet';
import type { Locator, Page } from '@playwright/test';
import type { Rng } from '../fuzz';

/** The Agent page with no conversation open, through the agent panel's link unless the page is already open. */
export async function openAgent(page: Page) {
  const heading = page.getByRole('heading', { name: 'Agent', exact: true });
  const chip = page.getByRole('button', { name: 'Agent panel', exact: true });
  await expect(heading.or(chip)).toBeVisible();
  if (await chip.isVisible()) {
    if ((await chip.getAttribute('aria-pressed')) !== 'true') await chip.click();
    await page
      .getByRole('complementary', { name: 'Agent' })
      .getByRole('link', { name: 'Open on the Agent page' })
      .click();
    await expect(heading).toBeVisible();
  }
  // The panel's link opens the conversation it shows.
  await page.getByRole('link', { name: 'New conversation' }).click();
  await expect(page).toHaveURL(/#\/me\/agent$/);
}

/** The conversation `title` in the page's list. */
export function conversationLink(page: Page, title: string): Locator {
  return page
    .getByRole('navigation', { name: 'Conversations' })
    .getByRole('link', { name: new RegExp(`^${title}\\b`) });
}

/** Each effort as the model chip's menu offers it, and as the leader sends it. */
const EFFORTS: [string, string][] = [
  ['Low', 'low'],
  ['Medium', 'medium'],
  ['High', 'high'],
  ['Extra high', 'xhigh'],
  ['Max', 'max'],
];

/** Pick a model and an effort from the chip under the message box: the newest Opus, or the older one folded under
 * More models. What the leader should send for them. */
export async function pickModel(page: Page, rng: Rng): Promise<{ model: string; effort: string }> {
  const chip = page.getByRole('button', { name: 'Model and effort' });
  const older = rng.chance(2);
  await chip.click();
  // A model already picked is listed with the newest, so More models is there only while Opus 5 is not picked.
  const more = page.getByRole('button', { name: /^More models/ });
  await expect(page.getByRole('button', { name: /^Effort/ })).toBeVisible();
  if (older && (await more.count())) await more.click();
  await page.getByRole('button', { name: older ? /^Opus 5$/ : /^Opus 5\.5/ }).click();
  const [label, effort] = rng.pick(EFFORTS);
  await chip.click();
  await page.getByRole('button', { name: /^Effort/ }).click();
  await page.getByRole('button', { name: new RegExp(`^${label}`) }).click();
  await expect(chip).toContainText(`${label} effort`);
  return { model: older ? 'claude-opus-5' : 'claude-opus-5-5', effort };
}
