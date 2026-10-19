// The Agent page, as the person signed in reaches it from their name.
import { expect } from '../fleet';
import type { Locator, Page } from '@playwright/test';
import type { Rng } from '../fuzz';

/** The Agent page, through the menu under the person's name. */
export async function openAgent(page: Page) {
  await page.getByRole('button', { name: /^signed in as / }).click();
  await page.getByRole('button', { name: 'Agent', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Agent', exact: true })).toBeVisible();
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
  if (older) await page.getByRole('button', { name: /^More models/ }).click();
  await page.getByRole('button', { name: older ? /^Opus 5$/ : /^Opus 5\.5/ }).click();
  const [label, effort] = rng.pick(EFFORTS);
  await chip.click();
  await page.getByRole('button', { name: /^Effort/ }).click();
  await page.getByRole('button', { name: new RegExp(`^${label}`) }).click();
  await expect(chip).toContainText(`${label} effort`);
  return { model: older ? 'claude-opus-5' : 'claude-opus-5-5', effort };
}
