// The Admin pages, as an administrator reaches them: its sections, the Workers table, and a worker started from the
// lines the page hands out.
import { expect, type Fleet, type Proc } from '../fleet';
import type { Locator, Page } from '@playwright/test';

/** One of the Admin tab's pages, reached through the navigation. */
export async function openAdmin(page: Page, section: string) {
  // The top navigation, which comes first; a narrow window's breadcrumbs name Admin too.
  await page
    .getByRole('navigation')
    .first()
    .getByRole('link', { name: 'Admin', exact: true })
    .click();
  await page
    .getByRole('navigation', { name: 'Admin sections' })
    .getByRole('link', { name: section, exact: true })
    .click();
}

/** One of the Workers page's tabs. */
export async function openWorkersTab(page: Page, tab: 'Workers' | 'Enrollment codes') {
  await page
    .getByRole('navigation', { name: 'Workers', exact: true })
    .getByRole('link', { name: tab, exact: true })
    .click();
}

/** The Workers table's row for `worker`, by the cell that names it: the enrollment code that brought it in
 * names it too. */
export function workerRowOf(page: Page, worker: string): Locator {
  return page
    .getByRole('row')
    .filter({ has: page.getByRole('cell', { name: worker, exact: true }) });
}

/** The Workers table's row for `worker`, once it shows `state`. The Admin pages read the roster once, so
 * this reloads until it shows, as a person would. */
export async function workerRow(page: Page, worker: string, state: string): Promise<Locator> {
  const row = workerRowOf(page, worker);
  await expect(async () => {
    await page.reload();
    await expect(row.getByRole('cell', { name: state, exact: true })).toBeVisible({
      timeout: 2_000,
    });
  }).toPass({ timeout: 60_000 });
  return row;
}

/** A worker started from the lines the Workers page hands out once an enrollment code is minted. */
export function startWorkerFromLines(fleet: Fleet, name: string, lines: string): Proc {
  const settings: Record<string, string> = {};
  for (const line of lines.split('\n')) {
    const at = line.indexOf('=');
    if (at > 0) settings[line.slice(0, at).trim()] = line.slice(at + 1).trim();
  }
  return fleet.startWorkerWith(name, settings);
}

/** The Cluster page's detail panel for `worker`, once it heartbeats. */
export async function workerDetail(page: Page, worker: string): Promise<Locator> {
  await page.getByRole('link', { name: 'Status', exact: true }).first().click();
  await expect(page.getByRole('heading', { name: 'Fleet status' })).toBeVisible();
  const row = page.getByRole('button', { name: new RegExp(`^${worker}\\b`) });
  await expect(row).toBeVisible({ timeout: 60_000 });
  await row.click();
  const detail = page.getByRole('complementary').filter({
    has: page.getByRole('heading', { name: worker }),
  });
  await expect(detail).toBeVisible();
  return detail;
}

/** The value a `<dt>` names in `detail`. */
export function described(detail: Locator, term: string): Locator {
  return detail.locator('dt', { hasText: term }).locator('xpath=following-sibling::dd[1]');
}

/** Opens the row menu `name` names. Scrolled to first, and drawn once there: a row menu closes on any scroll,
 * and the scroll that brings it into view lands a frame after it. */
export async function openRowMenu(page: Page, name: string | RegExp) {
  const trigger = page.getByRole('button', { name, exact: true });
  await trigger.scrollIntoViewIfNeeded();
  await page.evaluate(
    () => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))),
  );
  await trigger.click();
}
