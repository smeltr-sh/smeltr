// The Certificates page under System, as an administrator changes where the leader's certificate comes from. A
// change of source is published to the fleet before it is served, and the page marks the source in force once
// every worker holds the incoming authority.
import type { Locator, Page } from '@playwright/test';
import type { Authority } from '../acme';
import { expect } from '../fleet';
import { openAdmin } from './admin';

export type Pki = { certificate: string; privateKey: string; authority: string };

/** What the Certificate field takes: the certificate, then the root that issued it. */
export function fullChain(pki: Pki): string {
  return pki.certificate.trimEnd() + '\n' + pki.authority;
}

/** The Leader certificate card, with the page open. */
export async function openCertificates(page: Page): Promise<Locator> {
  await openAdmin(page, 'System');
  await page
    .getByRole('navigation', { name: 'System settings' })
    .getByRole('link', { name: 'Leader certificate', exact: true })
    .click();
  return page.getByRole('region', { name: /^Leader certificate/ });
}

/** The Source chooser's option `label`. */
export function source(served: Locator, label: string): Locator {
  return served.getByRole('group', { name: 'Source' }).getByRole('button', { name: label });
}

/** Waits until the chooser marks `label` as the source in force: the switch has cut over. A fleet takes it on its
 * next heartbeats; a leader with nobody to wait for takes it on the page's next read. */
export async function inForce(served: Locator, label: string) {
  await expect(source(served, label)).toHaveAttribute('aria-current', 'true', { timeout: 60_000 });
  await expect(served.getByText(/in progress|new root/)).toHaveCount(0);
}

/** Waits for the page to say the switch just opened is being published, under `heading`, with every worker
 * waited for: the worker inside the leader follows its disk, so a switch waits for it like any other. */
async function publishing(served: Locator, heading: RegExp) {
  await expect(served.getByRole('status')).toHaveText(
    'the incoming authority is being published to the fleet',
    { timeout: 30_000 },
  );
  await expect(served.getByRole('heading', { name: heading })).toBeVisible();
  await expect(served.getByText(/Not waited for/)).toHaveCount(0);
}

let uploads = 0;

/** Adds `pki` to the certificate list through the chooser's add link, which comes back to the Leader certificate
 * card, and picks it there. Returns the card. */
export async function chooseUploaded(page: Page, pki: Pki): Promise<Locator> {
  const name = `uploaded-${++uploads}`;
  let served = await openCertificates(page);
  await source(served, 'Uploaded').click();
  await served.getByRole('link', { name: 'add a certificate' }).click();
  const dialog = page.getByRole('dialog', { name: 'new certificate' });
  await dialog.getByRole('textbox', { name: 'Name' }).fill(name);
  await dialog.getByRole('textbox', { name: 'Certificate', exact: true }).fill(fullChain(pki));
  await dialog.getByRole('textbox', { name: 'Private key' }).fill(pki.privateKey);
  await dialog.getByRole('button', { name: 'Add', exact: true }).click();
  served = page.getByRole('region', { name: /^Leader certificate/ });
  await source(served, 'Uploaded').click();
  await served.getByRole('combobox', { name: 'Certificate' }).selectOption(name);
  return served;
}

/** Serves `pki` in place of the leader's own, and waits until it is in force. */
export async function switchToUploaded(page: Page, pki: Pki) {
  const served = await chooseUploaded(page, pki);
  await served.getByRole('button', { name: 'Serve this certificate' }).click();
  await publishing(served, /^Switch to Uploaded in progress/);
  await inForce(served, 'Uploaded');
}

/** Replaces the certificate the leader serves with `pki`, on the Certificates page, which serves it too. */
export async function replaceServed(page: Page, pki: Pki) {
  await openAdmin(page, 'Certificates');
  const row = page.getByRole('row').filter({ has: page.getByText('leader', { exact: true }) });
  await row.getByRole('button', { name: 'Replace' }).click();
  const dialog = page.getByRole('dialog', { name: /^replace / });
  await dialog.getByRole('textbox', { name: 'Certificate', exact: true }).fill(fullChain(pki));
  await dialog.getByRole('textbox', { name: 'Private key' }).fill(pki.privateKey);
  await dialog.getByRole('button', { name: 'Replace', exact: true }).click();
  await expect(dialog).toHaveCount(0);
}

/** Goes back to the leader's own certificate, and waits until it is in force. */
export async function switchToLeaderIssued(page: Page) {
  const served = await openCertificates(page);
  await source(served, 'Leader issued').click();
  await served.getByRole('button', { name: 'Switch to Leader issued' }).click();
  await publishing(served, /^Switch to Leader issued in progress/);
  await inForce(served, 'Leader issued');
}

/** What the page says once an order is served at once, under the root in force. */
const ISSUED = 'this leader is serving the certificate the authority issued';

/** The ACME form's Advanced section, open. */
async function advanced(served: Locator): Promise<Locator> {
  const toggle = served.getByRole('button', { name: /^Advanced/ });
  if ((await toggle.getAttribute('aria-expanded')) !== 'true') await toggle.click();
  return served;
}

/** Points the ACME form at `authority` for `names`, trusting every root it has minted. */
async function fillAcme(served: Locator, authority: Authority, names: string[]) {
  await served.getByRole('combobox', { name: 'Directory' }).selectOption('other');
  await served.getByRole('textbox', { name: 'Directory URL' }).fill(authority.directory);
  const chips = served.getByRole('group', { name: 'Names' });
  for (const chip of await chips.getByRole('button', { name: /^remove / }).all())
    await chip.click();
  for (const name of names) {
    await chips.getByRole('textbox').fill(name);
    await chips.getByRole('textbox').press('Enter');
  }
  await served.getByRole('textbox', { name: 'Contact email' }).fill('ops@example.com');
  await advanced(served);
  await served
    .getByRole('textbox', { name: 'Authority', exact: true })
    .fill(authority.roots.join(''));
}

/** Orders a certificate for `names` from `authority`, and waits until it is in force. The first order is under a
 * root the fleet does not hold, so it is a switch like an upload's. */
export async function switchToAcme(page: Page, authority: Authority, names: string[]) {
  const served = await openCertificates(page);
  await source(served, 'ACME').click();
  await fillAcme(served, authority, names);
  await served.getByRole('checkbox', { name: /^I agree/ }).check();
  await served.getByRole('button', { name: 'Get certificate' }).click();
  await publishing(served, /^Switch to ACME in progress/);
  await inForce(served, 'ACME');
}

/** Renews the ACME certificate in force over `challenge`: the same root, so it is served at once. */
export async function renewAcme(page: Page, challenge: 'TLS-ALPN-01' | 'HTTP-01') {
  const served = await openCertificates(page);
  await source(served, 'ACME').click();
  await advanced(served);
  await served.getByRole('combobox', { name: 'Challenge' }).selectOption({ label: challenge });
  await served.getByRole('button', { name: 'Save' }).click();
  await expect(served.getByRole('status')).toHaveText('settings saved');
  await served.getByRole('button', { name: 'Renew now' }).click();
  await expect(served.getByRole('status')).toHaveText(ISSUED, { timeout: 30_000 });
  await expect(served.getByText(/in progress|new root/)).toHaveCount(0);
}

/** Tries to save DNS-01 through Cloudflare with `token` for names that include an IP address, which DNS-01 cannot
 * prove: the save is refused, saying which, and the challenge is put back. */
export async function refuseDns01(page: Page, token: string, ip: string) {
  const served = await openCertificates(page);
  await source(served, 'ACME').click();
  await advanced(served);
  const challenge = served.getByRole('combobox', { name: 'Challenge' });
  const was = await challenge.inputValue();
  await challenge.selectOption({ label: 'DNS-01' });
  await served
    .getByRole('combobox', { name: 'DNS provider' })
    .selectOption({ label: 'Cloudflare' });
  await served.getByLabel('API token', { exact: true }).fill(token);
  await served.getByRole('button', { name: 'Save' }).click();
  await expect(page.getByText(`DNS-01 cannot prove an IP address: ${ip}`)).toBeVisible();
  await challenge.selectOption(was);
  // What was typed goes with the page, so no later save carries it.
  await page.reload();
}

/** Has `authority` issue under a new root, tells the leader to trust it, and renews: the fleet is moved to the
 * new root before it is served under. */
export async function acmeNewRoot(page: Page, authority: Authority) {
  authority.rotateRoot();
  const served = await openCertificates(page);
  await source(served, 'ACME').click();
  await advanced(served);
  await served
    .getByRole('textbox', { name: 'Authority', exact: true })
    .fill(authority.roots.join(''));
  await served.getByRole('button', { name: 'Renew now' }).click();
  await publishing(served, /^Moving workers to a new root/);
  await inForce(served, 'ACME');
}
