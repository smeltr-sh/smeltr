// Where the leader's certificate comes from, changed under a running fleet: a worker beside the leader and the
// one inside its process both follow each switch, because the incoming authority reaches them before anything
// is served under it.
import fs from 'node:fs';
import path from 'node:path';
import type { Page } from '@playwright/test';
import {
  test,
  expect,
  claimAdmin,
  signInThroughGui,
  watchRequests,
  ADMIN_PASSWORD,
  type Fleet,
} from '../fleet';
import { Authority } from '../acme';
import { openAdmin, workerRow } from '../moves/admin';
import {
  acmeNewRoot,
  chooseUploaded,
  inForce,
  openCertificates,
  renewAcme,
  replaceServed,
  source,
  switchToAcme,
  switchToLeaderIssued,
  switchToUploaded,
} from '../moves/certificates';

/** A leader running a worker of its own, which reaches it by a name no certificate may drop, and `name` beside
 * it, with the administrator signed in on `page` and both workers heartbeating. `heard` checks they still are,
 * over handshakes against whatever is served now. */
async function fleetOfTwo(fleet: Fleet, page: Page, name: string) {
  const leader = await fleet.startLeader({ SMELTR_WORKER_ENABLED: '1' });
  const api = await claimAdmin(leader);
  await fleet.startWorker(leader, api, 'default', name);
  watchRequests(page);
  await page.goto(leader.url);
  await signInThroughGui(page, leader.username, ADMIN_PASSWORD);
  await expect(page.getByRole('button', { name: 'Sign in' })).toHaveCount(0);
  const inside = (await api.get('/api/workers')).find(
    (w: { workerId: string }) => w.workerId !== name,
  );
  expect(inside, 'the worker inside the leader').toBeDefined();
  const heard = async () => {
    await openAdmin(page, 'Workers');
    await workerRow(page, name, 'Heartbeating');
    await workerRow(page, inside.workerId, 'Heartbeating');
  };
  await heard();
  return { leader, api, heard };
}

test('every worker follows the certificate from Leader issued to Uploaded and back', async ({
  fleet,
  page,
}) => {
  // S6
  test.setTimeout(300_000);
  const { api, heard } = await fleetOfTwo(fleet, page, 'w-follows');
  const pki = await fleet.externalPki();

  await switchToUploaded(page, pki);
  await heard();
  const uploaded = await api.get('/api/settings/certificates');
  expect(uploaded.supplied).toBe(true);
  expect(uploaded.rotation).toBeNull();

  await switchToLeaderIssued(page);
  await heard();
  const own = await api.get('/api/settings/certificates');
  expect(own.supplied).toBe(false);
  expect(own.rotation).toBeNull();
  await api.ctx.dispose();
});

test('a renewal is served while the switch back to Leader issued waits for a worker', async ({
  fleet,
  page,
}) => {
  // S6
  test.setTimeout(300_000);
  const { api } = await fleetOfTwo(fleet, page, 'w-away');
  // Two leaves under one authority that is somebody else's: the second is the first's renewal.
  const other = await fleet.startLeader();
  const otherApi = await claimAdmin(other);
  const tls = path.join(other.proc.dir, 'smeltr-leader-data', 'tls');
  const read = (name: string) => fs.readFileSync(path.join(tls, name), 'utf8');
  const pki = {
    certificate: read('cert.pem'),
    privateKey: read('key.pem'),
    authority: read('ca.pem'),
  };
  await otherApi.post('/api/settings/certificates/reissue', {});
  const renewal = {
    certificate: read('cert.pem'),
    privateKey: read('key.pem'),
    authority: pki.authority,
  };
  expect(renewal.certificate).not.toBe(pki.certificate);
  await otherApi.ctx.dispose();
  await other.proc.stop();

  await switchToUploaded(page, pki);
  const before = await api.get('/api/settings/certificates');

  // A worker gone quiet is still waited for, so the switch back stays open.
  await fleet.stopWorker('w-away');
  let served = await openCertificates(page);
  await source(served, 'Leader issued').click();
  await served.getByRole('button', { name: 'Switch to Leader issued' }).click();
  await expect(
    served.getByRole('heading', { name: /^Switch to Leader issued in progress/ }),
  ).toBeVisible();

  // Each worker is listed with whether it holds the incoming authority, and the one gone quiet does not.
  await expect(served.getByRole('listitem').filter({ hasText: 'w-away' })).toContainText('waiting');
  // The deadline moves, and Discard and Save show only while it is being changed.
  const picker = served.getByLabel('Cut over by');
  const later = new Date(Date.now() + 3 * 60 * 60 * 1000);
  later.setSeconds(0, 0);
  const local = new Date(later.getTime() - later.getTimezoneOffset() * 60_000)
    .toISOString()
    .slice(0, 16);
  await expect(served.getByRole('button', { name: 'Discard' })).toHaveCount(0);
  await picker.fill(local);
  await served.getByRole('button', { name: 'Save', exact: true }).click();
  await expect(served.getByRole('button', { name: 'Discard' })).toHaveCount(0);
  expect((await api.get('/api/settings/certificates')).rotation.deadlineMillis).toBe(
    later.getTime(),
  );

  // The chooser is still there under the switch, and a renewal under the uploaded root is served at once.
  served = await chooseUploaded(page, renewal);
  await served.getByRole('button', { name: 'Serve this certificate' }).click();
  await expect(served.getByRole('status')).toHaveText(
    'this leader is serving the certificate you supplied',
  );
  await expect(
    served.getByRole('heading', { name: /^Switch to Leader issued in progress/ }),
  ).toBeVisible();
  const renewed = await api.get('/api/settings/certificates');
  expect(renewed.supplied).toBe(true);
  expect(renewed.rotation?.installs).toBe('own');
  expect(renewed.served.fingerprint).not.toBe(before.served.fingerprint);
  expect(renewed.authority.fingerprint).toBe(before.authority.fingerprint);

  await served.getByRole('button', { name: 'Abandon switch' }).click();
  await inForce(served, 'Uploaded');

  // Replacing the entry the leader serves serves the new version.
  await replaceServed(page, pki);
  const replaced = await api.get('/api/settings/certificates');
  expect(replaced.served.fingerprint).toBe(before.served.fingerprint);
  await api.ctx.dispose();
});

test('every worker follows the certificate to ACME, through a renewal and on to a new root', async ({
  fleet,
  page,
}) => {
  // S6
  test.setTimeout(300_000);
  const { leader, api, heard } = await fleetOfTwo(fleet, page, 'w-acme');
  const authority = await Authority.start({ tlsPort: leader.port, httpPort: leader.acmeHttpPort });
  try {
    // The first order is under a root the fleet does not hold: a switch, published before it is served.
    await switchToAcme(page, authority, ['127.0.0.1']);
    await heard();
    const first = await api.get('/api/settings/certificates');
    expect(first.source).toBe('acme');
    expect(first.rotation).toBeNull();
    expect(first.served.names).toContain('127.0.0.1');
    expect(authority.issued).toBe(1);
    const acme = await api.get('/api/settings/acme');
    expect(acme.issued).toBe(first.served.fingerprint);
    expect(acme.lastError).toBeNull();
    expect(acme.renewAfterMillis).toBeLessThan(acme.notAfterMillis);

    // A renewal under the same root, proved the other way, is served at once.
    await renewAcme(page, 'HTTP-01');
    const renewed = await api.get('/api/settings/certificates');
    expect(renewed.served.fingerprint).not.toBe(first.served.fingerprint);
    expect(renewed.authority.fingerprint).toBe(first.authority.fingerprint);
    await heard();

    // The authority moves to a new root: the fleet is given it before anything is served under it.
    await acmeNewRoot(page, authority);
    const moved = await api.get('/api/settings/certificates');
    expect(moved.source).toBe('acme');
    expect(moved.authority.fingerprint).not.toBe(renewed.authority.fingerprint);
    await heard();

    // And back to the leader's own.
    await switchToLeaderIssued(page);
    await heard();
    expect((await api.get('/api/settings/certificates')).source).toBe('own');
  } finally {
    await authority.stop();
    await api.ctx.dispose();
  }
});
