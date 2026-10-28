// The leader's authority rotated under a running worker, with the cutover left to the heartbeat that finds the
// fleet holding the incoming root. Afterwards the leader presents what its disk holds, and the worker is handed
// the bundle without the root that was rotated away.
import fs from 'node:fs';
import path from 'node:path';
import { test, expect, claimAdmin } from '../fleet';

const roots = (pem: string) => pem.split('-----BEGIN CERTIFICATE-----').length - 1;

test('a rotation the fleet finishes leaves the leader presenting what is on disk', async ({
  fleet,
}) => {
  // S6
  test.setTimeout(120_000);
  const leader = await fleet.startLeader();
  const api = await claimAdmin(leader);
  const worker = await fleet.startWorker(leader, api, 'default', 'w-rotates');
  const pinned = path.join(worker.dir, 'smeltr-worker-data', 'pinned-ca.pem');
  await expect.poll(() => fs.existsSync(pinned), { timeout: 30_000 }).toBe(true);
  const before = await api.get('/api/settings/tls');

  const begun = await api.post('/api/settings/tls/rotation', { waitHours: 1 });
  expect(begun.rotation).not.toBeNull();
  // The worker takes the overlap bundle on a beat, and the beat after cuts the rotation over.
  await expect
    .poll(async () => (await api.get('/api/settings/tls')).rotation, { timeout: 60_000 })
    .toBeNull();

  const after = await api.get('/api/settings/tls');
  expect(after.authority.fingerprint).not.toBe(before.authority.fingerprint);
  expect(after.restartRequired).toBe(false);
  // What a worker pins and an operator downloads: refused while the socket presents a certificate other than
  // the one on disk, which a cutover must not leave behind.
  const handed = await api.ctx.get('/api/worker/authority');
  expect(handed.status(), await handed.text()).toBe(200);
  expect(roots(await handed.text())).toBe(1);
  // The outgoing root leaves the worker's trust by its absence from the bundle handed whole on the next beat.
  await expect.poll(() => roots(fs.readFileSync(pinned, 'utf8')), { timeout: 30_000 }).toBe(1);
  await api.ctx.dispose();
});
