// A fleet of its own for every test, started from empty data directories, and the browser pointed at it.
//
// `leader` is a fresh leader nobody has signed in to, with the one-time password it printed. `admin` is the
// same leader after its administrator has chosen `ADMIN_PASSWORD`, and a page already signed in as them, editing
// branch `work` as one who has chosen it: what most tests start from, since only the first-start and branch
// tests are about getting there. `api` is that administrator
// over the API, for arranging what a test is not about. Everything a test starts is stopped when it ends.
import {
  test as base,
  expect,
  request,
  type APIRequestContext,
  type Page,
  type Request,
} from '@playwright/test';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RUN_DIR, recordPid } from './run';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const TARGET = path.resolve(REPO, process.env.CARGO_TARGET_DIR ?? 'target');
const BINARY = process.env.SMELTR_BINARY ?? path.join(TARGET, 'debug', 'smeltr');

/** The password the `admin` fixture chooses for the administrator. */
export const ADMIN_PASSWORD = 'gui-tests-admin-password';

/** A smeltr process a test started, and everything it has written to its console. */
export class Proc {
  output = '';
  private failed = false;
  constructor(
    readonly name: string,
    readonly child: ChildProcess,
    readonly dir: string,
  ) {
    child.stdout!.on('data', (d) => (this.output += d));
    child.stderr!.on('data', (d) => (this.output += d));
    child.once('error', (e) => {
      // A process that failed to start may never emit `exit`.
      this.failed = true;
      this.output += `\nspawn failed: ${e.message}\n`;
    });
  }

  get running() {
    return !this.failed && this.child.exitCode === null && this.child.signalCode === null;
  }

  /** SIGTERM, then SIGKILL if it outlives `graceMs`. */
  async stop(graceMs = 30_000) {
    if (!this.running) return;
    const exited = new Promise((r) => this.child.once('exit', r));
    // A process a test froze to make it go silent handles no SIGTERM until it is woken.
    this.child.kill('SIGCONT');
    this.child.kill('SIGTERM');
    const timeout = setTimeout(() => this.child.kill('SIGKILL'), graceMs);
    await exited;
    clearTimeout(timeout);
  }
}

export type Leader = {
  url: string;
  port: number;
  /** Where it answers a certificate authority's HTTP-01 challenge. */
  acmeHttpPort: number;
  username: string;
  oneTimePassword: string;
  proc: Proc;
};

/** An API client signed in as one account. Every unsafe request carries the leader's exact origin and the
 * session's CSRF value, as the GUI's own do. */
export class Api {
  constructor(
    readonly ctx: APIRequestContext,
    readonly csrf: string,
  ) {}

  async call<T = any>(method: string, url: string, data?: unknown): Promise<T> {
    const res = await this.ctx.fetch(url, {
      method,
      data: data as any,
      headers: method === 'GET' ? {} : { 'x-smeltr-csrf': this.csrf },
    });
    const text = await res.text();
    expect(res.ok(), `${method} ${url} -> ${res.status()}: ${text}`).toBe(true);
    const body = text ? JSON.parse(text) : undefined;
    // Many routes answer a refusal with 200 and `ok: false`; arranging never expects one.
    expect(body?.ok, `${method} ${url}: ${text}`).not.toBe(false);
    return body as T;
  }
  get = <T = any>(url: string) => this.call<T>('GET', url);
  post = <T = any>(url: string, data?: unknown) => this.call<T>('POST', url, data);
  put = <T = any>(url: string, data?: unknown) => this.call<T>('PUT', url, data);
  delete = <T = any>(url: string) => this.call<T>('DELETE', url);

  /** Opens `branch` for review as `title`. */
  openChange(branch: string, title: string, description = '') {
    return this.post(`/api/branches/${encodeURIComponent(branch)}/open`, { title, description });
  }

  /** Approves the change open on `branch`, which merges it to main and deploys it. */
  deploy(branch: string) {
    return this.post(`/api/branches/${encodeURIComponent(branch)}/deploy`, {});
  }
}

/** A certificate and its key, and the authority that issued it with that authority's key. */
export interface Pki {
  certificate: string;
  privateKey: string;
  authority: string;
  authorityKey: string;
}

/** Starts and stops the processes of one test. */
export class Fleet {
  private procs: Proc[] = [];
  private dirs: string[] = [];

  private tempDir(prefix: string) {
    const dir = fs.mkdtempSync(path.join(process.env[RUN_DIR] ?? os.tmpdir(), `${prefix}-`));
    this.dirs.push(dir);
    return dir;
  }

  private spawn(name: string, dir: string, settings: Record<string, string>): Proc {
    if (!fs.existsSync(BINARY))
      throw new Error(`no binary at ${BINARY}: run \`just build-dev\` first`);
    // The caller's environment without any Smeltr setting, so one exported for a developer's own fleet does
    // not point a test at that fleet's data.
    const env: NodeJS.ProcessEnv = {};
    for (const [k, v] of Object.entries(process.env)) if (!k.startsWith('SMELTR_')) env[k] = v;
    const child = spawn(BINARY, [], {
      cwd: dir,
      env: { ...env, ...settings },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    if (process.env[RUN_DIR] && child.pid) recordPid(process.env[RUN_DIR], child.pid, BINARY);
    const proc = new Proc(name, child, dir);
    this.procs.push(proc);
    return proc;
  }

  /** A leader on a free port. `settings` adds or overrides its `SMELTR_*` environment. */
  async startLeader(settings: Record<string, string> = {}): Promise<Leader> {
    const port = await freePort();
    const acmeHttpPort = await freePort();
    const proc = this.spawn('leader', this.tempDir('leader'), {
      SMELTR_LEADER_ENABLED: '1',
      SMELTR_GUI_PORT: String(port),
      SMELTR_GUI_BIND: '127.0.0.1',
      SMELTR_ACME_HTTP_PORT: String(acmeHttpPort),
      ...settings,
    });
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const username = /username: (\S+)/.exec(proc.output)?.[1];
      const password = /one-time password: (\S+)/.exec(proc.output)?.[1];
      // The password is printed before the certificate is served, and a test may read what is served.
      const served = fs.existsSync(path.join(proc.dir, 'smeltr-leader-data', 'tls', 'served.json'));
      if (username && password && served && (await listening(port)))
        return {
          url: `https://127.0.0.1:${port}`,
          port,
          acmeHttpPort,
          username,
          oneTimePassword: password,
          proc,
        };
      if (!proc.running) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error(`the leader did not start:\n${proc.output}`);
  }

  /** A worker enrolled in `group` with a code `api` mints for it, named `name`. */
  async startWorker(
    leader: Leader,
    api: Api,
    group: string,
    name: string,
    settings: Record<string, string> = {},
  ): Promise<Proc> {
    const minted = await api.post('/api/enrollment-codes', {
      groupId: group,
      maxUses: 1,
      lifetimeHours: 1,
    });
    expect(minted.ok, JSON.stringify(minted)).not.toBe(false);
    return this.startWorkerWith(name, {
      SMELTR_WORKER_GROUP: group,
      SMELTR_WORKER_ENROLLMENT_CODE: minted.code.value,
      SMELTR_LEADER_URL: leader.url,
      ...settings,
    });
  }

  /** A worker named `name`, enrolled with whatever `settings` give it: the lines the Workers page hands out
   * with an enrollment code, say. One transform thread and a heartbeat a second unless `settings` say
   * otherwise. */
  startWorkerWith(name: string, settings: Record<string, string>): Proc {
    const dir = this.tempDir(name);
    // A worker keeps the id it finds here, so the fleet reads as names rather than as `host-pid-nanos`.
    fs.mkdirSync(path.join(dir, 'smeltr-worker-data'));
    fs.writeFileSync(path.join(dir, 'smeltr-worker-data', 'worker_id'), name);
    return this.spawn(name, dir, {
      SMELTR_WORKER_ENABLED: '1',
      SMELTR_WORKER_THREAD_LIMIT: '1',
      SMELTR_WORKER_HEARTBEAT_INTERVAL_MS: '1000',
      ...settings,
    });
  }

  /** A certificate, key and authority from a PKI that is somebody else's, minted with `openssl`. It carries the
   * names every test leader answers to. With `under`, the certificate is another under that one's authority: its
   * renewal. */
  externalPki(under?: Pki): Pki {
    const dir = this.tempDir('pki');
    const openssl = (...args: string[]) =>
      execFileSync('openssl', args, { cwd: dir, stdio: 'pipe' });
    const key = ['-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes'];
    if (under) {
      fs.writeFileSync(path.join(dir, 'ca.pem'), under.authority);
      fs.writeFileSync(path.join(dir, 'ca.key'), under.authorityKey);
    } else {
      openssl(
        'req',
        '-x509',
        ...key,
        '-keyout',
        'ca.key',
        '-out',
        'ca.pem',
        '-days',
        '30',
        '-subj',
        '/CN=somebody else',
        '-addext',
        'basicConstraints=critical,CA:TRUE',
        '-addext',
        'keyUsage=critical,keyCertSign',
      );
    }
    fs.writeFileSync(
      path.join(dir, 'leaf.ext'),
      'subjectAltName=DNS:localhost,IP:127.0.0.1\nkeyUsage=critical,digitalSignature\n' +
        'extendedKeyUsage=serverAuth,clientAuth\n',
    );
    openssl(
      'req',
      '-new',
      ...key,
      '-keyout',
      'leaf.key',
      '-out',
      'leaf.csr',
      '-subj',
      '/CN=localhost',
    );
    openssl(
      'x509',
      '-req',
      '-in',
      'leaf.csr',
      '-CA',
      'ca.pem',
      '-CAkey',
      'ca.key',
      '-set_serial',
      String(Date.now()),
      '-days',
      '7',
      '-extfile',
      'leaf.ext',
      '-out',
      'leaf.pem',
    );
    const read = (name: string) => fs.readFileSync(path.join(dir, name), 'utf8');
    return {
      certificate: read('leaf.pem'),
      privateKey: read('leaf.key'),
      authority: read('ca.pem'),
      authorityKey: read('ca.key'),
    };
  }

  /** Stops the worker named `name`. */
  async stopWorker(name: string) {
    await Promise.all(this.procs.filter((p) => p.name === name).map((p) => p.stop()));
  }

  async stop() {
    // Workers first, so none of them spends its shutdown failing to reach a leader that is already gone.
    const [leaders, workers] = [
      this.procs.filter((p) => p.name === 'leader'),
      this.procs.filter((p) => p.name !== 'leader'),
    ];
    await Promise.all(workers.map((p) => p.stop()));
    await Promise.all(leaders.map((p) => p.stop()));
    for (const d of this.dirs) fs.rmSync(d, { recursive: true, force: true });
  }
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

async function listening(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.connect(port, '127.0.0.1');
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    socket.once('error', () => resolve(false));
  });
}

/** An API client for `leader`, signed in as `username`. */
export async function signedIn(leader: Leader, username: string, password: string): Promise<Api> {
  const ctx = await request.newContext({
    baseURL: leader.url,
    ignoreHTTPSErrors: true,
    extraHTTPHeaders: { Origin: leader.url },
  });
  const res = await ctx.post('/api/auth/login', { data: { username, password } });
  expect(res.ok(), await res.text()).toBe(true);
  return new Api(ctx, (await res.json()).csrf);
}

/** A second administrator, past their first-sign-in password change, signed in over the API. */
export async function addAccount(
  leader: Leader,
  api: Api,
  username: string,
  displayName: string,
): Promise<Api> {
  const made = await api.post('/api/users', { username, displayName });
  const first = await signedIn(leader, username, made.temporaryPassword);
  const password = `${username}-chosen-password`;
  await first.post('/api/auth/password', { username, newPassword: password });
  await first.ctx.dispose();
  return signedIn(leader, username, password);
}

/** What `settled` knows of a page's requests to the leader: those in flight, when the last of them ended, what
 * this page has already had answered, and the latest asked. */
type Requests = {
  open: Set<Request>;
  quietSince: number;
  answered: Set<string>;
  recent: { at: number; what: string }[];
};
const watched = new WeakMap<Page, Requests>();

/** Starts watching `page`'s requests to the leader, for `settled`. Call before the page's first navigation, so no
 * request is already in flight when watching begins. */
export function watchRequests(page: Page) {
  if (watched.has(page)) return;
  const s: Requests = { open: new Set(), quietSince: Date.now(), answered: new Set(), recent: [] };
  watched.set(page, s);
  const key = (r: Request) => {
    const url = new URL(r.url());
    return url.pathname.startsWith('/api/') ? `${r.method()} ${url.pathname}${url.search}` : null;
  };
  // A page shown anew asks for its data anew.
  page.on('framenavigated', (f) => {
    if (f === page.mainFrame()) s.answered.clear();
  });
  page.on('request', (r) => {
    const k = key(r);
    if (k === null) return;
    s.recent = [...s.recent.slice(-9), { at: Date.now(), what: k }];
    // Asked again after an answer, by the same page: a poll, which never stops.
    if (s.answered.has(k)) return;
    s.open.add(r);
  });
  const ended = (r: Request, status = 0) => {
    const k = key(r);
    // Only a read that worked is polled; a refusal or a write is asked again because something changed.
    if (k !== null && r.method() === 'GET' && status >= 200 && status < 300) s.answered.add(k);
    if (s.open.delete(r) && s.open.size === 0) s.quietSince = Date.now();
  };
  // Answered is enough: a body the page never reads is never "finished".
  page.on('response', (r) => ended(r.request(), r.status()));
  page.on('requestfailed', (r) => ended(r));
}

/** Waits until `page` has had everything it asked the leader for answered, and asked nothing new for half a
 * second: what its page shows has come. A poll, asking again for what was answered, is not waited on. The page
 * must be watched (`watchRequests`). */
export async function settled(page: Page) {
  const s = watched.get(page)!;
  // Waiting too long names what is still open, and what was asked last.
  await expect
    .poll(
      () =>
        (s.open.size === 0 && Date.now() - s.quietSince >= 500) || {
          open: [...s.open].map((r) => r.url()),
          latest: s.recent.map((r) => `${Date.now() - r.at}ms ago: ${r.what}`),
        },
      { timeout: 15_000 },
    )
    .toBe(true);
}

/** The administrator of a fresh `leader`, signed in over the API. The one-time password is spent here, swapped for
 * `ADMIN_PASSWORD`; changing it ends that session. */
export async function claimAdmin(leader: Leader): Promise<Api> {
  const first = await signedIn(leader, leader.username, leader.oneTimePassword);
  await first.post('/api/auth/password', {
    username: leader.username,
    newPassword: ADMIN_PASSWORD,
  });
  await first.ctx.dispose();
  return signedIn(leader, leader.username, ADMIN_PASSWORD);
}

/** Opens `hash`, a GUI route like `#/build/lookups`, on `leader`. */
export async function goto(page: Page, leader: Leader, hash: string) {
  await page.goto(`${leader.url}/${hash}`);
}

/** Signs `page` in through the GUI's own form, clicking into each field as a person does, so a field
 * something covers fails here. */
export async function signInThroughGui(page: Page, username: string, password: string) {
  for (const [field, value] of [
    [page.getByLabel('Username'), username],
    [page.getByLabel('Password', { exact: true }), password],
  ] as const) {
    await field.click();
    await field.fill(value);
  }
  await page.getByRole('button', { name: 'Sign in' }).click();
}

export const test = base.extend<{ fleet: Fleet; leader: Leader; api: Api; admin: Page }>({
  fleet: async ({}, use) => {
    const fleet = new Fleet();
    try {
      await use(fleet);
    } finally {
      await fleet.stop();
    }
  },
  leader: async ({ fleet }, use) => {
    await use(await fleet.startLeader());
  },
  api: async ({ leader }, use) => {
    const api = await claimAdmin(leader);
    await use(api);
    await api.ctx.dispose();
  },
  admin: async ({ page, leader, api }, use) => {
    void api;
    // The branch the GUI remembers, as a browser that already chose one does; a new browser opens on main.
    await page.addInitScript(() => {
      if (!localStorage.getItem('smeltr-branch')) localStorage.setItem('smeltr-branch', 'work');
    });
    watchRequests(page);
    await page.goto(leader.url);
    await signInThroughGui(page, leader.username, ADMIN_PASSWORD);
    await expect(page.getByRole('button', { name: 'Sign in' })).toHaveCount(0);
    await use(page);
  },
});

export { expect };
