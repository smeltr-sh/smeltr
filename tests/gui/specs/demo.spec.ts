// The website demo: the GUI over the leader its page plays (`gui-frontend/src/demo`), with `smeltr-demo` answering
// what only the engine can. A visitor captures from a source, saves what it caught on a branch of their own, and
// previews a pipeline over it, a lookup table included.
import { test, expect, type Page } from '@playwright/test';
import { spawn, type ChildProcess } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RUN_DIR, recordPid } from '../run';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..', '..');
const TARGET = path.resolve(REPO, process.env.CARGO_TARGET_DIR ?? 'target');
const SERVICE = path.join(TARGET, 'release', 'smeltr-demo');
const SITE = path.join(REPO, 'gui-frontend', 'dist-demo');
const TYPES: Record<string, string> = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
};

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

/** `dist-demo/` as the website serves it, with `/preview/` passed to the service at `servicePort`, as the page asks
 * for it when built with no service named. */
function site(servicePort: number): http.Server {
  return http.createServer((req, res) => {
    const url = new URL(req.url!, 'http://site');
    if (url.pathname.startsWith('/preview/')) {
      const out = http.request(
        {
          port: servicePort,
          path: url.pathname.slice('/preview'.length),
          method: req.method,
          headers: req.headers,
        },
        (answer) => {
          res.writeHead(answer.statusCode!, answer.headers);
          answer.pipe(res);
        },
      );
      req.pipe(out);
      return;
    }
    const file = path.join(SITE, path.normalize(url.pathname));
    if (!file.startsWith(SITE) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, { 'content-type': TYPES[path.extname(file)] ?? 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  });
}

let service: ChildProcess;
let server: http.Server;
let base: string;

test.beforeAll(async () => {
  for (const needed of [SERVICE, path.join(SITE, 'demo.html')]) {
    if (!fs.existsSync(needed))
      throw new Error(`no ${needed}: run \`just check-gui\`, which builds it`);
  }
  const servicePort = await freePort();
  service = spawn(SERVICE, [], {
    env: {
      PATH: process.env.PATH,
      SMELTR_DEMO_BIND: `127.0.0.1:${servicePort}`,
      SMELTR_DEMO_BUNDLE: path.join(REPO, 'gui-frontend', 'src', 'demo'),
    },
    stdio: 'inherit',
  });
  if (process.env[RUN_DIR] && service.pid) recordPid(process.env[RUN_DIR], service.pid, SERVICE);
  await expect
    .poll(() =>
      fetch(`http://127.0.0.1:${servicePort}/health`).then(
        (r) => r.ok,
        () => false,
      ),
    )
    .toBe(true);
  server = site(servicePort);
  const port = await freePort();
  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve));
  base = `http://127.0.0.1:${port}/demo.html`;
});

test.afterAll(() => {
  service?.kill();
  server?.close();
});

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
  await dialog.locator("label:has(> .name:text-is('Group')) select").selectOption('network');
  await dialog
    .locator("label:has(> .name:text-is('Capture target')) select")
    .selectOption('panw-firewalls');
  await dialog
    .locator("label:has(> .name:text-is('Keep records where')) input")
    .fill('_raw.includes("TRAFFIC")');
  await dialog.locator("input[aria-label='records']").fill('50');
  await dialog.locator('button[type=submit]', { hasText: 'Start' }).click();
  await expect(page.getByText('Save the capture')).toBeVisible();
  await expect(page.getByText('Finished. 50 records')).toBeVisible();

  await page.getByRole('button', { name: 'Save sample' }).click();
  // main is never edited, so the save asks for a branch and the demo's leader forks one.
  await page.getByRole('button', { name: 'Edit on this branch' }).click();
  await expect(page.locator('dialog[open]')).toHaveCount(0);
  const sample = await page.evaluate(() => {
    const state = JSON.parse(sessionStorage.getItem('smeltr-demo')!);
    const branch = Object.values(state.branches as Record<string, { touches: string[] }>).find(
      (b) => b.touches.some((t) => t.includes('/samples/')),
    );
    return branch!.touches
      .find((t) => t.includes('/samples/'))!
      .replace(/.*\/samples\/(.*)\.json$/, '$1');
  });

  await page.goto(`${base}#/build/pipelines/panos-traffic/for/${sample}`);
  await expect(page.getByText('panw-firewalls, captured', { exact: true })).toBeVisible();
  await expect(page.getByText('50 rows', { exact: true })).toBeVisible();
  // The directory table is a CSV under the service's bundle: a department in the output is a lookup that read it.
  await expect(page.getByText('Engineering').first()).toBeVisible();
});

test('the map draws the fleet from the service', async ({ page }) => {
  await open(page, '#/map');
  await expect(page.getByText('panw-firewalls').first()).toBeVisible();
  await expect(page.getByText('nginx-access').first()).toBeVisible();
  await expect(page.getByText('lookup key column')).toHaveCount(0);
});

test("the samples page runs the scope's locked tests in the service", async ({ page }) => {
  await open(page, '#/build');
  await expect(page.getByText('panos-traffic maps sessions to ECS')).toBeVisible();
  await expect(page.getByText('Passing')).toHaveCount(2);
});
