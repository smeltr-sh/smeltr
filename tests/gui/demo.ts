// The website demo as its page and its service serve it, for the specs that drive it.
import { expect } from '@playwright/test';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RUN_DIR, recordPid } from './run';

export const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const TARGET = path.resolve(REPO, process.env.CARGO_TARGET_DIR ?? 'target');
const SERVICE = path.join(TARGET, 'debug', 'smeltr-demo');
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

/** The demo as the website serves it, with its service beside it: `base` is its page. */
export interface Demo {
  base: string;
  stop: () => void;
}

/** Starts the demo from what `just build-dev` built. */
export async function startDemo(): Promise<Demo> {
  for (const needed of [SERVICE, path.join(SITE, 'demo.html')]) {
    if (!fs.existsSync(needed))
      throw new Error(`no ${needed}: run \`just build-dev\`, which builds it`);
  }
  const servicePort = await freePort();
  const service = spawn(SERVICE, [], {
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
  const server = site(servicePort);
  const port = await freePort();
  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve));
  return {
    base: `http://127.0.0.1:${port}/demo.html`,
    stop: () => {
      service.kill();
      server.close();
    },
  };
}
