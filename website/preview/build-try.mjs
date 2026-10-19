// Builds the demo you can use (`gui-frontend`, `npm run build:demo`) into build/demo/try, the assets the
// `smeltr-preview` Worker serves at /demo/try. The page and the service it asks ship in one deploy, from one commit,
// so the page never asks a service that answers another shape.
import { execFileSync } from 'node:child_process';
import { existsSync, renameSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const gui = fileURLToPath(new URL('../../gui-frontend/', import.meta.url));
const out = fileURLToPath(new URL('./build/demo/try/', import.meta.url));
/** The npm that runs this script, through node: on Windows `npm` is a `.cmd`, which no shell-less spawn can run.
 * @param {string[]} args */
const npm = (...args) =>
  process.env.npm_execpath
    ? execFileSync(process.execPath, [process.env.npm_execpath, ...args], {
        cwd: gui,
        stdio: 'inherit',
      })
    : execFileSync('npm', args, { cwd: gui, stdio: 'inherit' });

// Cloudflare's build installs the site's packages alone.
if (!existsSync(`${gui}node_modules`)) npm('ci');
npm('run', 'build:demo', '--', '--base', '/demo/try/', '--outDir', out, '--emptyOutDir');
renameSync(`${out}demo.html`, `${out}index.html`);
