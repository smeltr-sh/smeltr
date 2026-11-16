// Builds each frontend named on the command line only when a source is newer than its output. Every build rewrites
// the output, and build.rs watches it, so an unneeded build makes cargo recompile the crate. The outputs and sources
// are build.rs's `GUI` and `DOCS`.
import { execSync } from 'node:child_process';
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

const FRONTENDS = {
  gui: {
    dir: 'gui-frontend',
    output: 'dist',
    sources: [
      'src',
      'public',
      'index.html',
      'package.json',
      '../website/src/lib/highlight',
      '../website/src/lib/foundry',
      '../website/src/lib/reference.json',
      '../website/src/lib/code-language.json',
    ],
  },
  docs: {
    dir: 'website',
    output: 'leader-docs',
    sources: ['src', 'scripts', 'package.json', 'vite.config.ts'],
  },
};

function newest(path) {
  let stat;
  try {
    stat = statSync(path);
  } catch {
    return 0;
  }
  if (!stat.isDirectory()) return stat.mtimeMs;
  return Math.max(0, ...readdirSync(path).map((name) => newest(join(path, name))));
}

for (const name of process.argv.slice(2)) {
  const { dir, output, sources } = FRONTENDS[name];
  process.chdir(join(import.meta.dirname, '..', dir));
  if (Math.max(...sources.map(newest)) > newest(output)) {
    execSync('npm run build', { stdio: 'inherit' });
  }
}
