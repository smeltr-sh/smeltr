// Builds each frontend in `frontends.json` only when a source is newer than its output. Every build rewrites the
// output, and build.rs watches it, so an unneeded build makes cargo recompile the crate.
import { execSync } from 'node:child_process';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

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

const frontends = JSON.parse(readFileSync(join(import.meta.dirname, 'frontends.json'), 'utf8'));
for (const [dir, { output, sources }] of Object.entries(frontends)) {
  process.chdir(join(import.meta.dirname, '..', dir));
  if (Math.max(...sources.map(newest)) > newest(output)) {
    execSync('npm run build', { stdio: 'inherit' });
  }
}
