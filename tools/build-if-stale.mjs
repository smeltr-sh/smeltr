// Builds the frontends in `frontends.json` named on the command line, or each one the binary embeds when none is
// named, and only when a source is newer than its output. Every build rewrites the output, and build.rs watches it, so
// an unneeded build makes cargo recompile the crate.
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
const names = process.argv.slice(2);
for (const [name, { dir, script, output, sources, embed }] of Object.entries(frontends)) {
  if (names.length ? !names.includes(name) : !embed) continue;
  process.chdir(join(import.meta.dirname, '..', dir));
  if (Math.max(...sources.map(newest)) > newest(output)) {
    execSync(`npm run ${script}`, { stdio: 'inherit' });
  }
}
