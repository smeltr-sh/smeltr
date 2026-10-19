// Builds `dist` only when a source is newer than it. Every build rewrites `dist`, and build.rs watches it, so an
// unneeded build makes cargo recompile the crate. The sources are the ones build.rs compares against.
import { execSync } from 'node:child_process';
import { readdirSync, statSync } from 'node:fs';
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

process.chdir(import.meta.dirname);
const sources = [
  'src',
  'public',
  'index.html',
  'package.json',
  '../website/src/lib/highlight',
  '../website/src/lib/foundry',
];
if (Math.max(...sources.map(newest)) > newest('dist')) {
  execSync('npm run build', { stdio: 'inherit' });
}
