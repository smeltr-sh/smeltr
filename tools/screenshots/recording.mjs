// How the website demo's recording is written: what every page reads in one file, and each part a page reads only
// once the visitor asks for it -- a chart at another window, a capture -- in a file of its own, named for its key
// (`gui-frontend/src/demo/parts.ts`), under `recorded/` beside it.
//
//   node recording.mjs <recorded.json>   # splits a recording written whole into its parts
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const STATS = /^\/api\/areas\/[^/]+\/stats$/;
/** The answers the fleet moves in, read again over a minute so the demo moves as the fleet did. */
export const LIVE =
  /^\/api\/(map\/measurements|areas\/[^/?]+\/stats|flow|cluster|workers|warnings|streams)\b/;
/** The windows each chart opens on (`DEFAULT_CHART_RANGE` in `map.ts`, `DEFAULT_WINDOW_MINUTES` in `flow.ts`). */
const OPENS_ON = [
  [STATS, '10'],
  [/^\/api\/flow$/, '60'],
];

/** Whether `key` is a chart's read at a window it does not open on: the visitor picks it, and it stands still. */
export function pickedWindow(key) {
  const url = new URL(key, 'http://recording');
  const window = url.searchParams.get('windowMinutes');
  return (
    window !== null && !OPENS_ON.some(([p, opens]) => p.test(url.pathname) && opens === window)
  );
}

/** The file that holds `key`'s part. */
export function partFile(key) {
  return `recorded-${crypto.createHash('sha256').update(key).digest('hex').slice(0, 16)}.json`;
}

/** The recording as the public demo shows it. A version names the branch that built the fleet; the demo shows it as
 * built from main. The visitor is the fleet's first account, whose name, Administrator, would read twice beside its
 * role. */
function shown(value) {
  return JSON.stringify(value)
    .replace(/(\d{4}\.\d+\.\d+\+)[A-Za-z0-9-]+\.([0-9a-f]{12})/g, '$1main.$2')
    .replaceAll('"Administrator"', '"Demo User"');
}

/** Writes `recording` to `out` and its parts beside it, replacing the parts an earlier one wrote. */
export function write(out, { recordedAt, answers, moving, movingAt, captures }) {
  const dir = path.join(path.dirname(out), 'recorded');
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir);
  const part = (key, value) => fs.writeFileSync(path.join(dir, partFile(key)), shown(value));
  const every = {};
  for (const [key, answer] of Object.entries(answers)) {
    if (pickedWindow(key)) part(key, answer);
    else every[key] = answer;
  }
  for (const [key, pool] of Object.entries(captures)) part(key, pool);
  fs.writeFileSync(out, shown({ recordedAt, answers: every, moving, movingAt }));
  return fs.readdirSync(dir).length;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const out = process.argv[2];
  if (!out) throw new Error('usage: node recording.mjs <recorded.json>');
  console.log(`${write(out, JSON.parse(fs.readFileSync(out, 'utf8')))} parts written`);
}
