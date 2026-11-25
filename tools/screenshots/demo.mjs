// Record what the staged fleet's leader answers, for the website demo's leader to answer in its place.
//
//   node demo.mjs ../../gui-frontend/src/demo/recorded.json
//
// Opens every page `shots.json` names and everything the GUI reaches by reading (`reach.mjs`), as the signed-in
// administrator, and keeps the last answer to every GET under `/api/`, keyed by its path and sorted query. Then reads
// the live answers over a minute, so the demo's fleet moves as the staged one did, and captures what each of main's
// sources reads and destinations are sent, so a capture in the demo draws from what the fleet did. `stage.py` must
// have staged the fleet first.
import { chromium } from 'playwright';
import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { reachable } from './reach.mjs';
import { LIVE, STATS, pickedWindow, write } from './recording.mjs';

const BASE = 'https://127.0.0.1:8443';
const DIR = path.dirname(fileURLToPath(import.meta.url));
const out = process.argv[2];
if (!out) throw new Error('usage: node demo.mjs <out.json>');

const shots = JSON.parse(fs.readFileSync(path.join(DIR, 'shots.json'), 'utf8'));
const sessions = JSON.parse(fs.readFileSync(path.join(DIR, 'state', 'sessions.json'), 'utf8'));
const cookie = sessions.admin[0];
const eq = cookie.indexOf('=');

/** `/api/x?b=2&a=1` as `/api/x?a=1&b=2`, so one question asked two ways is one key. */
export function keyOf(url) {
  const u = new URL(url, BASE);
  const q = [...u.searchParams.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return u.pathname + (q.length ? '?' + new URLSearchParams(q) : '');
}

/** Every subject's points in the leader's series store at `file` (`series_points`), as `series.ts` reads them: per
 * tier, each bucket's start as the gap from the one before it, which keeps a regular series to small numbers. */
function stored(file) {
  const db = new DatabaseSync(file, { readOnly: true });
  const rows = db
    .prepare(
      'SELECT s.id, s.metric, s.labels_json AS key, s.kind, p.tier, p.bucket_ms AS at, p.value ' +
        'FROM series s JOIN series_points p ON p.series_id = s.id ORDER BY s.id, p.tier, p.bucket_ms',
    )
    .all();
  db.close();
  const out = [];
  let subject, tier, last;
  for (const row of rows) {
    if (subject?.id !== row.id) {
      subject = { id: row.id, metric: row.metric, key: row.key, counter: row.kind === 'counter', tiers: [] };
      out.push(subject);
      tier = undefined;
    }
    if (tier?.[0] !== row.tier) {
      tier = [row.tier, [], []];
      subject.tiers.push(tier);
      last = 0;
    }
    tier[1].push(Number(row.at) - last);
    tier[2].push(row.value);
    last = Number(row.at);
  }
  return out.map(({ id, ...s }) => s);
}

const answers = {};
const browser = await chromium.launch({ args: ['--no-sandbox'] });
const ctx = await browser.newContext({
  ignoreHTTPSErrors: true,
  viewport: { width: 1440, height: 1000 },
});
await ctx.addCookies([
  {
    name: cookie.slice(0, eq),
    value: cookie.slice(eq + 1),
    url: BASE,
    secure: true,
    httpOnly: true,
    sameSite: 'Strict',
  },
]);

// A page that fails to load fails the recording: a partial one would answer its missing reads as not found.
const failed = [];

/** A page that records every GET answer it reads, opened on `scope` and `branch`. */
async function recording(scope, branch) {
  const page = await ctx.newPage();
  await page.addInitScript(
    ([scope, branch]) => {
      localStorage.setItem('smeltr-scope', scope);
      localStorage.setItem('smeltr-branch', branch);
    },
    [scope, branch],
  );
  // Each answer's body is read after it arrives; closing the page waits for them, or one read last is lost.
  const reads = [];
  page.on('response', (res) => {
    const req = res.request();
    const url = new URL(res.url());
    if (req.method() !== 'GET' || !url.pathname.startsWith('/api/')) return;
    // A node's stats are the fleet's, whichever branch draws it: the demo answers them from main's.
    if (STATS.test(url.pathname) && url.searchParams.get('branch') !== 'main') return;
    reads.push(
      res.json().then(
        (body) => (answers[keyOf(res.url())] = { status: res.status(), body }),
        // A body that is not JSON (a lookup file's bytes) is not the demo's to answer.
        () => {},
      ),
    );
  });
  const close = page.close.bind(page);
  page.close = async () => {
    await Promise.all(reads);
    await close();
  };
  return page;
}

// One visit per page, scope and branch: a page's answers do not depend on the shot's steps.
const visits = new Map();
for (const s of shots) visits.set(`${s.hash}|${s.scope || 'shared'}|${s.branch || 'main'}`, s);
for (const [visit] of visits) {
  const [hash, scope, branch] = visit.split('|');
  const page = await recording(scope, branch);
  try {
    await page.goto(`${BASE}/${hash}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(1500);
  } catch (e) {
    failed.push(`${visit}: ${String(e).split('\n')[0]}`);
  }
  await page.close();
  console.log(`${visit}: ${Object.keys(answers).length} answers so far`);
}
// And everything the GUI reaches by reading, on both branches, so the demo answers each one.
for (const branch of ['main', 'work']) {
  try {
    const reached = await reachable(() => recording('shared', branch), BASE + '/');
    console.log(
      `${branch}: ${reached.length} pages reached, ${Object.keys(answers).length} answers so far`,
    );
  } catch (e) {
    failed.push(`${branch}: ${String(e).split('\n')[0]}`);
  }
}
if (failed.length) {
  await browser.close();
  throw new Error(
    `no recording written: ${failed.length} pages did not load\n${failed.join('\n')}`,
  );
}

// Every branch's config, samples and locked tests, whether or not a page opened them, and every CSV lookup table's
// text: the demo's leader holds each branch as it was and shows each table's text, and its preview service reads the
// tables from `lookups/` beside the recording.
const lookups = path.join(path.dirname(out), 'lookups');
fs.mkdirSync(lookups, { recursive: true });
const reader = await ctx.newPage();
await reader.goto(`${BASE}/#/map`, { waitUntil: 'networkidle' });
const listed = answers['/api/branches'].body.branches.map((b) => b.name);
for (const [branch, scope] of ['main', ...listed].flatMap((b) =>
  ['shared', 'network', 'apps'].map((s) => [b, s]),
)) {
  const q = `branch=${encodeURIComponent(branch)}`;
  const area = `/api/areas/${scope}`;
  // The tables come from main: the service holds one `lookups/` for every branch.
  const read = await reader.evaluate(
    async ([area, q, onMain]) => {
      const get = (u) => fetch(u).then((r) => r.json());
      const { samples = [] } = await get(`${area}/samples?${q}`);
      const each = await Promise.all(
        samples.map((s) => get(`${area}/samples/${encodeURIComponent(s.id)}?${q}`)),
      );
      const { tests = [] } = await get(`${area}/tests?${q}`);
      const locked = await Promise.all(
        tests.map((t) => get(`${area}/tests/${encodeURIComponent(t.id)}?${q}`)),
      );
      const configured = await get(`${area}?${q}`);
      const config = configured.config;
      const tables = await Promise.all(
        (onMain ? (config?.lookupTables ?? []) : [])
          .filter((t) => t.type === 'csv')
          .map(async (t) => {
            const key = `${area}/lookups/file?${q}&path=${encodeURIComponent(t.path)}`;
            return { key, path: t.path, body: await get(key) };
          }),
      );
      return { each, locked, tables, configured };
    },
    [area, q, branch === 'main'],
  );
  if (read.configured.config)
    answers[keyOf(`${area}?${q}`)] = { status: 200, body: read.configured };
  for (const answer of read.each)
    answers[keyOf(`${area}/samples/${encodeURIComponent(answer.sample.id)}?${q}`)] = {
      status: 200,
      body: answer,
    };
  for (const answer of read.locked)
    answers[keyOf(`${area}/tests/${encodeURIComponent(answer.test.id)}?${q}`)] = {
      status: 200,
      body: answer,
    };
  for (const table of read.tables) {
    answers[keyOf(table.key)] = { status: 200, body: table.body };
    fs.writeFileSync(path.join(lookups, path.basename(table.path)), table.body.text);
  }
}
// What each Parser step read in production, which the pipeline editor offers beside a sample's fields.
answers['/api/streams'] = {
  status: 200,
  body: await reader.evaluate(() => fetch('/api/streams').then((r) => r.json())),
};
await reader.close();

// The fleet moving: every live answer read again every five seconds for a minute, so the demo's Map, charts and
// counters move as the fleet's did. A config read is not live; the demo's own edits change those.
// A chart moves on the window it opens on; on any other it stands still, as recorded.
const live = Object.keys(answers).filter((k) => LIVE.test(k) && !pickedWindow(k));
const api = await ctx.newPage();
await api.goto(`${BASE}/#/map`, { waitUntil: 'networkidle' });
const moving = {};
const movingAt = [];
for (let i = 0; i < 12; i++) {
  movingAt.push(Date.now());
  const read = await api.evaluate(async (keys) => {
    const get = (u) => fetch(u, { headers: { accept: 'application/json' } }).then((r) => r.json());
    return Promise.all(keys.map(get));
  }, live);
  live.forEach((k, j) => (moving[k] ??= []).push(read[j]));
  await api.waitForTimeout(5000);
}

// What each source reads and each destination is sent: one capture of each on main, as the GUI's Capture asks for
// it, kept as the table it held. A destination's is what main's pipelines send it; a fleet's workers run those
// pipelines whatever branch is open.
const POOL = 200;
const captures = {};
const capture = (group, what) =>
  api.evaluate(
    async ([group, what, max]) => {
      const { csrf } = await fetch('/api/auth/session').then((r) => r.json());
      const send = (method, u, body) =>
        fetch(u, {
          method,
          headers: { 'Content-Type': 'application/json', 'x-smeltr-csrf': csrf },
          body: body && JSON.stringify(body),
        }).then((r) => r.json());
      const post = (u, body) => send('POST', u, body);
      const started = await post('/api/captures', {
        group,
        ...what,
        maxMatches: max,
        deadlineSecs: 30,
      });
      if (!started.ok) throw new Error(JSON.stringify(started.errors));
      for (;;) {
        await new Promise((r) => setTimeout(r, 1000));
        const { capture } = await fetch(`/api/captures/${started.session}`).then((r) => r.json());
        if (!capture.done) continue;
        // The demo's leader keeps a pool as one batch: the table the capture held.
        const { columns, rows } = await fetch(`/api/captures/${started.session}/records`).then(
          (r) => r.json(),
        );
        await send('DELETE', `/api/captures/${started.session}`);
        return { batches: [{ columns, rows }] };
      }
    },
    [group, what, POOL],
  );
for (const scope of ['network', 'apps']) {
  const { config } = answers[`/api/areas/${scope}?branch=main`].body;
  for (const source of config.sources) {
    const pool = await capture(scope, { source });
    captures[`${scope}/source/${source.id}`] = pool;
    console.log(`${scope}/${source.id}: ${pool.batches[0].rows.length} records captured`);
  }
  for (const { id } of config.destinations) {
    const pool = await capture(scope, { destination: id });
    captures[`${scope}/destination/${id}`] = pool;
    console.log(`${scope}/${id}: ${pool.batches[0].rows.length} records captured`);
  }
}

// The demo answers a branch it holds no answer on with main's, so an answer on `work` that is main's is left out.
for (const key of Object.keys(answers)) {
  const url = new URL(key, BASE);
  if (url.searchParams.get('branch') !== 'work') continue;
  url.searchParams.set('branch', 'main');
  const onMain = answers[keyOf(url.href)];
  if (onMain && JSON.stringify(onMain) === JSON.stringify(answers[key])) delete answers[key];
}
// The demo's visitor signs nothing: a session's token is the staged fleet's alone.
delete answers['/api/auth/session'].body.csrf;
// The demo's leader answers the series reads itself, from every point the leader stored (`series.ts`), so a visitor
// can ask what nobody recorded. One read of each shape the GUI made is asked again, beside the points, for
// `series.test.ts` to hold those answers to the leader's.
const askedSeries = Object.keys(answers).filter((k) => new URL(k, BASE).pathname.startsWith('/api/series'));
const shapes = new Map();
for (const key of askedSeries) {
  const url = new URL(key, BASE);
  const q = url.searchParams;
  const shape = [url.pathname, !!q.get('filter'), !!q.get('by')]
    .concat(['windowMinutes', 'draw', 'agoMinutes', 'utcOffsetMinutes'].map((n) => q.get(n)))
    .join('|');
  if (!shapes.has(shape)) shapes.set(shape, key);
}
// And the shapes only the Metrics page's own pickers ask: a split, a gauge, the reader's clock and a week before.
for (const q of [
  'metric=smeltr_rows_polled_total&by=source&draw=perSecond&windowMinutes=60',
  'metric=smeltr_rows_polled_total&by=worker&filter=group:network&draw=totals&windowMinutes=1440&utcOffsetMinutes=-300',
  'metric=smeltr_rows_polled_total&draw=totals&windowMinutes=10080&utcOffsetMinutes=330',
  'metric=smeltr_rows_polled_total&draw=perSecond&windowMinutes=60&agoMinutes=10080',
  'metric=smeltr_source_lag&by=source&windowMinutes=10080',
])
  shapes.set(q, `/api/series?${q}`);
const checks = await api.evaluate(
  (keys) =>
    Promise.all(
      keys.map(async (key) => {
        const askedAt = Date.now();
        const body = await fetch(key, { headers: { accept: 'application/json' } }).then((r) => r.json());
        return { key, askedAt, body };
      }),
    ),
  [...shapes.values()],
);
const series = {
  series: stored(path.join(DIR, 'state', 'leader', 'smeltr-leader-data', 'registry.db')),
  checks,
};
for (const key of askedSeries) delete answers[key];
console.log(`${series.series.length} series stored, ${checks.length} answers to check them by`);
// A picked answer stands still and is moved to the visitor's now from when the recording was written, so it is read
// again last: one read while the pages were reached would end that long before now.
const still = Object.keys(answers).filter(pickedWindow);
const reread = await api.evaluate(
  (keys) =>
    Promise.all(
      keys.map((u) =>
        fetch(u, { headers: { accept: 'application/json' } }).then(async (r) => ({
          status: r.status,
          body: await r.json(),
        })),
      ),
    ),
  still,
);
still.forEach((k, j) => (answers[k] = reread[j]));
// What every recorded time is relative to: the demo moves them to the visitor's now.
const recordedAt = Date.now();
const parts = await write(out, { recordedAt, answers, moving, movingAt, captures, series });
console.log(`${Object.keys(answers).length} answers written to ${out}, ${parts} parts beside it`);
await browser.close();
