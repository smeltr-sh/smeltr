// Record what the staged fleet's leader answers, for the website demo's leader to answer in its place.
//
//   node demo.mjs ../../gui-frontend/src/demo/recorded.json
//
// Opens every page `shots.json` names, as the signed-in administrator, and keeps the last answer to every GET under
// `/api/`, keyed by its path and sorted query. Then reads the live answers over a minute, so the demo's fleet moves
// as the staged one did, and captures what each of main's sources reads and destinations are sent, so a capture
// in the demo draws from what the fleet did. `stage.py` must have staged the fleet first.
import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

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

/** The branch the capture pools are saved on: the recorder's, not the fleet's. */
const POOLS = 'demo-capture-pools';
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

// One visit per page, scope and branch: a page's answers do not depend on the shot's steps.
const visits = new Map();
for (const s of shots) visits.set(`${s.hash}|${s.scope || 'shared'}|${s.branch || 'main'}`, s);
for (const extra of [
  '#/status/issues',
  '#/map',
  '#/build/samples',
  '#/admin/groups',
  '#/admin/enrollment',
  '#/admin/secrets',
  '#/admin/accounts',
  '#/admin/accounts/roles',
  '#/admin/system/general',
  '#/admin/system/sessions',
  '#/admin/accounts/oidc',
  '#/admin/system/certificates',
  '#/admin/system/git',
  '#/admin/system/license',
  '#/changes',
]) {
  for (const branch of ['main', 'work']) visits.set(`${extra}|shared|${branch}`, { hash: extra });
}

for (const [visit] of visits) {
  const [hash, scope, branch] = visit.split('|');
  const page = await ctx.newPage();
  await page.addInitScript(
    ([scope, branch]) => {
      localStorage.setItem('smeltr-scope', scope);
      localStorage.setItem('smeltr-branch', branch);
    },
    [scope, branch],
  );
  page.on('response', async (res) => {
    const req = res.request();
    if (req.method() !== 'GET' || !new URL(res.url()).pathname.startsWith('/api/')) return;
    try {
      answers[keyOf(res.url())] = { status: res.status(), body: await res.json() };
    } catch {
      // A body that is not JSON (a lookup file's bytes) is not the demo's to answer.
    }
  });
  try {
    await page.goto(`${BASE}/${hash}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(1500);
  } catch (e) {
    failed.push(`${visit}: ${String(e).split('\n')[0]}`);
  }
  await page.close();
  console.log(`${visit}: ${Object.keys(answers).length} answers so far`);
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
const listed = answers['/api/branches'].body.branches.map((b) => b.name).filter((b) => b !== POOLS);
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
// Every object's panel on the Map, whether or not a page opened it. The demo answers other branches from main's.
for (const node of answers['/api/map?branch=main'].body.nodes) {
  const { group, kind, authorId: id } = node;
  const q = new URLSearchParams({ branch: 'main', kind, id, windowMinutes: '10' });
  const key = keyOf(`/api/areas/${group}/stats?${q}`);
  answers[key] = {
    status: 200,
    body: await reader.evaluate((u) => fetch(u).then((r) => r.json()), key),
  };
}
await reader.close();

// The fleet moving: every live answer read again every five seconds for a minute, so the demo's Map, charts and
// counters move as the fleet's did. A config read is not live; the demo's own edits change those.
const LIVE =
  /^\/api\/(map\/measurements|areas\/[^/?]+\/stats|flow|cluster|workers|warnings|streams)\b/;
const live = Object.keys(answers).filter((k) => LIVE.test(k));
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
// it. Each is kept both as the capture showed it and as the sample a save made of it, row for record; the saves land
// on a branch of their own. A destination's is what main's pipelines send it, which is what a fleet's workers run
// whatever branch is open. One that caught nothing has no sample.
const POOL = 200;
const captures = {};
const capture = (group, what, branch) =>
  api.evaluate(
    async ([group, what, max, branch]) => {
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
        if (capture.batches.every((b) => !b.rows.length)) {
          await send('DELETE', `/api/captures/${started.session}`);
          return { batches: capture.batches };
        }
        const id = `pool-${what.source?.id ?? what.destination}-${Date.now()}`;
        const samples = `/api/areas/${group}/samples`;
        const saved = await post(`${samples}?branch=${branch}`, {
          capture: started.session,
          id,
          name: id,
          source: what.source?.id,
        });
        if (!saved.ok) throw new Error(JSON.stringify(saved.errors));
        const { sample } = await fetch(`${samples}/${id}?branch=${branch}`).then((r) => r.json());
        return {
          batches: capture.batches,
          sample: { columns: sample.columns, records: sample.records },
        };
      }
    },
    [group, what, POOL, branch],
  );
for (const scope of ['network', 'apps']) {
  const { config } = answers[`/api/areas/${scope}?branch=main`].body;
  for (const source of config.sources) {
    const pool = await capture(scope, { source }, POOLS);
    captures[`${scope}/source/${source.id}`] = pool;
    console.log(`${scope}/${source.id}: ${pool.sample?.records.length ?? 0} records captured`);
  }
  for (const { id } of config.destinations) {
    const pool = await capture(scope, { destination: id }, POOLS);
    captures[`${scope}/destination/${id}`] = pool;
    console.log(
      `${scope}/${id}: ${pool.batches.reduce((n, b) => n + b.rows.length, 0)} records captured`,
    );
  }
}

// The branch the pools were saved on is the recorder's, not the fleet's.
const branches = answers['/api/branches'].body;
branches.branches = branches.branches.filter((b) => b.name !== POOLS);
// The demo's visitor signs nothing: a session's token is the staged fleet's alone.
delete answers['/api/auth/session'].body.csrf;
// What every recorded time is relative to: the demo moves them to the visitor's now.
const recordedAt = Date.now();
// A version names the branch that built the fleet; the public demo shows it as built from main.
const recorded = JSON.stringify({ recordedAt, answers, moving, movingAt, captures });
fs.writeFileSync(
  out,
  recorded.replace(/(\d{4}\.\d+\.\d+\+)[A-Za-z0-9-]+\.([0-9a-f]{12})/g, '$1main.$2'),
);
console.log(`${Object.keys(answers).length} answers written to ${out}`);
await browser.close();
