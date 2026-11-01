// Capture GUI pages as a signed-in administrator, once per theme.
//
//   node shoot.mjs shots.json [name-regex]
//
// Each shot names a route, the scope and branch the GUI opens on, its window, and the steps that bring the page
// to the state worth showing. PNGs land in $OUT, or ./shots. A shot with `site` also replaces the website's
// image of that name, `website/static/<site>-<theme>.png`, so the site's pages, and the crops they show, keep
// pointing at the same state of the same page. A shot with `card` captures that one step's card instead of the
// window.
import { chromium } from 'playwright';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const BASE = 'https://127.0.0.1:8443';
const DIR = path.dirname(fileURLToPath(import.meta.url));
const OUT = process.env.OUT || path.join(DIR, 'shots');
const SITE = path.join(DIR, '..', '..', 'website', 'static');
fs.mkdirSync(OUT, { recursive: true });

const spec = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const only = process.argv[3] ? new RegExp(process.argv[3]) : null;
// The session `stage.py` signed in with, so capturing costs the leader no sign-ins.
const sessions = JSON.parse(fs.readFileSync(path.join(DIR, 'state', 'sessions.json'), 'utf8'));
const cookie = sessions.admin[0];
const eq = cookie.indexOf('=');

// Run as root, as in a container, Chromium refuses to start sandboxed.
const browser = await chromium.launch({ args: ['--no-sandbox', '--font-render-hinting=none'] });
let failed = 0;

// The k-th pipeline step's card, an If step's branches counted after it, in page order.
function cardOf(page, k) {
  return page.locator('.card').nth(k);
}

async function step(page, st) {
  if (st.click) await page.click(st.click, { timeout: 10000 });
  if (st.text) await page.getByText(st.text, { exact: true }).first().click();
  if (st.select) await page.selectOption(st.select[0], st.select[1]);
  if (st.fill) await page.fill(st.fill[0], st.fill[1]);
  // A key on the control just filled, such as Enter to make a chip of it.
  if (st.press) await page.keyboard.press(st.press);
  // Off the control just filled, so no focus ring lands in the shot.
  if (st.blur) await page.evaluate(() => document.activeElement?.blur());
  // The k-th pipeline step's card: open its editor, or show what that step alone changed.
  if (st.card !== undefined) {
    const card = cardOf(page, st.card);
    // An If step has no editor to open; its branches' cards are its own.
    const head = card.locator(':scope > .card-head');
    if (st.expand && (await head.locator('button.expander').count()))
      await head.locator('button.expander').click();
    if (st.diff) await head.locator('button[aria-label^="Show this step"]').click();
  }
  // Drag the element's center across to x.
  if (st.drag) {
    const [selector, x] = st.drag;
    const box = await page.locator(selector).first().boundingBox();
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await page.mouse.move(x, box.y + box.height / 2, { steps: 12 });
    await page.mouse.up();
  }
  // Every match, one at a time: a click can take an element out of the match (an opened card), so the first
  // match is always the next one.
  if (st.clickAll) {
    for (let i = 0; i < 50 && (await page.locator(st.clickAll).count()) > 0; i++) {
      await page.locator(st.clickAll).first().click();
      await page.waitForTimeout(200);
    }
  }
  // The editor's output table, cut to these columns.
  if (st.keepColumns) {
    await page.click('button[aria-label="Choose columns"]');
    await page.waitForTimeout(300);
    const picks = page.locator('.column-list button.pick');
    for (let i = 0, n = await picks.count(); i < n; i++) {
      const pick = picks.nth(i);
      const name = (await pick.locator('.mono').innerText()).trim();
      const shown = (await pick.getAttribute('aria-pressed')) === 'true';
      if (shown !== st.keepColumns.includes(name)) await pick.click();
    }
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);
  }
  // A state the page reaches in its own time, such as a capture finishing.
  if (st.wait) await page.waitForSelector(st.wait, { timeout: 60000 });
  if (st.delay) await page.waitForTimeout(st.delay);
}

for (const s of spec) {
  if (only && !only.test(s.name)) continue;
  for (const theme of s.themes) {
    const ctx = await browser.newContext({
      ignoreHTTPSErrors: true,
      viewport: { width: s.width || 1440, height: s.height || 900 },
      deviceScaleFactor: s.scale || 2,
      colorScheme: theme,
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
    // What the GUI keeps per browser: the theme, and the scope and branch it opens on.
    await ctx.addInitScript(
      ([t, scope, branch]) => {
        localStorage.setItem('smeltr-theme', t);
        localStorage.setItem('smeltr-scope', scope);
        localStorage.setItem('smeltr-branch', branch);
      },
      [theme, s.scope || 'shared', s.branch || 'main'],
    );
    const page = await ctx.newPage();
    const errors = [];
    page.on('pageerror', (e) => errors.push(String(e)));
    page.on('console', (m) => m.type() === 'error' && errors.push(m.text()));
    const file = path.join(OUT, `${s.name}-${theme}.png`);
    try {
      await page.goto(`${BASE}/${s.hash}`, { waitUntil: 'networkidle' });
      if (s.wait) await page.waitForSelector(s.wait, { timeout: 20000 });
      for (const st of s.steps || []) await step(page, st);
      await page.waitForTimeout(s.delay ?? 1500);
      // Off every control, so no hover state lands in the shot.
      await page.mouse.move(1, 1);
      await page.waitForTimeout(300);
      // A shot with `card` is that step's card alone.
      if (s.card === undefined) await page.screenshot({ path: file, fullPage: !!s.full, clip: s.clip });
      else await cardOf(page, s.card).screenshot({ path: file });
      if (s.site) {
        const to = path.join(SITE, `${s.site}-${theme}.png`);
        fs.mkdirSync(path.dirname(to), { recursive: true });
        fs.copyFileSync(file, to);
      }
      console.log(
        path.basename(file) + (errors.length ? `  console: ${errors.slice(0, 3).join(' | ')}` : ''),
      );
    } catch (e) {
      console.log(`${path.basename(file)}  FAILED: ${String(e).split('\n')[0]}`);
      failed++;
    }
    await ctx.close();
  }
}
await browser.close();
if (failed) process.exitCode = 1;
