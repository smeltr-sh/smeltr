// Everything a visitor can reach in the GUI by reading, never by changing anything: every page a link in the app
// leads to, and on each page every choice of a picker that changes nothing but the page itself. The demo's recorder
// records what this reaches, and `tests/gui/specs/demo.spec.ts` checks the demo answers every read it makes, so a
// read the recording cannot answer fails that test until the recording is made again.

/** Pickers whose every choice is visited. A `Segmented` holds a page's or a form's own state; the chart range picker
 * moves the link, so each range is a page too, and is opened before its choices show. */
const SEGMENTED = '.seg[role=group] button:not(:disabled)';
const OPENED = [{ face: 'button[aria-label="Chart range"]', choice: '[popover] button.pick' }];

/** How long a page must say nothing is loading before it counts as drawn. */
const QUIET_MS = 1500;

/** How long a page may take to load. A month's chart range reads every node's month of history at once, which a staged
 * fleet's leader can take longer than Playwright's default 30 s to answer. */
const LOAD_MS = 120_000;

/** Pages visited at once. */
const AT_ONCE = 4;

/** Every page reachable through a link from `#/status` in the GUI at `base`. Each is visited in a page `open` makes
 * for it, every choice of its pickers is made in turn, and the page is closed once its reads have settled, so the
 * caller sees every read it makes. */
export async function reachable(open, base) {
  const seen = new Set(['#/status']);
  const queue = ['#/status'];
  let busy = 0;
  const visit = async (hash) => {
    const page = await open(hash);
    try {
      await page.goto(`${base}${hash}`, { waitUntil: 'networkidle', timeout: LOAD_MS });
      await settle(page);
      const links = await page.$$eval('a[href^="#/"]', (as) =>
        as.map((a) => a.getAttribute('href')),
      );
      for (const link of links) {
        if (seen.has(link)) continue;
        seen.add(link);
        queue.push(link);
      }
      const choices = await page.locator(SEGMENTED).count();
      for (let i = 0; i < choices; i++) {
        await page.locator(SEGMENTED).nth(i).click();
        await settle(page);
      }
      for (const { face, choice } of OPENED) {
        if (!(await page.locator(face).count())) continue;
        await page.locator(face).click();
        const count = await page.locator(choice).count();
        for (let i = 0; i < count; i++) {
          if (i) await page.locator(face).click();
          await page.locator(choice).nth(i).click();
          await settle(page);
          seen.add(await page.evaluate(() => location.hash));
        }
      }
    } finally {
      await page.close();
    }
  };
  // Each worker takes the next page until none is left and none is being visited, which could find more.
  const worker = async () => {
    for (;;) {
      const hash = queue.shift();
      if (hash === undefined) {
        if (!busy) return;
        await new Promise((r) => setTimeout(r, 100));
        continue;
      }
      busy++;
      try {
        await visit(hash);
      } finally {
        busy--;
      }
    }
  };
  await Promise.all(Array.from({ length: AT_ONCE }, worker));
  return [...seen];
}

/** Until the page's reads have answered and it has drawn them: the GUI says it is still loading in these words, and
 * may start saying so a moment after its first reads settle, so it must stay quiet for a while. */
async function settle(page) {
  await page.waitForLoadState('networkidle');
  // The demo's strip says it is waking until it can answer what only its service can.
  const loading = page.getByText(/^(Loading…|Reading…|Waking the demo up\.)/);
  for (let quiet = 0, waited = 0; quiet < QUIET_MS && waited < 60_000; waited += 250) {
    quiet = (await loading.count()) ? 0 : quiet + 250;
    await page.waitForTimeout(250);
  }
}
