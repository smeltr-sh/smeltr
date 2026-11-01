// Every page the GUI's own navigation reaches: its tabs, each section's rail and each page's secondary tabs. The demo's
// recorder records these pages, and `tests/gui/specs/demo.spec.ts` checks the demo answers every read they make, so a
// page the recording cannot answer fails that test until the recording is made again.

/** Every page reachable through a `nav` link from `#/status` in the GUI at `base`. Each is visited in a page `open`
 * makes for it, and closed once its reads have settled, so the caller sees every read it makes. */
export async function navPages(open, base) {
  const seen = new Set();
  const queue = ['#/status'];
  while (queue.length) {
    const hash = queue.shift();
    if (seen.has(hash)) continue;
    seen.add(hash);
    const page = await open(hash);
    await page.goto(`${base}${hash}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(1000);
    const links = await page.$$eval('nav a[href^="#/"]', (as) =>
      as.map((a) => a.getAttribute('href')),
    );
    queue.push(...links.filter((h) => !seen.has(h)));
    await page.close();
  }
  return [...seen];
}
