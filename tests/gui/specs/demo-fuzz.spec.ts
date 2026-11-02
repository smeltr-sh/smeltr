// Everything a visitor can reach in the website demo (`tools/screenshots/reach.mjs`), each read checked against the
// recording. It takes minutes, so it runs with the fuzzers (`just fuzz-demo`) rather than in `check-gui`.
import { test, expect } from '@playwright/test';
import { startDemo, type Demo } from '../demo';
import { reachable } from '../../../tools/screenshots/reach.mjs';

let demo: Demo;
let base: string;

test.beforeAll(async () => {
  demo = await startDemo();
  base = demo.base;
});

test.afterAll(() => demo?.stop());

// A page that reads what the recording does not hold shows "not found": `just demo-fixtures` records it again.
test('the recording answers every read of everything a visitor reaches', async ({ browser }) => {
  // It opens every page a link reaches and makes every choice on it, which passes the default by far.
  test.setTimeout(900_000);
  const unrecorded = new Set<string>();
  const open = async (hash: string) => {
    const page = await browser.newPage();
    // The demo's leader answers in the page, so its answers are read where it is installed.
    await page.addInitScript(() => {
      const native = window.fetch.bind(window);
      let fetch = native;
      Object.defineProperty(window, 'fetch', {
        get: () => async (input: RequestInfo | URL, init?: RequestInit) => {
          // The leader passes on what is not its own through the fetch it found, which is this one.
          const href = input instanceof Request ? input.url : String(input);
          if (!new URL(href, location.href).pathname.startsWith('/api/'))
            return native(input, init);
          const answer = await fetch(input, init);
          if (answer.status === 404) {
            const body = await answer
              .clone()
              .json()
              .catch(() => null);
            if (body?.errors?.[0]?.message === 'not found')
              console.log(`unrecorded ${input instanceof Request ? input.url : String(input)}`);
          }
          return answer;
        },
        set: (installed) => (fetch = installed),
      });
    });
    page.on('console', (m) => {
      if (m.text().startsWith('unrecorded ')) unrecorded.add(`${hash}: ${m.text().slice(11)}`);
    });
    return page;
  };
  const pages = await reachable(open, base);
  expect(pages).toContain('#/status/issues');
  expect(pages.filter((p) => p.includes('range=7d')).length).toBeGreaterThan(0);
  expect([...unrecorded]).toEqual([]);
});
