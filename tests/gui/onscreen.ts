// What every walk through the GUI must leave true, whatever it is doing: a fuzzer checks it after each step. A
// person sees nothing broken: the page throws nothing and logs no error, the leader answers nothing with a 5xx,
// nothing is locked that they may change, and every control that looks usable is what a click at its center
// reaches.
import type { Page } from '@playwright/test';
import { settled, type Leader } from './fleet';

export const CLICKABLE =
  'a[href], button, summary, [role="button"], [role="tab"], [role="menuitem"], input[type="checkbox"], input[type="radio"]';
export const TYPEABLE =
  'input:not([type]), input[type="text"], input[type="search"], input[type="password"], input[type="email"], input[type="url"], input[type="number"], textarea';

/** Collects into `problems` everything `page` and `leader` complain of from now on. */
export function listen(page: Page, leader: Leader, problems: string[]) {
  page.on('pageerror', (e) => problems.push(`page threw: ${e.stack ?? e.message}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    // A refusal is the leader doing its job; the browser logs each one. Anything else missing is broken.
    const refused = /Failed to load resource: the server responded with a status of 4\d\d/.test(
      m.text(),
    );
    if (refused && m.location().url.startsWith(`${leader.url}/api/`)) return;
    problems.push(`console error: ${m.text()}`);
  });
  page.on('response', (r) => {
    if (r.url().startsWith(leader.url) && r.status() >= 500)
      problems.push(`${r.request().method()} ${new URL(r.url()).pathname} -> ${r.status()}`);
  });
}

/** Which screen `page` shows: one of those shown in place of a page, or the app. */
export async function screen(page: Page): Promise<'sign-in' | 'choose-password' | 'app' | 'other'> {
  if (await page.getByRole('heading', { name: 'Sign in to smeltr' }).isVisible()) return 'sign-in';
  if (await page.getByRole('heading', { name: 'Choose your own password' }).isVisible())
    return 'choose-password';
  if (await page.getByRole('navigation').first().isVisible()) return 'app';
  return 'other';
}

/** What is wrong with what `page` shows once it has settled. `mayLock` is whether the person may find sections
 * locked here: a viewer may, in the app. The page must be watched (`watchRequests`). */
export async function onScreen(page: Page, mayLock: boolean): Promise<string[]> {
  const problems: string[] = [];
  // A page that never stops asking is a finding too, so this is waited on rather than skipped.
  await settled(page);
  const locked = await page.locator('fieldset.read-only').count();
  if (locked > 0 && !mayLock)
    problems.push(`${locked} locked section(s) on the ${await screen(page)} screen`);

  const covered = await page.evaluate((selector) => {
    const name = (el: Element) => {
      const text = (el.getAttribute('aria-label') ?? el.textContent ?? '').trim().slice(0, 40);
      return `<${el.tagName.toLowerCase()}${el.className ? ` class="${el.className}"` : ''}> "${text}"`;
    };
    const layer =
      document.querySelector('dialog[open]') ??
      [...document.querySelectorAll('[popover]')].find((p) => p.matches(':popover-open'));
    const menuOpen = document.querySelector('[aria-expanded="true"]') !== null;
    const found: string[] = [];
    for (const el of document.querySelectorAll(selector)) {
      const e = el as HTMLElement & { disabled?: boolean };
      if (layer && !layer.contains(e)) continue;
      if (e.disabled || e.matches(':disabled') || e.closest('[inert]')) continue;
      // What a closed <details> holds is laid out but not drawn, so it is neither covered nor clickable.
      if (e.closest('details:not([open]) > :not(summary)')) continue;
      const style = getComputedStyle(e);
      if (style.visibility !== 'visible' || style.pointerEvents === 'none') continue;
      // A link that wraps has a box per line, and the middle of the box round them all may fall in neither.
      const r = [...e.getClientRects()].find((b) => b.width >= 2 && b.height >= 2);
      if (!r) continue;
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      if (x < 0 || y < 0 || x >= innerWidth || y >= innerHeight) continue;
      // Past the edge of a box that clips it. One that scrolls has scrolled it away, and a person scrolls to it, past
      // any box round that one; one that does not has cut it off, where no one reaches it. The clip is the box's
      // padding edge, inside its border.
      const scrolls = (overflow: string) => overflow === 'auto' || overflow === 'scroll';
      let [scrolledX, scrolledY] = [false, false];
      let cut: Element | undefined;
      for (let a = e.parentElement; a && !cut; a = a.parentElement) {
        const o = getComputedStyle(a);
        if (o.overflowX === 'visible' && o.overflowY === 'visible') continue;
        const b = a.getBoundingClientRect();
        const left = b.left + a.clientLeft;
        const top = b.top + a.clientTop;
        const outX = !scrolledX && (x < left || x > left + a.clientWidth);
        const outY = !scrolledY && (y < top || y > top + a.clientHeight);
        if ((outX && !scrolls(o.overflowX)) || (outY && !scrolls(o.overflowY))) cut = a;
        scrolledX ||= outX;
        scrolledY ||= outY;
      }
      if (cut) {
        found.push(`cut off: ${name(e)} is past the edge of ${name(cut)}`);
        continue;
      }
      if (scrolledX || scrolledY) continue;
      const hit = document.elementFromPoint(x, y);
      if (!hit || e === hit || e.contains(hit)) continue;
      const labels = (e as HTMLInputElement).labels;
      if (labels && [...labels].some((l) => l.contains(hit))) continue;
      // Scrolled under the page's own pinned header or footer, where a person scrolls to reach it.
      let pinned = false;
      for (let a: Element | null = hit; a; a = a.parentElement) {
        const p = getComputedStyle(a).position;
        if ((p === 'fixed' || p === 'sticky') && !a.contains(e)) pinned = true;
      }
      if (pinned && !layer) continue;
      // Under a menu the person opened, which floats over the page until they close it.
      if (menuOpen) {
        let floating = false;
        for (let a: Element | null = hit; a; a = a.parentElement) {
          const st = getComputedStyle(a);
          if (st.position === 'absolute' && st.zIndex !== 'auto' && !a.contains(e)) floating = true;
        }
        if (floating) continue;
      }
      found.push(`covered: ${name(e)} is under ${name(hit)}`);
    }
    return found;
  }, `${CLICKABLE}, ${TYPEABLE}, select`);
  problems.push(...covered);
  return problems;
}
