// The GUI fuzzer. Each seed is one person on a leader of their own, starting on a page with a theme and a window
// size the seed draws, then walking the GUI by what is on screen: clicking, typing, choosing, going back,
// reloading, losing the session. After every step, what a person sees must work:
//
// - no control is locked except on a viewer's page in the app;
// - every control that looks usable is the topmost thing at its own center, so a click reaches it;
// - a field clicked and typed into takes the focus and the text;
// - the page throws nothing, logs no error, and the leader answers nothing with a 5xx.
//
// A failure names its seed and the steps that led there. `SMELTR_FUZZ_SEED=n` replays one; `SMELTR_FUZZ_SEEDS=n`
// runs `n`. `gui-fuzz-coverage.spec.ts` then checks that the seeds reached every option below.
import fs from 'node:fs';
import path from 'node:path';
import type { Browser, Page } from '@playwright/test';
import {
  test,
  expect,
  claimAdmin,
  settled,
  signInThroughGui,
  watchRequests,
  ADMIN_PASSWORD,
  type Api,
  type Leader,
} from '../fleet';
import { Rng, seeds } from '../fuzz';
import { ACTIONS, DEFAULT_SEEDS, THEMES, VIEWPORTS, WHO, type Action, type Who } from '../gui-fuzz';
import { CLIENT_ID, CLIENT_SECRET, Provider } from '../oidc';
import { CLICKABLE, TYPEABLE, listen, onScreen, screen } from '../onscreen';
import { RUN_DIR } from '../run';

/** Where a walk may start before it has seen a link: each tab, the authoring pages behind the scope chip, and what
 * belongs to the person, behind their name. */
const STARTS = [
  '#/status',
  '#/status/logs',
  '#/map',
  '#/build',
  '#/build/sources',
  '#/build/destinations',
  '#/build/pipelines',
  '#/build/routes',
  '#/build/lookups',
  '#/changes',
  '#/admin',
  '#/admin/system/general',
  '#/admin/system/agents',
  '#/me/agent-permissions',
  '#/me/agent',
];

const STEPS = 12;

/** What gets typed. Each has a letter, so a text field that takes it changes. */
const TEXT = [
  'abc',
  'Hello world',
  'ünïcødé x',
  '  spaced  ',
  '<b>x</b>',
  "'; drop table x",
  'a'.repeat(300),
];

const PROVIDER_GROUP = 'smeltr-people';
const ROLE_OF: Partial<Record<Who, string>> = {
  viewer: 'viewer',
  editor: 'editor',
  deployer: 'deployer',
};

type Walk = {
  seed: number;
  rng: Rng;
  who: Who;
  page: Page;
  leader: Leader;
  /** The password a local sign-in uses now. */
  password: string;
  steps: string[];
  problems: string[];
  links: Set<string>;
  signIns: number;
};

/** Configures `leader` to sign people in through `provider`, with a role giving its group `level` in `shared`. */
async function offerSso(api: Api, provider: Provider, level: string) {
  await api.put('/api/settings/oidc', {
    enabled: true,
    issuer: provider.base,
    clientId: CLIENT_ID,
    clientSecret: CLIENT_SECRET,
    requiredGroup: PROVIDER_GROUP,
  });
  await api.put(`/api/roles/${PROVIDER_GROUP}`, { administrator: false, areas: { shared: level } });
}

/** Signs in as the seed's person by whichever way the screen offers, or chooses their password. */
async function signIn(walk: Walk) {
  const { page, who, leader } = walk;
  const on = await screen(page);
  if (on === 'choose-password') {
    const chosen = `fuzz-chosen-${walk.seed}`;
    for (const label of ['New password', 'Confirm new password']) {
      const field = page.getByLabel(label, { exact: true });
      await field.click();
      await field.fill(chosen);
    }
    await page.getByRole('button', { name: 'Change password' }).click();
    walk.password = chosen;
    return;
  }
  if (on !== 'sign-in') return;
  walk.signIns++;
  if (ROLE_OF[who]) {
    // A click may have gone on to the password form, which has its way back to the choice.
    const back = page.getByRole('button', { name: 'Back' });
    if (await back.isVisible()) await back.click();
    await page.getByRole('link', { name: 'Sign in with SSO' }).click();
    return;
  }
  const byUsername = page.getByRole('button', { name: 'Sign in with username' });
  if (await byUsername.isVisible()) await byUsername.click();
  // Too many attempts hold the button until the wait is over; the walk goes on without signing in.
  if (await page.getByRole('button', { name: 'Sign in' }).isDisabled()) return;
  await signInThroughGui(page, leader.username, walk.password);
}

/** The controls of the layer a person can reach now: an open dialog or popover if there is one, else the page. */
async function controls(page: Page, selector: string): Promise<number[]> {
  return page.locator(selector).evaluateAll((els) => {
    const layer =
      document.querySelector('dialog[open]') ??
      [...document.querySelectorAll('[popover]')].find((p) => p.matches(':popover-open'));
    return els.flatMap((el, i) => {
      const e = el as HTMLElement & { disabled?: boolean; readOnly?: boolean };
      if (layer && !layer.contains(e)) return [];
      if (e.disabled || e.matches(':disabled') || e.readOnly || e.closest('[inert]')) return [];
      // What a closed <details> holds is laid out but not drawn, so it is neither covered nor clickable.
      if (e.closest('details:not([open]) > :not(summary)')) return [];
      if (e.getAttribute('aria-disabled') === 'true') return [];
      const r = e.getBoundingClientRect();
      if (r.width < 2 || r.height < 2 || getComputedStyle(e).visibility !== 'visible') return [];
      // A link out of the GUI leaves what is being fuzzed.
      const href = e.getAttribute('href');
      if (href && !href.startsWith('#') && !href.startsWith('/api/auth/')) return [];
      if (e.hasAttribute('download') || e.getAttribute('target') === '_blank') return [];
      return [i];
    });
  });
}

/** The checks every step must leave true. */
async function check(walk: Walk) {
  const { page, who } = walk;
  // Settled first: until the app has loaded, the screen reads as no screen, and a viewer's locks as a finding.
  await settled(page);
  const mayLock = (await screen(page)) === 'app' && who === 'viewer';
  walk.problems.push(...(await onScreen(page, mayLock)));
  for (const href of await page
    .locator('a[href^="#/"]')
    .evaluateAll((as) => as.map((a) => a.getAttribute('href')!)))
    walk.links.add(href.split('?')[0]);
}

/** One step, drawn from what is on screen. Returns what it did. */
async function step(walk: Walk, action: Action): Promise<string> {
  const { page, rng, leader } = walk;
  switch (action) {
    case 'click': {
      const all = page.locator(CLICKABLE);
      const usable = await controls(page, CLICKABLE);
      const i = rng.index(usable.length);
      if (i === undefined) return 'click: nothing to click';
      const target = all.nth(usable[i]);
      const label = await describe(target);
      try {
        await target.click({ timeout: 5_000 });
      } catch (e) {
        const why = String(e);
        if (/intercepts pointer events/.test(why))
          walk.problems.push(
            `click on ${label} was taken by another element: ${why.split('\n')[0]}`,
          );
        else if (!/detached|not visible|Timeout/.test(why)) throw e;
      }
      return `click ${label}`;
    }
    case 'type': {
      const all = page.locator(TYPEABLE);
      const usable = await controls(page, TYPEABLE);
      const i = rng.index(usable.length);
      const [number, word] = [rng.below(1000), rng.pick(TEXT)];
      if (i === undefined) return 'type: nothing to type in';
      const target = all.nth(usable[i]);
      const label = await describe(target);
      const numeric = (await target.getAttribute('type')) === 'number';
      const text = numeric ? String(number) : word;
      try {
        await target.click({ timeout: 5_000 });
      } catch (e) {
        if (/intercepts pointer events/.test(String(e)))
          walk.problems.push(`click into ${label} was taken by another element`);
        return `type: ${label} could not be clicked`;
      }
      // The click may have taken the field away -- a row it closed -- and one gone is not judged.
      const before = await target.inputValue({ timeout: 1_000 }).catch(() => null);
      if (before === null) return `type: ${label} went when clicked`;
      await page.keyboard.press('ControlOrMeta+a');
      await page.keyboard.type(text, { delay: 0 });
      const typed = await target
        .evaluate(
          (el: HTMLInputElement) => ({ focused: document.activeElement === el, after: el.value }),
          null,
          {
            timeout: 1_000,
          },
        )
        .catch(() => null);
      if (typed && !typed.focused && typed.after === before)
        walk.problems.push(`${label} took neither the focus nor "${text.slice(0, 20)}"`);
      return `type "${text.slice(0, 20)}" into ${label}`;
    }
    case 'choose': {
      const all = page.locator('select');
      const usable = await controls(page, 'select');
      const i = rng.index(usable.length);
      const target = i === undefined ? undefined : all.nth(usable[i]);
      const options = target
        ? await target
            .locator('option')
            .evaluateAll((os) =>
              os.flatMap((o, i) => ((o as HTMLOptionElement).disabled ? [] : [i])),
            )
        : [];
      const o = rng.index(options.length);
      if (target === undefined) return 'choose: nothing to choose';
      if (o === undefined) return 'choose: no option';
      const index = options[o];
      await target.selectOption({ index }, { timeout: 5_000 }).catch(async (e) => {
        // A choice can redraw the page and take the select with it; a select still there refused the choice.
        if (await target.isVisible()) walk.problems.push(`choose option ${index}: ${e.message}`);
      });
      return `choose option ${index} of ${await describe(target)}`;
    }
    case 'go': {
      const hash = rng.pick([...walk.links, ...STARTS].sort());
      // A fresh load draws every screen anew; a change of hash keeps the shell and swaps the page.
      if (rng.chance(2)) {
        await page.goto('about:blank');
        await page.goto(`${leader.url}/${hash}`);
        return `load ${hash}`;
      }
      await page.evaluate((h) => (location.hash = h), hash);
      return `go to ${hash}`;
    }
    case 'back':
      await page.goBack().catch(() => {});
      return 'back';
    case 'reload':
      await page.reload();
      return 'reload';
    case 'lose-session':
      await page.context().clearCookies();
      if (rng.chance(2)) await page.reload();
      return 'lose the session';
    case 'keys': {
      const presses = 1 + rng.below(6);
      for (let i = 0; i < presses; i++) await page.keyboard.press('Tab');
      const key = rng.pick(['Enter', 'Space', 'Escape']);
      await page.keyboard.press(key);
      return `tab ${presses}, ${key}`;
    }
    case 'sign-in':
      await signIn(walk);
      return `sign in (${await screen(page)})`;
  }
}

async function describe(target: ReturnType<Page['locator']>): Promise<string> {
  return target
    .evaluate((el) => {
      const text = (
        el.getAttribute('aria-label') ??
        el.getAttribute('name') ??
        el.textContent ??
        ''
      ).trim();
      return `<${el.tagName.toLowerCase()}> "${text.slice(0, 30)}"`;
    })
    .catch(() => '<gone>');
}

/** Draws the next action. Signing in comes first whenever the person is not in, so a walk is mostly spent inside
 * the GUI; the session is lost seldom, since every sign-in spends the throttle's allowance. */
async function next(walk: Walk): Promise<Action> {
  const on = await screen(walk.page);
  // Somebody signed out stays out longer, so the screens shown in place of a page get walked too.
  const eager = walk.who === 'signed-out' ? walk.rng.chance(4) : !walk.rng.chance(3);
  if ((on === 'sign-in' || on === 'choose-password') && walk.signIns < 4 && eager) return 'sign-in';
  const action = walk.rng.pick(ACTIONS);
  if (action === 'lose-session' && !walk.rng.chance(3)) return 'click';
  return action;
}

async function walk(seed: number, browser: Browser, leader: Leader, api: Api | null, who: Who) {
  const rng = new Rng(seed);
  const theme = rng.pick(THEMES);
  const size = rng.pick(Object.keys(VIEWPORTS) as (keyof typeof VIEWPORTS)[]);
  const start = rng.pick(STARTS);
  const reached = new Set<string>([`who:${who}`, `theme:${theme}`, `viewport:${size}`]);

  let provider: Provider | null = null;
  const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: VIEWPORTS[size] });
  try {
    if (ROLE_OF[who]) {
      provider = await Provider.start({
        subject: `00u-${who}`,
        name: `Fuzz ${who}`,
        email: `${who}@example.com`,
        groups: [PROVIDER_GROUP],
      });
      await offerSso(api!, provider, ROLE_OF[who]!);
    }
    await context.addInitScript((t) => {
      // `about:blank`, between loads, has no storage.
      if (location.protocol === 'https:') localStorage.setItem('smeltr-theme', t);
    }, theme);
    const page = await context.newPage();
    watchRequests(page);
    const state: Walk = {
      seed,
      rng,
      who,
      page,
      leader,
      password: who === 'administrator' ? ADMIN_PASSWORD : leader.oneTimePassword,
      steps: [`start on ${start} as ${who}, ${theme}, ${size}`],
      problems: [],
      links: new Set(),
      signIns: 0,
    };
    listen(page, leader, state.problems);
    await page.goto(`${leader.url}/${start}`);
    await check(state);
    if (who !== 'signed-out' && state.problems.length === 0) {
      await signIn(state);
      await check(state);
    }
    const replay = () =>
      `seed ${seed} (replay with SMELTR_FUZZ_SEED=${seed}):\n  ${state.steps.join('\n  ')}`;
    try {
      for (let i = 0; i < STEPS && state.problems.length === 0; i++) {
        const action = await next(state);
        reached.add(`action:${action}`);
        state.steps.push(await step(state, action));
        // Somewhere outside the leader -- the provider mid-redirect, or a link that left -- is waited out.
        await page.waitForURL((u) => u.href.startsWith(leader.url) || u.protocol === 'about:', {
          timeout: 10_000,
        });
        await check(state);
      }
    } catch (e) {
      // A step that could not finish fails the seed too, said with the steps that led to it.
      throw new Error(`${replay()}\n${e instanceof Error ? e.message : e}`);
    }
    expect(state.problems, replay()).toEqual([]);
  } finally {
    // A test that ran out of time has had its context closed already.
    await context.close().catch(() => {});
    await provider?.stop();
    const dir = path.join(process.env[RUN_DIR] ?? '', 'fuzz');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `${seed}.json`), JSON.stringify([...reached]));
  }
}

for (const seed of seeds(DEFAULT_SEEDS)) {
  const who = WHO[seed % WHO.length];
  test(`gui fuzz seed ${seed} (${who})`, async ({ browser, leader }) => {
    test.setTimeout(300_000);
    // A person who signs in with the one-time password must find it unspent.
    const api = who === 'signed-out' || who === 'required-change' ? null : await claimAdmin(leader);
    try {
      await walk(seed, browser, leader, api, who);
    } finally {
      await api?.ctx.dispose();
    }
  });
}
