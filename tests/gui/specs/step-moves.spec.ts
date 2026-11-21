// Moving a pipeline's steps by dragging: a step held still on its head is picked up and drops into whichever gap the
// pointer is over, at any depth. Each test drags one step into every gap of a pipeline with an If in it and another
// If nested in that one's else, so each list's start, middle and end and each empty branch is a drop, and checks
// the steps on screen against where the drop must put it.
import type { Locator, Page } from '@playwright/test';
import { onScreen as works } from '../onscreen';
import { test, expect, goto, settled, watchRequests, type Api, type Leader } from '../fleet';

const ON = '?branch=work';
const PIPELINE = 'moved';

type Tree = { id: string; then?: Tree[]; else?: Tree[] };

const leaf = (id: string): Tree => ({ id });
/** a b i[t1 t2 | e1 j[|]] d */
const START = (): Tree[] => [
  leaf('a'),
  leaf('b'),
  { id: 'i', then: [leaf('t1'), leaf('t2')], else: [leaf('e1'), { id: 'j', then: [], else: [] }] },
  leaf('d'),
];

/** A list of the pipeline: the top, or a branch of the If `owner`. */
type ListRef = { owner: string | null; branch?: 'then' | 'else' };
const LISTS: ListRef[] = [
  { owner: null },
  { owner: 'i', branch: 'then' },
  { owner: 'i', branch: 'else' },
  { owner: 'j', branch: 'then' },
  { owner: 'j', branch: 'else' },
];

const outline = (steps: Tree[]): string =>
  steps.map((s) => (s.then ? `${s.id}[${outline(s.then)}|${outline(s.else!)}]` : s.id)).join(' ');

function find(steps: Tree[], id: string): Tree | undefined {
  for (const s of steps) {
    if (s.id === id) return s;
    const inner = s.then && (find(s.then, id) ?? find(s.else!, id));
    if (inner) return inner;
  }
}

function listOf(steps: Tree[], ref: ListRef): Tree[] {
  return ref.owner === null ? steps : find(steps, ref.owner)![ref.branch!];
}

/** The place that dropping step `id` at `index` of `ref` puts it in; `index` is counted before the step leaves its
 * place. */
function dropped(id: string, ref: ListRef, index: number): string {
  const steps = START();
  const moved = find(steps, id)!;
  const to = listOf(steps, ref);
  const strip = (list: Tree[]): boolean => {
    const at = list.indexOf(moved);
    if (at !== -1) {
      list.splice(at, 1);
      if (list === to && at < index) index--;
      return true;
    }
    return list.some((s) => s.then && (strip(s.then) || strip(s.else!)));
  };
  strip(steps);
  to.splice(index, 0, moved);
  return outline(steps);
}

function dto(s: Tree): object {
  return s.then
    ? { type: 'if', id: s.id, condition: 'true', then: s.then.map(dto), else: s.else!.map(dto) }
    : { type: 'comment', id: s.id, text: s.id };
}

/** The steps on screen as an outline, by their cards' ids. */
async function onScreen(page: Page): Promise<string> {
  return page.evaluate(() => {
    const walk = (list: Element): string =>
      [...list.querySelectorAll(':scope > .slot > .card')]
        .map((card) => {
          const id = card.id.replace(/^step-/, '');
          const branches = card.querySelectorAll(':scope > .branches > .branch > .step-list');
          return branches.length ? `${id}[${walk(branches[0])}|${walk(branches[1])}]` : id;
        })
        .join(' ');
    const top = document.querySelector('.step-list');
    return top ? walk(top) : '';
  });
}

/** The element showing list `ref`. */
function listOn(page: Page, ref: ListRef): Locator {
  if (ref.owner === null) return page.locator('.step-list').first();
  const n = ref.branch === 'then' ? 1 : 2;
  return page.locator(`#step-${ref.owner} > .branches > .branch:nth-child(${n}) > .step-list`);
}

/** A point over the gap before step `index` of `ref`'s list: the top edge of that step, or its + step row at the
 * end. */
async function gapPoint(page: Page, ref: ListRef, index: number) {
  const list = listOn(page, ref);
  const slots = list.locator(':scope > .slot');
  const n = await slots.count();
  // The dragged step's card is lifted out, and its empty slot stands where it was.
  const card = slots.nth(index).locator(':scope > .card:not(.carried)');
  const box =
    index < n
      ? ((await card.count()) ? await card.boundingBox() : await slots.nth(index).boundingBox())!
      : (await list.locator(':scope > .add-row').boundingBox())!;
  return { x: box.x + 20, y: index < n ? box.y + 3 : box.y + box.height / 2 };
}

async function arrange(page: Page, api: Api, leader: Leader) {
  await api.put(`/api/areas/shared/pipelines/${PIPELINE}${ON}`, {
    id: PIPELINE,
    name: PIPELINE,
    steps: START().map(dto),
  });
  await goto(page, leader, `#/build/pipelines/${PIPELINE}`);
  await expect.poll(() => onScreen(page)).toBe(outline(START()));
  await settled(page);
}

/** A new page in `page`'s session, which it replaces. */
async function fresh(page: Page): Promise<Page> {
  const next = await page.context().newPage();
  await page.close();
  await next.setViewportSize({ width: 1920, height: 1080 });
  watchRequests(next);
  return next;
}

/** Waits for what is moving on the page to come to rest. */
async function still(page: Page) {
  await page.evaluate(
    () => new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done))),
  );
  await page.waitForFunction(() =>
    document.getAnimations().every((a) => a.playState !== 'running'),
  );
}

/** Moves the pointer with `move` onto `to`, and again until `to` stops moving from under it: the gaps a drag opens
 * shift the steps after them. */
async function aim(
  page: Page,
  to: () => Promise<{ x: number; y: number }>,
  move: (at: { x: number; y: number }) => Promise<void>,
): Promise<{ x: number; y: number }> {
  let at = await to();
  for (let tries = 0; ; tries++) {
    expect(tries, 'the pointer comes to rest over its gap').toBeLessThan(5);
    await move(at);
    await still(page);
    const next = await to();
    if (Math.hypot(next.x - at.x, next.y - at.y) < 1) return at;
    at = next;
  }
}

/** Holds `id`'s head until it is picked up, then drags it over `to` and lets go. */
async function drag(page: Page, id: string, to: () => Promise<{ x: number; y: number }>) {
  const badge = (await page.locator(`#step-${id} > .card-head .type-badge`).boundingBox())!;
  const from = { x: badge.x + badge.width / 2, y: badge.y + badge.height / 2 };
  await page.mouse.move(from.x, from.y);
  await page.mouse.down();
  await expect(page.locator('.carried-chip').first()).toBeVisible();
  await aim(page, to, (at) => page.mouse.move(at.x, at.y, { steps: 8 }));
  await page.mouse.up();
  await page.mouse.move(0, 0);
}

for (const id of ['b', 'a', 'd', 't2', 'e1', 'i', 'j']) {
  test(`a dragged ${id} drops into every gap at every depth`, async ({ admin, api, leader }) => {
    let page = admin;
    test.setTimeout(300_000);
    for (const ref of LISTS) {
      // An If's branches are lifted out with it, so they are no place to drop it.
      if (ref.owner !== null && find([find(START(), id)!], ref.owner)) continue;
      const size = listOf(START(), ref).length;
      for (let index = 0; index <= size; index++) {
        // A page of its own, so the last case's unsaved move is not this one's start.
        page = await fresh(page);
        await arrange(page, api, leader);
        await drag(page, id, () => gapPoint(page, ref, index));
        const where = `${ref.owner ?? 'top'}${ref.branch ? `.${ref.branch}` : ''}[${index}]`;
        await expect.poll(() => onScreen(page), { message: where }).toBe(dropped(id, ref, index));
        // Once the steps have glided to their places, which starts on the frame after the drop.
        await still(page);
        expect(await works(page, false), where).toEqual([]);
      }
    }
  });
}

test('a press that moves before it is held, or a drag Esc ends, moves nothing', async ({
  admin: page,
  api,
  leader,
}) => {
  await arrange(page, api, leader);
  const head = (await page.locator('#step-b > .card-head').boundingBox())!;
  await page.mouse.move(head.x + 50, head.y + head.height / 2);
  await page.mouse.down();
  await page.mouse.move(head.x + 50, head.y + 200, { steps: 4 });
  await page.mouse.up();
  await expect(page.locator('.carried-chip')).toHaveCount(0);
  expect(await onScreen(page)).toBe(outline(START()));

  await page.mouse.move(head.x + 50, head.y + head.height / 2);
  await page.mouse.down();
  await expect(page.locator('.carried-chip').first()).toBeVisible();
  const end = await gapPoint(page, { owner: null }, 4);
  await page.mouse.move(end.x, end.y, { steps: 4 });
  await page.keyboard.press('Escape');
  await page.mouse.up();
  await expect(page.locator('.carried-chip')).toHaveCount(0);
  expect(await onScreen(page)).toBe(outline(START()));
});

test('a step dragged to the edge of the steps scrolls them toward a gap out of sight', async ({
  admin: page,
  api,
  leader,
}) => {
  await page.setViewportSize({ width: 1920, height: 600 });
  const notes = Array.from({ length: 12 }, (_, k) => `n${k}`);
  await api.put(`/api/areas/shared/pipelines/${PIPELINE}${ON}`, {
    id: PIPELINE,
    name: PIPELINE,
    steps: [...notes.map((id) => dto(leaf(id))), dto({ id: 'i', then: [], else: [] })],
  });
  await goto(page, leader, `#/build/pipelines/${PIPELINE}`);
  await expect.poll(() => onScreen(page)).toBe(`${notes.join(' ')} i[|]`);
  await settled(page);
  const then = listOn(page, { owner: 'i', branch: 'then' }).locator(':scope > .add-row');
  await expect(then).not.toBeInViewport();
  let scrolled = false;
  await drag(page, 'n0', async () => {
    // Held at the bottom edge until the If's then comes into sight.
    if (!scrolled) {
      await page.mouse.move(200, 590, { steps: 4 });
      await expect(then).toBeInViewport({ ratio: 1 });
      scrolled = true;
    }
    const box = (await then.boundingBox())!;
    return { x: box.x + 20, y: box.y + box.height / 2 };
  });
  await expect.poll(() => onScreen(page)).toBe(`${notes.slice(1).join(' ')} i[n0|]`);
});

test('↓ pressed from the keyboard walks a step through an If and out, keeping its focus', async ({
  admin: page,
  api,
  leader,
}) => {
  await arrange(page, api, leader);
  await page.locator('#step-b > .card-head').getByRole('button', { name: 'Move down' }).focus();
  for (const want of [
    'a i[b t1 t2|e1 j[|]] d',
    'a i[t1 b t2|e1 j[|]] d',
    'a i[t1 t2 b|e1 j[|]] d',
    'a i[t1 t2|b e1 j[|]] d',
    'a i[t1 t2|e1 b j[|]] d',
    'a i[t1 t2|e1 j[b|]] d',
    'a i[t1 t2|e1 j[|b]] d',
    'a i[t1 t2|e1 j[|] b] d',
    'a i[t1 t2|e1 j[|]] b d',
    'a i[t1 t2|e1 j[|]] d b',
  ]) {
    await page.keyboard.press('Enter');
    await expect.poll(() => onScreen(page)).toBe(want);
    await expect(
      page.locator('#step-b > .card-head').getByRole('button', { name: 'Move down' }),
    ).toBeFocused();
  }
});

test('a finger held on a step drags it, rather than scrolling the page', async ({
  admin: page,
  api,
  leader,
}) => {
  await arrange(page, api, leader);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('Emulation.setTouchEmulationEnabled', { enabled: true, maxTouchPoints: 1 });
  const badge = (await page.locator('#step-b > .card-head .type-badge').boundingBox())!;
  const from = { x: badge.x + badge.width / 2, y: badge.y + badge.height / 2 };
  const touch = (type: string, at: { x: number; y: number }) =>
    cdp.send('Input.dispatchTouchEvent', {
      type,
      touchPoints: type === 'touchEnd' ? [] : [{ x: at.x, y: at.y }],
    });
  await touch('touchStart', from);
  await expect(page.locator('.carried-chip').first()).toBeVisible();
  let finger = from;
  const to = await aim(
    page,
    () => gapPoint(page, { owner: 'i', branch: 'then' }, 0),
    async (at) => {
      for (let k = 1; k <= 8; k++)
        await touch('touchMove', {
          x: finger.x + ((at.x - finger.x) * k) / 8,
          y: finger.y + ((at.y - finger.y) * k) / 8,
        });
      finger = at;
    },
  );
  await touch('touchEnd', to);
  await expect.poll(() => onScreen(page)).toBe(dropped('b', { owner: 'i', branch: 'then' }, 0));
});

test('a screen reader hears where a moved step now is', async ({ admin: page, api, leader }) => {
  await arrange(page, api, leader);
  const said = page.locator('[aria-live="polite"]', { hasText: /^Moved/ });
  await page.locator('#step-b > .card-head').getByRole('button', { name: 'Move down' }).focus();
  await page.keyboard.press('Enter');
  await expect(said).toHaveText('Moved to step 1 of 3 in the then branch of If true.');
  await page.locator('#step-b > .card-head').getByRole('button', { name: 'Move up' }).focus();
  await page.keyboard.press('Enter');
  await expect(said).toHaveText('Moved to step 2 of 4.');
});

test('with reduced motion, nothing glides', async ({ admin: page, api, leader }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await arrange(page, api, leader);
  await drag(page, 'b', () => gapPoint(page, { owner: 'i', branch: 'then' }, 0));
  await expect.poll(() => onScreen(page)).toBe(dropped('b', { owner: 'i', branch: 'then' }, 0));
  expect(await page.evaluate(() => document.getAnimations().map((a) => a.playState))).not.toContain(
    'running',
  );
});

test('an open step and an If each open a gap only as tall as their head', async ({
  admin: page,
  api,
  leader,
}) => {
  await arrange(page, api, leader);
  await page.locator('#step-b > .card-head .expander').click();
  for (const id of ['b', 'i']) {
    const card = page.locator(`#step-${id}`);
    const head = (await card.locator(':scope > .card-head').boundingBox())!.height;
    expect((await card.boundingBox())!.height).toBeGreaterThan(head + 10);
    const badge = (await card.locator(':scope > .card-head .type-badge').boundingBox())!;
    await page.mouse.move(badge.x + badge.width / 2, badge.y + badge.height / 2);
    await page.mouse.down();
    await expect(page.locator('.carried-chip')).toBeVisible();
    await aim(
      page,
      () => gapPoint(page, { owner: null }, 0),
      (at) => page.mouse.move(at.x, at.y, { steps: 8 }),
    );
    const gap = (await page.locator('.insert.drop').boundingBox())!.height;
    expect(Math.abs(gap - head)).toBeLessThan(4);
    await page.keyboard.press('Escape');
    await page.mouse.up();
    await still(page);
  }
});

test('a dropped If brings its steps with it as it glides', async ({ admin: page, api, leader }) => {
  await arrange(page, api, leader);
  const within = () =>
    page.evaluate(() => {
      const outer = document.getElementById('step-i')!.getBoundingClientRect();
      return ['t1', 't2', 'e1', 'j'].map((id) => {
        const r = document.getElementById(`step-${id}`)!.getBoundingClientRect();
        return [Math.round(r.left - outer.left), Math.round(r.top - outer.top)];
      });
    });
  const landed = await within();
  await drag(page, 'i', () => gapPoint(page, { owner: null }, 0));
  await page.evaluate(() => new Promise((done) => requestAnimationFrame(done)));
  expect(await within()).toEqual(landed);
  await still(page);
  expect(await within()).toEqual(landed);
});
