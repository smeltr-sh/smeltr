// Moving a pipeline's steps by dragging: a step held still on its head is picked up and drops into whichever gap the
// pointer is over, at any depth. Each test drags one step into every gap of a pipeline with an If in it and another
// If nested in that one's else, so each list's start, middle and end and each empty branch is a drop, and checks
// the steps on screen against where the drop must put it.
import type { Locator, Page } from '@playwright/test';
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

/** Where a drop of `id` at `index` of `ref`, counted before it leaves its place, puts it; unchanged into its own
 * branches. */
function dropped(id: string, ref: ListRef, index: number): string {
  const steps = START();
  const moved = find(steps, id)!;
  const to = listOf(steps, ref);
  if (ref.owner !== null && find([moved], ref.owner)) return outline(steps);
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
  const box =
    index < n
      ? (await slots.nth(index).locator(':scope > .card').boundingBox())!
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

/** Holds `id`'s head until it is picked up, then drags it over `to` and lets go. */
async function drag(page: Page, id: string, to: () => Promise<{ x: number; y: number }>) {
  const badge = (await page.locator(`#step-${id} > .card-head .type-badge`).boundingBox())!;
  const from = { x: badge.x + badge.width / 2, y: badge.y + badge.height / 2 };
  await page.mouse.move(from.x, from.y);
  await page.mouse.down();
  await expect(page.locator('.carried').first()).toBeVisible();
  const at = await to();
  await page.mouse.move(at.x, at.y, { steps: 8 });
  await page.mouse.up();
  await page.mouse.move(0, 0);
}

for (const id of ['b', 'a', 'd', 't2', 'e1', 'i', 'j']) {
  test(`a dragged ${id} drops into every gap at every depth`, async ({ admin, api, leader }) => {
    let page = admin;
    test.setTimeout(300_000);
    for (const ref of LISTS) {
      const size = listOf(START(), ref).length;
      for (let index = 0; index <= size; index++) {
        // A page of its own, so the last case's unsaved move is not this one's start.
        page = await fresh(page);
        await arrange(page, api, leader);
        await drag(page, id, () => gapPoint(page, ref, index));
        const where = `${ref.owner ?? 'top'}${ref.branch ? `.${ref.branch}` : ''}[${index}]`;
        await expect.poll(() => onScreen(page), { message: where }).toBe(dropped(id, ref, index));
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
  await expect(page.locator('.carried')).toHaveCount(0);
  expect(await onScreen(page)).toBe(outline(START()));

  await page.mouse.move(head.x + 50, head.y + head.height / 2);
  await page.mouse.down();
  await expect(page.locator('.carried').first()).toBeVisible();
  const end = await gapPoint(page, { owner: null }, 4);
  await page.mouse.move(end.x, end.y, { steps: 4 });
  await page.keyboard.press('Escape');
  await page.mouse.up();
  await expect(page.locator('.carried')).toHaveCount(0);
  expect(await onScreen(page)).toBe(outline(START()));
});
