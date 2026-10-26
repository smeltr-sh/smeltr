// The pipeline journey fuzzer's moves (`specs/pipeline-fuzz.spec.ts`): what an author does to one pipeline's steps in
// the editor, each through the GUI, and the model of what the editor must then hold. After every move the steps on
// screen must be the model's; after a save, the leader's copy must be too.
import { expect, type Page, type Locator } from '@playwright/test';
import type { Api, Leader } from '../fleet';
import { goto } from '../fleet';
import type { Rng } from '../fuzz';

/** Seeds a run takes when nothing asks for more. */
export const PIPELINE_SEEDS = 1;

export const PIPELINE = 'edited';
/** Pipelines arranged beside it for a Call step to inline. Each can be called once: a second call repeats its
 * step ids, which the leader refuses. */
export const CALLEES = ['callee-a', 'callee-b'];
export const ON = '?branch=work&scope=shared';
/** The sample the editor previews on, arranged beside the pipeline: a record for each `_raw` a condition can name. */
export const SAMPLE = 'rows';
export const RAWS = ['1', '2', '3'];

export type StepModel =
  | { type: 'comment'; id: string; text: string }
  | { type: 'eval'; id: string; target: string; expr: string; disabled: boolean }
  | { type: 'drop'; id: string; condition: string; disabled: boolean }
  | { type: 'call'; id: string; pipeline: string; disabled: boolean }
  | { type: 'if'; id: string; condition: string; then: StepModel[]; else: StepModel[] };
export type StepType = StepModel['type'];
export const TYPES: readonly StepType[] = ['comment', 'eval', 'drop', 'call', 'if'];
const LABELS: Record<StepType, string> = {
  comment: 'Comment',
  eval: 'Eval',
  drop: 'Drop',
  call: 'Call',
  if: 'If',
};

/** A branch of an if step, as the index of the step in its list and which of its two lists. */
type Hop = { at: number; branch: 'then' | 'else' };
/** One list of steps: the pipeline's own, or a branch's, by the hops that reach it. */
type List = { path: Hop[]; steps: StepModel[] };

export type Model = {
  /** What the editor holds. */
  editing: StepModel[];
  /** What the leader holds. */
  saved: StepModel[];
  /** Whether each step's card is open, by id. An if card is always open. */
  open: Map<string, boolean>;
  /** For values nothing has used yet. */
  made: number;
  /** A sample is chosen, so the editor previews the steps on it. */
  sampled: boolean;
  /** The step ◎ paused the records after, by id. */
  paused: string | null;
};

export type Journey = { page: Page; rng: Rng; leader: Leader; api: Api; model: Model };

export type Move = {
  name: string;
  /** How often it is drawn; 0 when it makes no sense. */
  weight: (m: Model) => number;
  /** Does it through the GUI and changes the model to match. Answers what it drew. */
  run: (j: Journey) => Promise<string>;
};

export function newModel(): Model {
  return { editing: [], saved: [], open: new Map(), made: 0, sampled: false, paused: null };
}

const clone = (steps: StepModel[]): StepModel[] => structuredClone(steps);

function lists(steps: StepModel[], path: Hop[] = []): List[] {
  const out: List[] = [{ path, steps }];
  steps.forEach((s, at) => {
    if (s.type !== 'if') return;
    out.push(...lists(s.then, [...path, { at, branch: 'then' }]));
    out.push(...lists(s.else, [...path, { at, branch: 'else' }]));
  });
  return out;
}

/** Every step at any depth, with the list it is in and where. */
function placed(steps: StepModel[]): { list: List; at: number; step: StepModel }[] {
  return lists(steps).flatMap((list) => list.steps.map((step, at) => ({ list, at, step })));
}

const count = (steps: StepModel[]) => placed(steps).length;
const called = (steps: StepModel[]) =>
  placed(steps).flatMap(({ step }) => (step.type === 'call' ? [step.pipeline] : []));
const describe = (list: List, at: number) =>
  [...list.path.map((h) => `step ${h.at + 1}'s ${h.branch}`), `step ${at + 1}`].join(', ');

/** The list `path` reaches, in the editor. */
function listOn(page: Page, path: Hop[]): Locator {
  let list = page.locator('.step-list').first();
  for (const hop of path)
    list = slot(list, hop.at)
      .locator(':scope > .if-card > .branches > .branch')
      .nth(hop.branch === 'then' ? 0 : 1)
      .locator(':scope > .step-list');
  return list;
}
const slot = (list: Locator, at: number) => list.locator(':scope > .slot').nth(at);
const cardOn = (page: Page, list: List, at: number) =>
  slot(listOn(page, list.path), at).locator(':scope > [id^="step-"]');

/** Clicks a step's control: its own button where the head is wide enough to show it, or the same choice from the
 * More menu that takes the controls' place on a narrow one. */
async function control(
  j: Journey,
  card: Locator,
  label: 'Delete' | 'Move up' | 'Move down' | 'Disable',
) {
  const head = card.locator(':scope > .card-head');
  const own = head.locator(
    `:scope > .controls > button.fold${label === 'Disable' ? '[aria-label$="this step"]' : `[aria-label="${label}"]`}`,
  );
  await head.hover({ position: { x: 4, y: 4 } });
  if (await own.isVisible()) {
    await own.click();
    return;
  }
  const more = head.locator(':scope > .controls > .more');
  await more.getByRole('button', { name: 'More actions' }).click();
  const pick = label === 'Disable' ? /^(Disable|Enable)$/ : new RegExp(`^${label}$`);
  await more.locator('button.pick', { hasText: pick }).click();
}

/** Opens a closed card, by the start of its head: on a narrow page the controls cover its middle. */
async function openCard(j: Journey, card: Locator, step: StepModel) {
  if (step.type === 'if' || j.model.open.get(step.id)) return;
  await card.locator(':scope > .card-head > .expander').click({ position: { x: 8, y: 8 } });
  j.model.open.set(step.id, true);
}

/** A condition on `_raw` that some of the sample's records meet, or none. */
const condition = (rng: Rng) => `_raw == "${rng.pick([...RAWS, '4'])}"`;

/** Fills a new or edited step's own value, the one its summary shows, and sets it in the model. */
async function fill(j: Journey, card: Locator, step: StepModel) {
  const n = j.model.made++;
  switch (step.type) {
    case 'comment':
      step.text = `note ${n}`;
      await card.locator('textarea').fill(step.text);
      break;
    case 'eval':
      if (!step.target) {
        step.target = `f${n}`;
        await card.getByPlaceholder('target field').fill(step.target);
      }
      step.expr = String(n);
      await card.getByPlaceholder('expression').fill(step.expr);
      break;
    case 'drop':
      step.condition = condition(j.rng);
      await card.getByPlaceholder('e.g. status == 500').fill(step.condition);
      break;
    case 'if':
      step.condition = condition(j.rng);
      await card
        .locator(':scope > .card-head .condition')
        .getByPlaceholder('condition, e.g. status >= 400')
        .fill(step.condition);
      break;
    case 'call':
      return;
  }
  // Done typing, as a person is, which closes what the field suggests.
  await j.page.keyboard.press('Escape');
}

const add: Move = {
  name: 'add',
  weight: (m) => (count(m.editing) >= 14 ? 0 : count(m.editing) < 4 ? 6 : 3),
  run: async (j) => {
    const { page, rng, model } = j;
    const free = CALLEES.filter((c) => !called(model.editing).includes(c));
    const list = rng.pick(lists(model.editing).filter((l) => l.path.length < 3));
    const at = rng.below(list.steps.length + 1);
    const type = rng.pick(TYPES.filter((t) => t !== 'call' || free.length > 0));
    const on = listOn(page, list.path);
    if (at === list.steps.length) await on.locator(':scope > .add-row button').click();
    else await slot(on, at).locator(':scope > .insert button').click();
    await page
      .getByRole('dialog', { name: 'add step' })
      .getByRole('button', { name: LABELS[type], exact: true })
      .click();
    const card = cardOn(page, { path: list.path, steps: [] }, at);
    const id = (await card.getAttribute('id'))!.slice('step-'.length);
    let step: StepModel;
    switch (type) {
      case 'comment':
        step = { type, id, text: '' };
        break;
      case 'eval':
        step = { type, id, target: '', expr: '', disabled: false };
        break;
      case 'drop':
        step = { type, id, condition: 'false', disabled: false };
        break;
      case 'call':
        step = { type, id, pipeline: rng.pick(free), disabled: false };
        await card.getByRole('combobox').selectOption(step.pipeline);
        break;
      case 'if':
        step = { type, id, condition: 'true', then: [], else: [] };
        break;
    }
    list.steps.splice(at, 0, step);
    // A step just added opens to be filled in.
    model.open.set(id, true);
    await fill(j, card, step);
    return `add ${type} ${id} at ${describe(list, at)}`;
  },
};

const remove: Move = {
  name: 'delete',
  weight: (m) => (count(m.editing) ? 3 : 0),
  run: async (j) => {
    const { page, rng, model } = j;
    const { list, at, step } = rng.pick(placed(model.editing));
    await control(j, cardOn(page, list, at), 'Delete');
    list.steps.splice(at, 1);
    keepPause(model);
    return `delete ${step.type} ${step.id}, ${describe(list, at)}${step.type === 'if' ? ` with ${count(step.then) + count(step.else)} inside` : ''}`;
  },
};

function mover(name: 'move-up' | 'move-down', label: 'Move up' | 'Move down', by: -1 | 1): Move {
  return {
    name,
    weight: (m) => (count(m.editing) > 1 ? 2 : 0),
    run: async (j) => {
      const { page, rng, model } = j;
      const { list, at, step } = rng.pick(placed(model.editing));
      await control(j, cardOn(page, list, at), label);
      // At the end of its list it stays where it is.
      const to = at + by;
      if (to >= 0 && to < list.steps.length) {
        const [s] = list.steps.splice(at, 1);
        list.steps.splice(to, 0, s);
      }
      return `${label.toLowerCase()} ${step.type} ${step.id}, ${describe(list, at)}`;
    },
  };
}

const disable: Move = {
  name: 'disable',
  weight: (m) => (placed(m.editing).some(({ step }) => 'disabled' in step) ? 2 : 0),
  run: async (j) => {
    const { page, rng, model } = j;
    const { list, at, step } = rng.pick(
      placed(model.editing).filter(({ step }) => 'disabled' in step),
    );
    await control(j, cardOn(page, list, at), 'Disable');
    if ('disabled' in step) step.disabled = !step.disabled;
    return `${'disabled' in step && step.disabled ? 'disable' : 'enable'} ${step.type} ${step.id}`;
  },
};

const edit: Move = {
  name: 'edit',
  weight: (m) => (placed(m.editing).some(({ step }) => step.type !== 'call') ? 3 : 0),
  run: async (j) => {
    const { page, rng, model } = j;
    const { list, at, step } = rng.pick(
      placed(model.editing).filter(({ step }) => step.type !== 'call'),
    );
    const card = cardOn(page, list, at);
    await openCard(j, card, step);
    await fill(j, card, step);
    return `edit ${step.type} ${step.id}, ${describe(list, at)}`;
  },
};

const fold: Move = {
  name: 'fold',
  weight: (m) => (placed(m.editing).some(({ step }) => step.type !== 'if') ? 1 : 0),
  run: async (j) => {
    const { page, rng, model } = j;
    const { list, at, step } = rng.pick(
      placed(model.editing).filter(({ step }) => step.type !== 'if'),
    );
    await cardOn(page, list, at)
      .locator(':scope > .card-head > .expander')
      .click({ position: { x: 8, y: 8 } });
    model.open.set(step.id, !model.open.get(step.id));
    return `${model.open.get(step.id) ? 'open' : 'close'} ${step.type} ${step.id}`;
  },
};

const dirty = (m: Model) => JSON.stringify(m.editing) !== JSON.stringify(m.saved);

const save: Move = {
  name: 'save',
  weight: (m) => (dirty(m) ? 3 : 0),
  run: async (j) => {
    const { page, model } = j;
    await page.getByRole('button', { name: 'Save', exact: true }).click();
    await expect(page.getByText('Saved.')).toBeVisible();
    model.saved = clone(model.editing);
    await expect.poll(() => savedSteps(j.api)).toEqual(model.saved);
    return 'save';
  },
};

const discard: Move = {
  name: 'discard',
  weight: (m) => (dirty(m) ? 1 : 0),
  run: async (j) => {
    const { page, model } = j;
    await page.getByRole('button', { name: 'Discard', exact: true }).click();
    model.editing = clone(model.saved);
    // The steps put back are new to the editor, so the pause goes with the ones discarded.
    model.paused = null;
    return 'discard';
  },
};

const reload: Move = {
  name: 'reload',
  weight: () => 1,
  run: async (j) => {
    await j.page.reload();
    // Unsaved edits are held for the tab's life (`edits.ts`), which a reload ends.
    j.model.editing = clone(j.model.saved);
    j.model.paused = null;
    j.model.sampled = false;
    return 'reload';
  },
};

const leaveAndReturn: Move = {
  name: 'leave-and-return',
  weight: () => 1,
  run: async (j) => {
    const { page } = j;
    await page.getByRole('link', { name: 'Pipelines' }).first().click();
    await page.getByRole('link', { name: PIPELINE, exact: true }).click();
    // The editor mounts anew, on no sample, and its pause was its own.
    j.model.paused = null;
    j.model.sampled = false;
    return 'leave the editor and come back';
  },
};

/** Every step in the order the editor numbers them: each if before its then and its else. */
function flat(steps: StepModel[]): StepModel[] {
  return steps.flatMap((s) => (s.type === 'if' ? [s, ...flat(s.then), ...flat(s.else)] : [s]));
}

/** A pause outlives its step only as the same step: deleted, or put back by a discard, it goes. */
function keepPause(m: Model) {
  if (m.paused && !flat(m.editing).some((s) => s.id === m.paused)) m.paused = null;
}

const chooseSample: Move = {
  name: 'choose-sample',
  weight: (m) => (m.sampled ? 0 : 2),
  run: async (j) => {
    const { page } = j;
    await page.getByRole('button', { name: 'Choose a sample' }).click();
    await page
      .getByRole('listbox', { name: 'Saved samples' })
      .getByRole('option', { name: new RegExp(`^${SAMPLE}\\b`) })
      .click();
    j.model.sampled = true;
    return `choose the sample ${SAMPLE}`;
  },
};

const pause: Move = {
  name: 'pause',
  weight: (m) => (m.sampled && m.editing.length ? 3 : 0),
  run: async (j) => {
    const { page, rng, model } = j;
    const { list, at, step } = rng.pick(placed(model.editing));
    const head = cardOn(page, list, at).locator(':scope > .card-head');
    await head.hover({ position: { x: 4, y: 4 } });
    const paused = model.paused === step.id;
    await head
      .locator(':scope > .controls')
      .getByRole('button', {
        name: paused ? 'Show the whole pipeline' : 'Pause the records after this step',
      })
      .click();
    model.paused = paused ? null : step.id;
    return `${paused ? 'unpause' : 'pause after'} ${step.type} ${step.id}, ${describe(list, at)}`;
  },
};

/** Every move. */
export const MOVES: readonly Move[] = [
  add,
  remove,
  mover('move-up', 'Move up', -1),
  mover('move-down', 'Move down', 1),
  disable,
  edit,
  fold,
  save,
  discard,
  reload,
  leaveAndReturn,
  chooseSample,
  pause,
];

export function nextMove(rng: Rng, model: Model): Move {
  const offered = MOVES.flatMap((m) => Array<Move>(m.weight(model)).fill(m));
  return rng.pick(offered);
}

/** Opens the pipeline in the editor. */
export async function open(page: Page, leader: Leader) {
  await goto(page, leader, `#/build/pipelines/${PIPELINE}`);
  await expect(page.getByRole('button', { name: 'Add step' }).first()).toBeVisible();
}

/** The steps as the leader's API holds them, in the model's shape. */
export async function savedSteps(api: Api): Promise<StepModel[]> {
  type Dto = {
    type: StepType;
    id: string;
    text?: string;
    condition?: string;
    pipeline?: string;
    disabled?: boolean;
    assignments?: { target: string; expr: string }[];
    then?: Dto[];
    else?: Dto[];
  };
  const of = (d: Dto): StepModel => {
    switch (d.type) {
      case 'comment':
        return { type: 'comment', id: d.id, text: d.text! };
      case 'eval':
        return {
          type: 'eval',
          id: d.id,
          target: d.assignments![0].target,
          expr: d.assignments![0].expr,
          disabled: d.disabled!,
        };
      case 'drop':
        return { type: 'drop', id: d.id, condition: d.condition!, disabled: d.disabled! };
      case 'call':
        return { type: 'call', id: d.id, pipeline: d.pipeline!, disabled: d.disabled! };
      case 'if':
        return {
          type: 'if',
          id: d.id,
          condition: d.condition!,
          then: d.then!.map(of),
          else: d.else!.map(of),
        };
    }
  };
  const res = await api.get(`/api/pipelines/${PIPELINE}${ON}`);
  return (res.pipeline.steps as Dto[]).map(of);
}

/** What a card shows of a step, as the model expects it and the screen is read. */
export type Shown =
  | { type: 'if'; id: string; condition: string; paused: boolean; then: Shown[]; else: Shown[] }
  | {
      type: string;
      id: string;
      summary: string;
      disabled: boolean;
      open: boolean;
      paused: boolean;
    };

export function expected(model: Model, steps = model.editing): Shown[] {
  return steps.map((s): Shown => {
    const paused = model.paused === s.id;
    if (s.type === 'if')
      return {
        type: 'if',
        id: s.id,
        condition: s.condition,
        paused,
        then: expected(model, s.then),
        else: expected(model, s.else),
      };
    const summary = {
      comment: () => ('text' in s && s.text) || '(empty comment)',
      eval: () => ('target' in s ? `${s.target || '…'} = ${s.expr || '…'}` : ''),
      drop: () => ('condition' in s ? `drop where ${s.condition || '…'}` : ''),
      call: () => ('pipeline' in s ? s.pipeline : ''),
    }[s.type]();
    return {
      type: LABELS[s.type],
      id: s.id,
      summary,
      disabled: 'disabled' in s && s.disabled,
      open: !!model.open.get(s.id),
      paused,
    };
  });
}

/** What the Paused chip says, as the model expects it: the step's place in the editor's numbering and its type. */
export function expectedPause(model: Model): string | null {
  if (!model.sampled || !model.paused) return null;
  const all = flat(model.editing);
  const at = all.findIndex((s) => s.id === model.paused);
  return `◉ After step ${at + 1} · ${LABELS[all[at].type].toUpperCase()}`;
}

/** Every card in the editor that the step tree does not reach: one left behind by a list that moved on. Each is
 * named with the classes of the elements between it and the editor. */
export async function strayCards(page: Page): Promise<string[]> {
  return page.locator('.builder').evaluate((builder) => {
    const reached = new Set<Element>();
    const walk = (list: Element) => {
      for (const card of list.querySelectorAll(':scope > .slot > [id^="step-"]')) {
        reached.add(card);
        for (const branch of card.querySelectorAll(':scope > .branches > .branch > .step-list'))
          walk(branch);
      }
    };
    walk(builder.querySelector(':scope > .step-list')!);
    return [...builder.querySelectorAll('[id^="step-"]')]
      .filter((card) => !reached.has(card))
      .map((card) => {
        const path: string[] = [];
        for (let a: Element | null = card; a && a !== builder; a = a.parentElement)
          path.unshift(
            `${a.tagName.toLowerCase()}.${a.className.split(' ')[0]}${a.id ? `#${a.id}` : ''}`,
          );
        return path.join(' > ');
      });
  });
}

/** What the Paused chip says, or null where there is none. */
export async function pauseOnScreen(page: Page): Promise<string | null> {
  const chip = page.getByText(/^◉/);
  return (await chip.count())
    ? (await chip.first().textContent())!.replace(/[✕\uFE0E]/g, '').trim()
    : null;
}

/** The steps on screen, read from the editor's cards. */
export async function onScreenSteps(page: Page): Promise<Shown[]> {
  return page
    .locator('.step-list')
    .first()
    .evaluate((root) => {
      const read = (list: Element): unknown[] =>
        [...list.querySelectorAll(':scope > .slot > [id^="step-"]')].map((card) => {
          const id = card.id.slice('step-'.length);
          if (card.classList.contains('if-card')) {
            const [then, otherwise] = card.querySelectorAll(
              ':scope > .branches > .branch > .step-list',
            );
            return {
              type: 'if',
              id,
              condition: (
                card.querySelector(
                  ':scope > .card-head .condition :is(input, textarea)',
                ) as HTMLTextAreaElement
              ).value,
              paused: card.classList.contains('paused-at'),
              then: read(then),
              else: read(otherwise),
            };
          }
          return {
            type: card.querySelector(':scope > .card-head .type-name')!.textContent!.trim(),
            id,
            summary: card.querySelector(':scope > .card-head .summary')!.textContent!.trim(),
            disabled: card.classList.contains('step-disabled'),
            open: card.classList.contains('open'),
            paused: card.classList.contains('paused-at'),
          };
        });
      return read(root);
    }) as Promise<Shown[]>;
}
