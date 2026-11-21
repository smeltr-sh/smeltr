/**
 * Moving the pipeline's steps: ↑ and ↓ through the outline (`stepAside`), and a step held, dragged and dropped
 * between any two steps at any depth. Every step list in a pipeline shares the one `StepMoves` the outermost list
 * makes (`stepMoves`).
 */

import { getContext, setContext, tick } from 'svelte';
import type { Attachment } from 'svelte/attachments';
import { flatSteps, moveStep, placeOf, stepAside, type Step } from './pipeline';
import { glideCards, lessMotion } from './glide';

const CONTEXT = Symbol('step moves');

/** How long a press holds a step before it picks it up, so a click or a slip of the mouse never moves one. */
const HOLD_MS = 400;
/** How far the pointer may wander during the hold before the press is a click or a scroll instead. */
const SLOP_PX = 5;
/** How near the edge of the steps' scrolling box the pointer must be for the dragged step to scroll it, toward a gap
 * out of sight. */
const EDGE_PX = 48;

/** The box that scrolls `el`: its nearest ancestor that scrolls up and down, or the page. */
function scroller(el: Element | null): Element {
  for (let at = el?.parentElement; at; at = at.parentElement)
    if (/auto|scroll/.test(getComputedStyle(at).overflowY) && at.scrollHeight > at.clientHeight)
      return at;
  return document.scrollingElement ?? document.documentElement;
}

/** Where a dropped step goes: before the step at `index` of `list`, or at its end. */
export type Drop = { list: Step[]; index: number };

export class StepMoves {
  /** The step being dragged, once held; null otherwise. */
  dragging = $state<Step | null>(null);
  /** How tall the dragged step's head is; the gap it would drop into opens to this height. */
  height = $state(0);
  /** Where it would land if dropped now; null when the pointer is over no place it can go. */
  over = $state<Drop | null>(null);
  /** The pointer's position, which the dragged step's chip follows. */
  at = $state({ x: 0, y: 0 });
  /** True while steps land from a move: the rows take their new places at once and the cards glide there. */
  landing = $state(false);
  /** Where the step just moved now is, for a screen reader to say. */
  said = $state('');

  #steps: () => Step[];
  /** The step list each element shows, so the element under the pointer finds its list. */
  #lists = new WeakMap<Element, Step[]>();

  constructor(steps: () => Step[]) {
    this.#steps = steps;
  }

  /** Moves `step` one place up or down the outline (`stepAside`). A control of its head that had focus has it again
   * after: a step that changes lists is drawn anew in its new list, and its old controls go with the old drawing. */
  async aside(step: Step, by: -1 | 1) {
    const head = `#step-${CSS.escape(step.id)} > .card-head`;
    const focused = document.activeElement?.closest(head) ? document.activeElement : null;
    // A focused control without a label of its own is taken to be a choice in the More menu; the menu's button takes
    // the focus back.
    const label = focused && (focused.getAttribute('aria-label') ?? 'More actions');
    const card = document.getElementById(`step-${step.id}`);
    if (!card) return;
    await this.#glideMoved(step, card.getBoundingClientRect(), 1, () =>
      stepAside(this.#steps(), step, by),
    );
    const control = label && document.querySelector<HTMLElement>(`${head} [aria-label="${label}"]`);
    if (!control) return;
    // Brought into sight only once its card has stopped gliding, since focus would scroll to where the card is drawn
    // mid-glide.
    control.focus({ preventScroll: true });
    const moved = document.getElementById(`step-${step.id}`);
    await Promise.allSettled(moved?.getAnimations().map((a) => a.finished) ?? []);
    control.scrollIntoView({ block: 'nearest' });
  }

  /** Makes `move`, then glides each step's card from where it was to its new place. `step`'s card glides from `from`
   * instead, fading in from `opacity` and drawn over the other cards. A card may be drawn anew in another list, so the
   * cards glide, not their rows; each branch's + step row glides with them. */
  async #glideMoved(step: Step, from: DOMRect, opacity: number, move: () => boolean) {
    const moved = `step-${step.id}`;
    const cards = () => [...document.querySelectorAll<HTMLElement>('.card[id^="step-"]')];
    const addRows = () =>
      [...document.querySelectorAll<HTMLElement>('.step-list')].flatMap((el) => {
        const list = this.#lists.get(el);
        const row = el.querySelector<HTMLElement>(':scope > .add-row');
        return list && row ? [[list, row] as const] : [];
      });
    // The steps inside a dragged If are not drawn while it is lifted out; they land with it.
    const drawn = (el: Element) => el.getClientRects().length > 0;
    const cardsWere = new Map(
      cards()
        .filter(drawn)
        .map((c) => [c.id, c.getBoundingClientRect()]),
    );
    cardsWere.set(moved, from);
    const rowsWere = new Map(
      addRows()
        .filter(([, row]) => drawn(row))
        .map(([list, row]) => [list, row.getBoundingClientRect()]),
    );
    this.landing = true;
    const done = move();
    this.said = '';
    await tick();
    if (done) this.said = this.#whereIs(step);
    // The rows read `landing` when their glides start, in the flush above, so it clears only on the next frame.
    requestAnimationFrame(() => (this.landing = false));
    const glides = new Map<HTMLElement, { from: DOMRect; opacity?: number; above?: boolean }>();
    for (const card of cards()) {
      const was = cardsWere.get(card.id);
      if (was)
        glides.set(card, card.id === moved ? { from: was, opacity, above: true } : { from: was });
    }
    for (const [list, row] of addRows()) {
      const was = rowsWere.get(list);
      // The + step row at the end of the outermost list stays put, as does everything below the steps.
      if (was && list !== this.#steps()) glides.set(row, { from: was });
    }
    glideCards(glides);
  }

  /** Marks an element as showing `list`: a step dropped over it lands in `list`, between the slots (`.slot`) the
   * pointer falls between. */
  shows(list: Step[]): Attachment<HTMLElement> {
    return (el) => {
      this.#lists.set(el, list);
      return () => this.#lists.delete(el);
    };
  }

  /** Makes an element the handle of `step`: a press on it held still for `HOLD_MS` picks the step up, and from then
   * the pointer drags it until release drops it or Esc puts it back. `step` returns null where this handle moves
   * nothing. */
  holds(step: () => Step | null): Attachment<HTMLElement> {
    return (el) => {
      const down = (e: PointerEvent) => {
        const s = step();
        if (s) this.#press(e, s);
      };
      el.addEventListener('pointerdown', down);
      return () => el.removeEventListener('pointerdown', down);
    };
  }

  #press(e: PointerEvent, step: Step) {
    if (e.button !== 0 || (e.target as Element).closest('input, textarea, select, .controls'))
      return;
    const start = { x: e.clientX, y: e.clientY };
    // The card presses in as the hold runs, and springs back if the press ends first.
    const card = (e.currentTarget as Element).closest<HTMLElement>('.card');
    const pressing = lessMotion()
      ? null
      : card?.animate([{ transform: 'none' }, { transform: 'scale(0.98)' }], {
          duration: HOLD_MS,
          easing: 'ease-in',
        });
    const timer = setTimeout(() => {
      pressing?.cancel();
      this.#pickUp(step, start);
    }, HOLD_MS);
    const wander = (m: PointerEvent) => {
      if (Math.hypot(m.clientX - start.x, m.clientY - start.y) > SLOP_PX) quit();
    };
    // A long touch opens no menu while it holds or drags the step.
    const menu = (c: Event) => c.preventDefault();
    const quit = () => {
      if (pressing?.playState === 'running') pressing.reverse();
      clearTimeout(timer);
      removeEventListener('pointermove', wander);
      removeEventListener('pointerup', quit);
      removeEventListener('pointercancel', quit);
      removeEventListener('contextmenu', menu, true);
    };
    addEventListener('contextmenu', menu, true);
    addEventListener('pointermove', wander);
    addEventListener('pointerup', quit);
    addEventListener('pointercancel', quit);
  }

  #pickUp(step: Step, at: { x: number; y: number }) {
    // An open step, or an If with its branches, opens a gap only as tall as its head, so the list it is dragged over
    // stays short.
    const card = document.getElementById(`step-${step.id}`);
    const head = card?.querySelector(':scope > .card-head');
    this.height =
      card && head
        ? head.getBoundingClientRect().height + card.offsetHeight - card.clientHeight
        : 0;
    this.dragging = step;
    this.at = at;
    this.over = null;
    // The press ends in a click on the step's head, which would open or close it.
    const swallow = (c: MouseEvent) => {
      c.stopPropagation();
      c.preventDefault();
    };
    // A finger that drags a held step moves the step, not the page: the browser would take the move for a scroll
    // and cancel the drag.
    const pan = (t: TouchEvent) => t.preventDefault();
    // A step picked up near an edge scrolls nothing until the pointer sets off.
    let setOff = false;
    const aim = () => {
      const over = this.#dropAt(step, this.at.x, this.at.y);
      if (over?.list !== this.over?.list || over?.index !== this.over?.index) this.over = over;
    };
    const move = (m: PointerEvent) => {
      setOff ||= Math.hypot(m.clientX - at.x, m.clientY - at.y) > SLOP_PX;
      this.at = { x: m.clientX, y: m.clientY };
      aim();
    };
    const box = scroller(document.elementFromPoint(at.x, at.y));
    // Each frame, the drop is read again, because the steps under a still pointer move as they scroll and as the gaps
    // open and close; at the edges the loop also scrolls.
    const follow = () => {
      aim();
      const view =
        box === document.scrollingElement
          ? { top: 0, bottom: innerHeight }
          : box.getBoundingClientRect();
      const y = this.at.y;
      const by =
        y < view.top + EDGE_PX
          ? y - view.top - EDGE_PX
          : y > view.bottom - EDGE_PX
            ? y - view.bottom + EDGE_PX
            : 0;
      if (setOff && by !== 0) box.scrollBy(0, by / 4);
      frame = requestAnimationFrame(follow);
    };
    let frame = requestAnimationFrame(follow);
    const end = (drop: boolean) => {
      // From the chip, the step glides to where it drops, or back where it was, and unfolds from the height of the
      // gap it had.
      const to = drop ? this.over : null;
      const chip = document.querySelector('.carried-chip')?.getBoundingClientRect();
      void this.#glideMoved(
        step,
        new DOMRect(chip?.x ?? at.x, chip?.y ?? at.y, chip?.width ?? 0, this.height),
        0.4,
        () => to !== null && moveStep(this.#steps(), step, to.list, to.index),
      );
      this.dragging = null;
      this.over = null;
      cancelAnimationFrame(frame);
      removeEventListener('touchmove', pan);
      removeEventListener('pointermove', move);
      removeEventListener('pointerup', up);
      removeEventListener('pointercancel', cancel);
      removeEventListener('keydown', key, true);
      setTimeout(() => removeEventListener('click', swallow, true));
      root.removeProperty('user-select');
      root.removeProperty('-webkit-user-select');
    };
    const up = () => end(true);
    const cancel = () => end(false);
    const key = (k: KeyboardEvent) => {
      if (k.key !== 'Escape') return;
      k.stopPropagation();
      end(false);
    };
    addEventListener('touchmove', pan, { passive: false });
    addEventListener('pointermove', move);
    addEventListener('pointerup', up);
    addEventListener('pointercancel', cancel);
    addEventListener('keydown', key, true);
    addEventListener('click', swallow, true);
    // The press that picked the step up would select the text it is dragged over.
    getSelection()?.removeAllRanges();
    const root = document.documentElement.style;
    root.setProperty('user-select', 'none');
    root.setProperty('-webkit-user-select', 'none');
  }

  /** Where `step` is, as a screen reader says it after a move: its place in its list, and the branch and If that
   * hold the list, if any. */
  #whereIs(step: Step): string {
    const at = placeOf(this.#steps(), step);
    if (!at) return '';
    const place = `Moved to step ${at.index + 1} of ${at.list.length}`;
    if (!at.owner) return `${place}.`;
    return `${place} in the ${at.owner.branch} branch of If ${at.owner.step.condition}.`;
  }

  /** The drop under the pointer: in the innermost list there, before the first of its steps whose middle is below
   * the pointer. None inside the dragged step itself. */
  #dropAt(step: Step, x: number, y: number): Drop | null {
    let el = document.elementFromPoint(x, y);
    while (el && !this.#lists.has(el)) el = el.parentElement;
    if (!el) return null;
    const list = this.#lists.get(el)!;
    if (flatSteps([step]).some((s) => s.type === 'if' && (s.then === list || s.else === list)))
      return null;
    const slots = [...el.querySelectorAll(':scope > .slot, :scope > .step-list > .slot')];
    const index = slots.filter((s) => {
      // The step's card, not its slot: the slot holds the gap above it too, which opens as the pointer comes near. The
      // dragged step's card is lifted out, and its empty slot stands where it was.
      const r = (s.querySelector(':scope > .card:not(.carried)') ?? s).getBoundingClientRect();
      return r.top + r.height / 2 < y;
    }).length;
    return { list, index };
  }
}

/** The `StepMoves` of the pipeline a step list is in. The outermost list makes it, over its `steps`, and says so with
 * `outermost`. */
export function stepMoves(steps: () => Step[]): { moves: StepMoves; outermost: boolean } {
  const moves = getContext<StepMoves | undefined>(CONTEXT);
  if (moves) return { moves, outermost: false };
  return { moves: setContext(CONTEXT, new StepMoves(steps)), outermost: true };
}
