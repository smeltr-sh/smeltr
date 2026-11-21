/**
 * Moving the pipeline's steps: ↑ and ↓ through the outline (`stepAside`), and a step held, dragged and dropped
 * between any two steps at any depth. Every step list in a pipeline shares the one `StepMoves` the outermost list
 * makes (`stepMoves`).
 */

import { getContext, setContext } from 'svelte';
import type { Attachment } from 'svelte/attachments';
import { flatSteps, moveStep, stepAside, type Step } from './pipeline';

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
  /** Where it would land if dropped now; null when the pointer is over no place it can go. */
  over = $state<Drop | null>(null);
  /** The pointer's position, which the dragged step's chip follows. */
  at = $state({ x: 0, y: 0 });

  #steps: () => Step[];
  /** The step list each element shows, so the element under the pointer finds its list. */
  #lists = new WeakMap<Element, Step[]>();

  constructor(steps: () => Step[]) {
    this.#steps = steps;
  }

  /** Moves `step` one place up or down the outline (`stepAside`). */
  aside(step: Step, by: -1 | 1) {
    stepAside(this.#steps(), step, by);
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
    const timer = setTimeout(() => this.#pickUp(step, start), HOLD_MS);
    const wander = (m: PointerEvent) => {
      if (Math.hypot(m.clientX - start.x, m.clientY - start.y) > SLOP_PX) quit();
    };
    const quit = () => {
      clearTimeout(timer);
      removeEventListener('pointermove', wander);
      removeEventListener('pointerup', quit);
      removeEventListener('pointercancel', quit);
    };
    addEventListener('pointermove', wander);
    addEventListener('pointerup', quit);
    addEventListener('pointercancel', quit);
  }

  #pickUp(step: Step, at: { x: number; y: number }) {
    this.dragging = step;
    this.at = at;
    this.over = null;
    // The press ends in a click on the step's head, which would open or close it.
    const swallow = (c: MouseEvent) => {
      c.stopPropagation();
      c.preventDefault();
    };
    // A step picked up near an edge scrolls nothing until the pointer sets off.
    let setOff = false;
    const move = (m: PointerEvent) => {
      setOff ||= Math.hypot(m.clientX - at.x, m.clientY - at.y) > SLOP_PX;
      this.at = { x: m.clientX, y: m.clientY };
      this.over = this.#dropAt(step, m.clientX, m.clientY);
    };
    // The steps under a still pointer change as they scroll.
    const scrolled = () => {
      this.over = this.#dropAt(step, this.at.x, this.at.y);
    };
    const box = scroller(document.elementFromPoint(at.x, at.y));
    const edge = () => {
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
      frame = requestAnimationFrame(edge);
    };
    let frame = requestAnimationFrame(edge);
    const end = (drop: boolean) => {
      if (drop && this.over) moveStep(this.#steps(), step, this.over.list, this.over.index);
      this.dragging = null;
      this.over = null;
      cancelAnimationFrame(frame);
      removeEventListener('scroll', scrolled, true);
      removeEventListener('pointermove', move);
      removeEventListener('pointerup', up);
      removeEventListener('pointercancel', cancel);
      removeEventListener('keydown', key, true);
      setTimeout(() => removeEventListener('click', swallow, true));
    };
    const up = () => end(true);
    const cancel = () => end(false);
    const key = (k: KeyboardEvent) => {
      if (k.key !== 'Escape') return;
      k.stopPropagation();
      end(false);
    };
    addEventListener('scroll', scrolled, true);
    addEventListener('pointermove', move);
    addEventListener('pointerup', up);
    addEventListener('pointercancel', cancel);
    addEventListener('keydown', key, true);
    addEventListener('click', swallow, true);
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
      const r = s.getBoundingClientRect();
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
