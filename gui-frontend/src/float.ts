/**
 * A piece that floats over the page from an anchor: a list under a field, a menu under its button, a help text
 * under its `?`. It is a native popover, so it draws in the top layer and no box that scrolls or clips can cut it
 * off, and it is placed from the anchor's box on screen, since the top layer has no layout relation to the page.
 */
import { untrack } from 'svelte';
import type { Attachment } from 'svelte/attachments';

/** The box a piece hangs from, on screen. */
export type Anchor = Pick<DOMRect, 'top' | 'bottom' | 'left' | 'right'>;

export type FloatOptions = {
  /** The anchor's edge the piece lines up with. */
  edge?: 'left' | 'right';
  /** The piece is as wide as its anchor. */
  matchWidth?: boolean;
  /** The least room under the anchor the piece takes before it goes over it. By default, all of its height. */
  floor?: number;
  /** The piece stands beside the anchor, level with its top: to its right, or to its left where the right has no
   * room. A pane that tells more of what a list beside it has chosen. */
  beside?: boolean;
};

/** Between the piece and its anchor, and between the piece and the window's edge. */
const GAP = 4;
const MARGIN = 8;

/** Places `el`, a shown popover, at the anchor `at` gives: under it, or over it where less than the floor fits under
 * it and more fits over it. Its height is capped to the room on that side, under the cap its own style sets, and it
 * is kept inside the window across. It is placed again as the anchor moves, for a scroll or for the page shifting
 * under it, as the window resizes and as `el` changes size, until `stop` is called. `place` places it again, for an
 * anchor that moves on its own: a caret. */
export function follow(
  el: HTMLElement,
  at: () => Anchor | undefined,
  options: FloatOptions = {},
): { place: () => void; stop: () => void } {
  // The cap its own style sets, not the room a last open left on it.
  el.style.maxHeight = '';
  const cap = getComputedStyle(el).maxHeight;
  Object.assign(el.style, { position: 'fixed', inset: 'auto', margin: '0', overflowY: 'auto' });
  const place = () => {
    const a = at();
    if (!a) return;
    if (options.matchWidth) {
      el.style.boxSizing = 'border-box';
      el.style.width = `${a.right - a.left}px`;
    }
    if (options.beside) {
      const width = el.offsetWidth;
      const right = a.right + GAP;
      const fits = right + width + MARGIN <= window.innerWidth;
      el.style.left = `${fits ? right : Math.max(MARGIN, a.left - GAP - width)}px`;
      const top = Math.max(MARGIN, Math.min(a.top, window.innerHeight - el.offsetHeight - MARGIN));
      const room = window.innerHeight - top - MARGIN;
      Object.assign(el.style, {
        top: `${top}px`,
        bottom: 'auto',
        maxHeight: cap === 'none' ? `${room}px` : `min(${cap}, ${room}px)`,
      });
      return;
    }
    const below = window.innerHeight - a.bottom - GAP - MARGIN;
    const above = a.top - GAP - MARGIN;
    const height = el.scrollHeight;
    const under = below >= Math.min(options.floor ?? height, height) || below >= above;
    const room = Math.max(0, under ? below : above);
    el.style.maxHeight = cap === 'none' ? `${room}px` : `min(${cap}, ${room}px)`;
    el.style.top = under ? `${a.bottom + GAP}px` : 'auto';
    el.style.bottom = under ? 'auto' : `${window.innerHeight - a.top + GAP}px`;
    const width = el.offsetWidth;
    const left = options.edge === 'right' ? a.right - width : a.left;
    el.style.left = `${Math.max(MARGIN, Math.min(left, window.innerWidth - width - MARGIN))}px`;
  };
  place();
  const resized = new ResizeObserver(place);
  resized.observe(el);
  window.addEventListener('resize', place);
  // The anchor is read each frame rather than on scroll: content loading above it moves it with no event at all.
  let last = at();
  let frame = requestAnimationFrame(function watch() {
    const a = at();
    if (
      a &&
      (!last ||
        a.top !== last.top ||
        a.bottom !== last.bottom ||
        a.left !== last.left ||
        a.right !== last.right)
    )
      place();
    last = a;
    frame = requestAnimationFrame(watch);
  });
  return {
    place,
    stop: () => {
      resized.disconnect();
      window.removeEventListener('resize', place);
      cancelAnimationFrame(frame);
    },
  };
}

/** `follow` for a `popover="manual"` drawn only while it is open: shown as it is attached. What `at` reads does not
 * attach it again. `held` is given `place` while it is attached, and `undefined` after. */
export function float(
  at: () => Anchor | undefined,
  options: FloatOptions = {},
  held?: (place: (() => void) | undefined) => void,
): Attachment<HTMLElement> {
  return (el) =>
    untrack(() => {
      if (!el.matches(':popover-open')) el.showPopover();
      const { place, stop } = follow(el, at, options);
      held?.(place);
      return () => {
        held?.(undefined);
        stop();
      };
    });
}
