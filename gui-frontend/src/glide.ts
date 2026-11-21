import { flip } from 'svelte/animate';
import type { AnimationConfig } from 'svelte/animate';

/** Whether the reader asked for less motion. */
export const lessMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

/** How long a row glides to its new place when its list reorders, so the eye follows a move; none where the reader
 * asked for less motion. */
export const glide = () => (lessMotion() ? 0 : 250);

/** Once `row`'s glides have run, sets its transform and clears it again: Chromium can leave a glided row's
 * hit-testing where the glide had it, so clicks on the row pass through to the list behind it until its transform
 * next changes. */
function nudgeOnceLanded(row: HTMLElement) {
  // Svelte starts a glide's animation after the frame it is asked for in.
  requestAnimationFrame(() =>
    Promise.all(row.getAnimations().map((a) => a.finished)).then(
      () => {
        const transform = row.style.transform;
        row.style.transform = 'none';
        void row.offsetHeight;
        row.style.transform = transform;
      },
      // Cut short by another glide, which nudges the row when it lands.
      () => {},
    ),
  );
}

/** `animate:flip` at `glide`'s pace, for a row of a keyed `{#each}`. A row marked `still` takes its new place at once,
 * for a move whose cards glide themselves (`glideCards`). */
export function glideTo(
  node: Element,
  fromTo: { from: DOMRect; to: DOMRect },
  params?: { still?: boolean },
): AnimationConfig {
  const config = flip(node, fromTo, { duration: params?.still ? 0 : glide() });
  nudgeOnceLanded(node as HTMLElement);
  return config;
}

/** Glides each element from where it was to where it now is, fading in from its `opacity`, and drawn over the other
 * elements if it is `above`: for elements drawn anew in another place, which `glideTo` cannot follow. An element
 * inside another that glides moves with it, so it glides only by how far it moved within that one. */
export function glideCards(
  cards: Map<HTMLElement, { from: DOMRect; opacity?: number; above?: boolean }>,
) {
  if (lessMotion()) return;
  const by = new Map<HTMLElement, { x: number; y: number }>();
  for (const [card, { from }] of cards) {
    const to = card.getBoundingClientRect();
    by.set(card, { x: from.left - to.left, y: from.top - to.top });
  }
  for (const [card, { opacity = 1, above }] of cards) {
    let { x, y } = by.get(card)!;
    let outer = card.parentElement;
    while (outer && !by.has(outer)) outer = outer.parentElement;
    if (outer) {
      x -= by.get(outer)!.x;
      y -= by.get(outer)!.y;
    }
    if (Math.abs(x) < 0.5 && Math.abs(y) < 0.5 && opacity === 1) continue;
    const gliding = card.animate(
      [
        { transform: `translate(${x}px, ${y}px)`, opacity },
        { transform: 'none', opacity: 1 },
      ],
      { duration: glide(), easing: 'cubic-bezier(0.33, 1, 0.68, 1)' },
    );
    if (above) {
      card.style.position = 'relative';
      card.style.zIndex = '1';
      const settle = () => {
        card.style.position = '';
        card.style.zIndex = '';
      };
      gliding.finished.then(settle, settle);
    }
    nudgeOnceLanded(card);
  }
}
