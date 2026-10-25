import { flip } from 'svelte/animate';
import type { AnimationConfig } from 'svelte/animate';

/** Whether the reader asked for less motion. */
export const lessMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

/** How long a row glides to its new place when its list reorders, so the eye follows a move; none where the reader
 * asked for less motion. */
export const glide = () => (lessMotion() ? 0 : 250);

/** `animate:flip` at `glide`'s pace, for a row of a keyed `{#each}`. Once the row has landed its transform is set
 * and cleared again: Chromium can leave a glided row's hit-testing where the glide had it, so clicks on the row pass
 * through to the list behind it until its transform next changes. */
export function glideTo(node: Element, fromTo: { from: DOMRect; to: DOMRect }): AnimationConfig {
  const config = flip(node, fromTo, { duration: glide() });
  const row = node as HTMLElement;
  // Svelte starts the glide's animation after this returns.
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
  return config;
}
