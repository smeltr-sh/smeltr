/** How long a row glides to its new place when its list reorders, so the eye follows a move; none where the reader
 * asked for less motion. For `animate:flip`'s `duration`. */
export const glide = () => (matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 250);
