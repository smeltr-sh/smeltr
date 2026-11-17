// Where the recording keeps each part (`leader.ts`'s `Parts`): a file named for its key, since a key is a URL or a
// path. `tools/screenshots/recording.mjs` names them by this too, which is why it is plain JavaScript: Node loads it
// without stripping types.

/** The part that holds every point the staged fleet's leader stored (`series.ts`). */
export const SERIES_PART = 'series';

/**
 * The file that holds `key`'s part, if any does.
 * @param {string} key
 * @returns {Promise<string>}
 */
export async function partFile(key) {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key));
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
  return `recorded-${hex.slice(0, 16)}.json`;
}
