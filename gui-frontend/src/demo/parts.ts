// Where the recording keeps each part (`leader.ts`'s `Parts`): a file named for its key, since a key is a URL or a
// path. `tools/screenshots/recording.mjs` names them the same way.

/** The file that holds `key`'s part, if any does. */
export async function partFile(key: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(key));
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
  return `recorded-${hex.slice(0, 16)}.json`;
}
