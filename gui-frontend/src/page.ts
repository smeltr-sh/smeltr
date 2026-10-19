// Helpers shared by the config pages.

import { errorMessage, type MutationResult } from './api';

/** `errors` is what the text itself got wrong; what the leader refused, the error bar says. */
export type JsonSaveResult = { ok: boolean; errors: string };

/** Parse `text` as a JSON array of `what`, PUT it, and hand the parsed
 * value to `accept` once the backend has taken it, with the answer that took
 * it -- the answer carries the commit the save produced, which is what the
 * page's next save has to be judged against. Backs every "Edit JSON" page. */
export async function saveArrayFromJson<T>(
  text: string,
  what: string,
  put: (items: T[]) => Promise<MutationResult>,
  accept: (items: T[], res: MutationResult) => void,
): Promise<JsonSaveResult> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    return { ok: false, errors: errorMessage(e) };
  }
  if (!Array.isArray(parsed)) return { ok: false, errors: `expected a JSON array of ${what}` };
  const res = await put(parsed as T[]);
  // The leader's refusal is the error bar's to say.
  if (!res.ok) return { ok: false, errors: '' };
  accept(parsed as T[], res);
  return { ok: true, errors: '' };
}

/** A numeric input's value, or `undefined` when it is empty. An optional
 * setting then reverts to its default rather than saving `null`, which
 * the loader rejects. */
export function numberOrUnset(input: HTMLInputElement): number | undefined {
  const n = input.valueAsNumber;
  return Number.isNaN(n) ? undefined : n;
}
