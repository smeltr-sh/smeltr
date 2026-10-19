/**
 * The id a new object is offered on the Build pages.
 *
 * An id is unique within the bundle that runs, which for a group is its own subtree merged with `shared/` (design
 * doc, Config plane). The leader refuses a save that collides either way; these are what keep an author from
 * walking into one.
 */

/** What keeps `id` from naming a new object beside `taken`, or `null` where nothing does. */
export function idProblem(id: string, taken: Iterable<string>): string | null {
  if (id.trim() === '') return 'an id is required';
  for (const t of taken) if (t === id) return `${id} is taken`;
  return null;
}

/** `kind-n`, at the lowest `n` free of `taken`.
 *
 * The kind leads because `source-4` says nothing about what it is, and a page of them read alike whether they were
 * Kafka or smeltr's own metrics. The number is the lowest free one rather than the count, which handed out an id
 * the list already held as soon as an earlier one had been removed. */
export function suggestId(kind: string, taken: Iterable<string>): string {
  const used = new Set(taken);
  for (let n = 1; ; n += 1) {
    const id = `${kind}-${n}`;
    if (!used.has(id)) return id;
  }
}

/** The alphabet minted IDs draw from. Never read aloud or typed, so it is not trimmed for lookalikes. */
const TOKEN_CHARS = 'abcdefghijklmnopqrstuvwxyz0123456789';
/** Characters of randomness in a minted id. Thirty-six to the eighth is far past the objects one bundle holds, and a
 * collision is refused by the leader rather than silently taken. */
const TOKEN_LEN = 8;

/** A route's id, minted once when the route is created and never derived from anything about it.
 *
 * A route has no name its author gives it or reads back -- the Routes page shows the
 * source, the pipeline and the destinations, never the id. So the id exists only to be referred to: by a diff telling
 * an edit from a delete-and-insert, and by the `route` label on a metric about it.
 *
 * That rules out the two cheaper answers. A number counted off the list recycles -- delete `route-2`, add a route, and
 * the new one inherits the deleted one's durable history. A hash of the route's own fields changes the moment the
 * route is edited, which loses the history exactly when it is worth keeping. Random needs no coordination, so two
 * branches minting at once do not collide on merge. */
export function mintRouteId(): string {
  return mintId('route');
}

function mintId(kind: string): string {
  const bytes = new Uint8Array(TOKEN_LEN);
  crypto.getRandomValues(bytes);
  let token = '';
  for (const b of bytes) token += TOKEN_CHARS[b % TOKEN_CHARS.length];
  return `${kind}-${token}`;
}

/** A pipeline's identity is independent of its editable display name and survives renames. */
export function mintPipelineId(): string {
  return mintId('pipeline');
}

/** A sample's identity: a test names its sample by it, so it outlives a rename of the sample. Samples saved
 * before ids were minted keep their name as their id, which is as valid. */
export function mintSampleId(): string {
  return mintId('sample');
}
