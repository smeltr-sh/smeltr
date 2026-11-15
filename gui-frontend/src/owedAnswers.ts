/**
 * Whether the server still owes any field an answer that changes what a save sends. A code field, once edited, waits
 * for the layout the server sends back (`ValidatedField`), and a save waits for it rather than sending the text as it
 * was.
 *
 * Counted rather than flagged: a pipeline may hold several such fields, and each edit supersedes its own outstanding
 * answer, not the others'.
 */
let owing = 0;
let waiters: (() => void)[] = [];

export function beginReconcile(): void {
  owing += 1;
}

export function endReconcile(): void {
  owing = Math.max(0, owing - 1);
  if (owing === 0) {
    const settling = waiters;
    waiters = [];
    for (const w of settling) w();
  }
}

/** Resolves once no field has an answer outstanding. */
export function fieldsSettled(): Promise<void> {
  if (owing === 0) return Promise.resolve();
  return new Promise((resolve) => waiters.push(resolve));
}
