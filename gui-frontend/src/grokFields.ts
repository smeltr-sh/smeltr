/**
 * Whether the server still owes any step an answer that changes what a save sends.
 *
 * A grok step's fields are its pattern's captures, which only the server can
 * report, so filling them in is asynchronous and debounced. A save that
 * serialized the step between the edit and the answer would send a `declared`
 * list the pattern disagrees with, and the compiler refuses exactly that -- so
 * the save waits here instead. A code field laid out as it is left waits the
 * same way, for the layout the server sends back (`ValidatedField`).
 *
 * Counted rather than a flag: a pipeline may hold several such steps, and each
 * edit supersedes its own outstanding answer rather than the others'.
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

/** Resolves once no grok step has a field reconciliation outstanding. */
export function fieldsSettled(): Promise<void> {
  if (owing === 0) return Promise.resolve();
  return new Promise((resolve) => waiters.push(resolve));
}
