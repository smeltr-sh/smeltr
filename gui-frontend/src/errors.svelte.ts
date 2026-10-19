/**
 * What the leader refused or failed to answer, held for the error bar (`ErrorBar.svelte`).
 *
 * Every call goes through `apiJson` (`api.ts`), which reports here, so a page shows its errors by being a page.
 * Each entry is one call, by its method and URL: a call that fails again replaces its entry rather than adding
 * one, and a call that succeeds clears it. A call that keeps its error beside its own control is `quiet` and never
 * reaches here; a failure that leaves the reader believing something happened that did not is `severe`, and the
 * bar opens it as a popup first.
 *
 * Errors a page shows itself (a `Validation`'s fields, a pipeline's steps, a branch that moved) are claimed: the
 * ones on a field or a step stay in their entry as links to them, so the bar says where each is without saying it
 * twice.
 */

import type { ApiError } from './api';

/** How a call's failure is shown: in the bar, beside its own control, or as a popup that collapses into the bar. */
export type Weight = 'bar' | 'quiet' | 'severe';

/** One call, as it was made. */
export interface Call {
  /** Its method, its path, and the branch, scope and commit it was about. Two reads of one path in different scopes
   * are two calls; a poll that moves its cursor is one. */
  readonly key: string;
  /** The page it was made from, so a slow answer is filed where it was asked. */
  readonly page: string;
  readonly weight: Weight;
}

/** An error a page shows on its own field or step, and how to take the reader there. */
export interface Placed {
  /** What holds it: an item's id, a step's summary. */
  readonly where: string;
  readonly message: string;
  readonly go: () => void;
}

export interface ErrorEntry {
  readonly key: string;
  readonly page: string;
  /** In the order it was reported, so a dialog shows only what was reported while it was open. */
  readonly seq: number;
  readonly severe: boolean;
  /** What the page does not show. */
  readonly errors: ApiError[];
  /** What it shows on a field or a step. Read when drawn, so an error edited away leaves the bar too. */
  readonly placed: (() => Placed[]) | null;
  /** The answer the errors came in, by identity, so a page can claim what it shows. */
  readonly answer: ApiError[] | null;
}

/** The page a hash names, without its query: what a call was made from, and what leaving clears. */
export function pageOf(hash: string): string {
  return hash.split('?')[0];
}

/** The query parameters that say which thing a call is about, rather than how much of it. */
const WHICH = new Set(['branch', 'scope', 'at']);

/** A failure already in the bar. A page that catches one has nothing left to show. */
export class Reported extends Error {}

class Errors {
  /** Raw, so an entry's `answer` keeps its identity: a proxy of it would never equal the list a page holds.
   * Every change replaces the list. */
  entries: ErrorEntry[] = $state.raw([]);
  #told = new Set<number>();
  #seq = 0;

  get seq(): number {
    return this.#seq;
  }

  /** A call about to be made. */
  call(method: string, url: string, weight: Weight): Call {
    const page = typeof location === 'undefined' ? '' : pageOf(location.hash);
    const [path, query = ''] = url.split('?');
    const about = [...new URLSearchParams(query)].filter(([k]) => WHICH.has(k)).sort();
    const which = about.length > 0 ? `?${new URLSearchParams(about)}` : '';
    return { key: `${method.toUpperCase()} ${path}${which}`, page, weight };
  }

  /** `call` failed with `errors`. `answer` is the list the leader sent, when it sent one. */
  report(call: Call, errors: ApiError[], answer: ApiError[] | null = null): void {
    if (call.weight === 'quiet') return;
    const entry: ErrorEntry = {
      key: call.key,
      page: call.page,
      seq: ++this.#seq,
      severe: call.weight === 'severe',
      errors,
      placed: null,
      answer,
    };
    this.entries = [...this.entries.filter((e) => e.key !== call.key), entry];
  }

  /** `call` succeeded: what it reported last no longer holds. */
  cleared(call: Call): void {
    if (this.entries.some((e) => e.key === call.key))
      this.entries = this.entries.filter((e) => e.key !== call.key);
  }

  dismiss(entry: ErrorEntry): void {
    this.entries = this.entries.filter((e) => e !== entry);
  }

  /** The severe entries not told yet, which count as told from here: each is a popup once. */
  untold(): ErrorEntry[] {
    const fresh = this.entries.filter((e) => e.severe && !this.#told.has(e.seq));
    for (const e of fresh) this.#told.add(e.seq);
    return fresh;
  }

  /** The page shows `answer`'s errors but `rest`, the ones on a field or a step as `placed` lists them. */
  claim(answer: ApiError[], placed: (() => Placed[]) | null, rest: ApiError[]): void {
    if (!this.entries.some((e) => e.answer === answer)) return;
    this.entries = this.entries.map((e) =>
      e.answer === answer ? { ...e, errors: rest, placed } : e,
    );
  }

  /** The page shows all of `answer` itself, as a branch that moved is shown with both versions. */
  shown(answer: ApiError[]): void {
    this.claim(answer, null, []);
  }

  /** The reader is on `page` now. What another page reported describes what is no longer on screen, unless a
   * popup is still owed or holds in the bar. */
  left(page: string): void {
    const kept = this.entries.filter((e) => e.page === page || e.severe);
    if (kept.length !== this.entries.length) this.entries = kept;
  }
}

export const errors = new Errors();

/** An error with only a message: a call that threw rather than answered. */
export function bareError(message: string): ApiError {
  return {
    pipeline: null,
    step: null,
    function: null,
    assignmentIndex: null,
    item: null,
    field: null,
    rule: null,
    message,
  };
}

/** For a page's `catch`: a failure `apiJson` reported is in the bar already, and anything else (a bug, a file
 * that did not parse) is put there, so nothing a page catches goes unseen. */
export function reported(e: unknown): void {
  if (e instanceof Reported) return;
  errors.report(
    errors.call('PAGE', typeof location === 'undefined' ? '' : pageOf(location.hash), 'bar'),
    [bareError(e instanceof Error ? e.message : String(e))],
  );
}

/** What a page's `catch` shows beside its control: nothing for a failure already in the bar, else its message. */
export function unreported(e: unknown): string {
  if (e instanceof Reported) return '';
  return e instanceof Error ? e.message : String(e);
}
