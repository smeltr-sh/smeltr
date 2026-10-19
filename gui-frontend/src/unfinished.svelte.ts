/**
 * The eval rows an author is still writing, held out of the live preview.
 *
 * A row is unfinished while it is wholly blank, or while one of its two boxes
 * is blank and the author has not yet left that box. Neither is a mistake yet,
 * so neither may fail the preview; a box left blank is, and its row goes back
 * in to show why. A save sends every row.
 */

import { getContext, setContext } from 'svelte';
import { SvelteSet } from 'svelte/reactivity';
import type { PreviewError, Step } from './pipeline';

type Row = { target: string; expr: string };
type Side = 'target' | 'expr';

const CONTEXT = Symbol('unfinished rows');

export class UnfinishedRows {
  #ids = new WeakMap<Row, number>();
  #next = 0;
  #left = new SvelteSet<string>();

  #key(row: Row, side: Side): string {
    let id = this.#ids.get(row);
    if (id === undefined) this.#ids.set(row, (id = this.#next++));
    return `${id}:${side}`;
  }

  /** The author left `side` of `row`. */
  left(row: Row, side: Side): void {
    this.#left.add(this.#key(row, side));
  }

  #unfinished(row: Row): boolean {
    const blank = (side: Side) => row[side].trim() === '';
    if (blank('target') && blank('expr')) return true;
    const held = (side: Side) => blank(side) && !this.#left.has(this.#key(row, side));
    return held('target') || held('expr');
  }

  /** `steps` without their unfinished rows, and `restore`, which renumbers a
   * preview error's row from the rows sent to the rows on screen. */
  hold(steps: Step[]): { steps: Step[]; restore: <E extends PreviewError>(e: E) => E } {
    const kept = new Map<string, number[]>();
    const strip = (list: Step[]): Step[] =>
      list.map((s) => {
        if (s.type === 'if') return { ...s, then: strip(s.then), else: strip(s.else) };
        if (s.type !== 'eval') return s;
        const rows = s.assignments.flatMap((row, i) => (this.#unfinished(row) ? [] : [i]));
        if (rows.length === s.assignments.length) return s;
        kept.set(s.id, rows);
        return { ...s, assignments: rows.map((i) => s.assignments[i]) };
      });
    return {
      steps: strip(steps),
      restore: <E extends PreviewError>(e: E): E => {
        const rows = e.step === null ? undefined : kept.get(e.step);
        if (!rows || e.assignmentIndex === null) return e;
        return { ...e, assignmentIndex: rows[e.assignmentIndex] ?? null };
      },
    };
  }
}

/** Makes one `UnfinishedRows` the one every step editor below marks. */
export function provideUnfinishedRows(): UnfinishedRows {
  return setContext(CONTEXT, new UnfinishedRows());
}

/** The editor's `UnfinishedRows`, when there is one. */
export function unfinishedRows(): UnfinishedRows | undefined {
  return getContext<UnfinishedRows | undefined>(CONTEXT);
}
