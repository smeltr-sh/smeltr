/**
 * A list editor's save errors, placed on the fields they are about.
 *
 * The leader names the item and the field an error is about (`ApiError`), by
 * the names the file uses, so placing one never reads its message. A page makes
 * one `Validation` for its section, saves through it, and wraps each item in a
 * `ValidationItem`; a `Field` given a `field` name inside one shows that field's
 * errors and opens its item to be focused. What no field here takes stays in
 * the error bar (`errors.svelte.ts`), where each placed error is a link to its
 * field.
 */

import { getContext, setContext, tick } from 'svelte';
import type { ApiError, MutationResult } from './api';
import { errors } from './errors.svelte';

const VALIDATION = Symbol('validation');
const ITEM = Symbol('validation item');

/** A field of an item, and for a setting of a route's delivery rule, the rule's position (`ApiError.rule`). */
const keyOf = (id: string, field: string, rule: number | null = null) =>
  JSON.stringify([id, field, rule]);
const keyOfError = (e: Placed) => keyOf(e.item.id, e.field, e.rule);

/** An error this page places: one about a field of an item in its section. */
export type Placed = ApiError & { item: { section: string; id: string }; field: string };

export class Validation {
  /** The errors on this page's fields, from the last save. */
  placed: Placed[] = $state([]);
  /** Counts the saves answered: what `placed` names by position, such as a rule, is where it was at this count. */
  answered = $state(0);
  #focusers = new Map<string, () => void>();
  /** What opens an item to show its fields: the item's card, and any
   * disclosure inside it. */
  #openers = new Map<string, Set<() => void>>();
  /** The edits and renames made while a save is out, replayed on what it
   * refuses: its errors are about what was sent, not what is on screen. */
  #pending: (() => void)[] | null = null;

  constructor(readonly section: string) {}

  /** Runs `request`, a save of this page's section, and puts what it refuses
   * on the fields it names, focusing the first. */
  async save<T extends MutationResult>(request: () => Promise<T>): Promise<T> {
    this.placed = [];
    const pending: (() => void)[] = [];
    this.#pending = pending;
    let res: T;
    try {
      res = await request();
    } finally {
      if (this.#pending === pending) this.#pending = null;
    }
    if (res.ok) return res;
    const ours = (e: ApiError): e is Placed => e.item?.section === this.section && e.field !== null;
    this.placed = res.errors.filter(ours);
    this.answered++;
    for (const replay of pending) replay();
    if (this.placed.length > 0) void this.focus(this.placed[0]);
    const rest = res.errors.filter((e) => !ours(e));
    errors.claim(
      res.errors,
      () =>
        this.placed.map((e) => ({
          where: e.item.id,
          message: e.message,
          go: () => void this.focus(e),
        })),
      rest,
    );
    return res;
  }

  /** Drops the last save's errors, as a discard leaves nothing they describe. */
  clear(): void {
    this.placed = [];
  }

  /** The messages on one field, or null. */
  message(id: string, field: string, rule: number | null = null): string | null {
    const key = keyOf(id, field, rule);
    const messages = this.placed.filter((e) => keyOfError(e) === key).map((e) => e.message);
    return messages.length > 0 ? messages.join('\n') : null;
  }

  /** An edit to a field: its errors no longer describe it. */
  edited(id: string, field: string, rule: number | null = null): void {
    this.#pending?.push(() => this.edited(id, field, rule));
    const key = keyOf(id, field, rule);
    if (this.placed.some((e) => keyOfError(e) === key)) {
      this.placed = this.placed.filter((e) => keyOfError(e) !== key);
    }
  }

  /** True while a save is out: an edit now is replayed on its answer, by position. */
  get saving(): boolean {
    return this.#pending !== null;
  }

  /** Drops these errors, as an edit to what they are about does. */
  drop(errors: Placed[]): void {
    if (errors.length > 0) this.placed = this.placed.filter((e) => !errors.includes(e));
  }

  /** Item `from` is now `to`: its errors go with it. */
  renamed(from: string, to: string): void {
    this.#pending?.push(() => this.renamed(from, to));
    if (this.placed.some((e) => e.item.id === from)) {
      this.placed = this.placed.map((e) =>
        e.item.id === from ? { ...e, item: { ...e.item, id: to } } : e,
      );
    }
  }

  /** A field on screen. Returns its removal. */
  mount(id: string, field: string, focus: () => void, rule: number | null = null): () => void {
    const key = keyOf(id, field, rule);
    this.#focusers.set(key, focus);
    return () => {
      if (this.#focusers.get(key) !== focus) return;
      this.#focusers.delete(key);
    };
  }

  /** Something that opens item `id`. Returns its removal. */
  opener(id: string, open: () => void): () => void {
    const set = this.#openers.get(id) ?? new Set();
    this.#openers.set(id, set);
    set.add(open);
    return () => set.delete(open);
  }

  /** Opens what hides `error`'s field and focuses it. A card mounts the
   * disclosures inside it only once open, so this opens, waits, and asks
   * again until the field is there or nothing more opens. */
  async focus(error: Placed): Promise<void> {
    const key = keyOfError(error);
    const opened = new Set<() => void>();
    for (;;) {
      const field = this.#focusers.get(key);
      if (field) return field();
      const next = [...(this.#openers.get(error.item.id) ?? [])].filter((o) => !opened.has(o));
      if (next.length === 0) return;
      for (const open of next) {
        opened.add(open);
        open();
      }
      await tick();
    }
  }
}

/** Makes `validation` the one every field below the calling component reads. */
export function provideValidation(section: string): Validation {
  return setContext(VALIDATION, new Validation(section));
}

/** The item a field below belongs to, by a getter so a renamed id follows. */
export function provideItem(id: () => string): void {
  setContext(ITEM, id);
}

/** The page's validation, when the calling component is inside one. */
export function pageValidation(): Validation | null {
  return getContext<Validation | undefined>(VALIDATION) ?? null;
}

/** The page's validation and the item in hand, when a field is inside both. */
export function validationContext(): { validation: Validation; id: () => string } | null {
  const validation = pageValidation();
  const id = getContext<(() => string) | undefined>(ITEM);
  return validation && id ? { validation, id } : null;
}
