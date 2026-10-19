/**
 * One file of the bundle, edited as a whole and saved as a whole.
 *
 * A section page -- sources, destinations, identities, lookup tables, routes --
 * reads its file out of the config, holds what the reader types until it is
 * saved, and writes the file back against the commit it read. The page keeps
 * the value itself, in `$state` of its own, which its template and its dialogs
 * edit; this holds everything around that value: the load, the saved baseline
 * and the commit it was read at, the dirty check, the edit held for the tab's
 * life (`edits.ts`), and the save with the two refusals a save can meet -- a
 * message, or a branch that moved (`SaveConflict`).
 *
 * A page that writes two files makes two of these, and syncs `readAtCommit`
 * between them when one save moves the branch the other saves against.
 */

import { onDestroy } from 'svelte';
import {
  getConfig,
  type FileConfigDto,
  type GetConfigResult,
  type MutationResult,
  type SaveConflict,
} from './api';
import { summarizeChanges, type ChangeRow } from './diff';
import { hold, verdictFor } from './edits';
import { saveArrayFromJson, type JsonSaveResult } from './page';
import type { Validation } from './validation.svelte';
import { errors, reported } from './errors.svelte';

export type SectionOptions<T> = {
  /** The value on screen: the page's own `$state`, read and written through these. */
  get: () => T;
  set: (value: T) => void;
  /** This file's part of the config. */
  pick: (config: FileConfigDto) => T;
  /** Writes the file, against the commit it was read at. */
  put: (value: T, readAtCommit: string | null) => Promise<MutationResult>;
  /** The file as the "Edit JSON" page's refusal names it, where it differs from `file`: "lookup tables". */
  what?: string;
  /** Places the errors a save refuses on the fields they name. */
  validation?: Validation;
  /** The value as the baseline and the held edit carry it. JSON unless the
   * value has a shape JSON cannot carry as is. */
  text?: (value: T) => string;
  /** `text` read back, for a held edit and a discard. */
  parse?: (text: string) => T;
  /** What is held instead of `text`, when building it reads more than the
   * dirty check should: held a moment after the last edit, and at once when
   * the page is left. `reads` names what it depends on, since it is not read
   * until then. */
  held?: { value: () => string; afterMs: number; reads: () => void };
  /** The rest of what the page reads from the config, once this file is in. */
  onLoaded?: (config: FileConfigDto, res: GetConfigResult) => void | Promise<void>;
  /** What else a discard puts back. */
  onDiscard?: () => void;
};

export class SectionEditor<T> {
  readonly file: string;
  readonly #opts: SectionOptions<T>;
  readonly #text: (value: T) => string;
  readonly #parse: (text: string) => T;

  loading = $state(true);
  loadFailed = $state(false);
  /** Something else in the bundle does not validate. The file still loaded,
   * so this is a banner, not a reason to hide the page. */
  bundleProblems = $state<string[]>([]);
  saving = $state(false);
  conflict = $state<SaveConflict | null>(null);
  /** The file as last read or saved, as `text` renders it. */
  saved = $state('');
  /** The commit `saved` was read at. A save carries it rather than leaning on
   * the shared base, which any other page's read can move. */
  readAtCommit: string | null = $state(null);
  /** Set when an edit held from an earlier visit was made against a different
   * file than the one just read. What is on screen stays the file; the panel
   * offers the edit. */
  moved: { value: string; rows: ChangeRow[] } | null = $state(null);
  /** The value on screen, as the baseline carries it. */
  text = $derived.by(() => this.#text(this.#opts.get()));
  isDirty = $derived(!this.loading && !this.loadFailed && this.text !== this.saved);

  /** Made where a component is made: it holds the edit through an effect of that component's. */
  constructor(file: string, opts: SectionOptions<T>) {
    this.file = file;
    this.#opts = opts;
    this.#text = opts.text ?? ((value) => JSON.stringify(value));
    this.#parse = opts.parse ?? ((text) => JSON.parse(text) as T);

    // The edit, held for the tab's life rather than this component's: leaving
    // this page destroys the component, since `App.svelte` draws one page per arm
    // of its `{#if}`, and the work should outlive that.
    //
    // This is the only thing that writes the record, so there is no list of paths
    // that must remember to clear it. A page that is clean holds nothing, and
    // every way of becoming clean -- a save landing, Discard, the reader undoing
    // what they typed -- runs through here. It is left alone while the panel is
    // up, because what is on screen then is the file and not the edit.
    let pending: (() => void) | null = null;
    $effect(() => {
      pending = null;
      if (this.loading || this.loadFailed || this.moved) return;
      const held = opts.held;
      if (!held) {
        hold(file, { value: this.text, baseline: this.saved, readAtCommit: this.readAtCommit });
        return;
      }
      void [this.text, this.saved, this.readAtCommit];
      held.reads();
      pending = () =>
        hold(file, { value: held.value(), baseline: this.saved, readAtCommit: this.readAtCommit });
      const later = setTimeout(() => {
        pending?.();
        pending = null;
      }, held.afterMs);
      return () => clearTimeout(later);
    });
    onDestroy(() => pending?.());
  }

  async load(): Promise<void> {
    this.loading = true;
    this.loadFailed = false;
    this.bundleProblems = [];
    try {
      const res = await getConfig();
      if (!(await this.adopt(res))) this.loadFailed = true;
    } catch (e) {
      this.loadFailed = true;
      reported(e);
    } finally {
      this.loading = false;
    }
  }

  /** Takes this file out of a config read, with the held edit and the rest the
   * page reads from it. False when the read carried no config. A page that
   * writes two files loads through one editor and adopts the same read into the
   * other from `onLoaded`. */
  async adopt(res: GetConfigResult): Promise<boolean> {
    if (!res.config) return false;
    this.#opts.set(this.#opts.pick(res.config));
    this.saved = this.text;
    this.readAtCommit = res.commit ?? null;
    this.adoptHeld();
    if (!res.ok) this.bundleProblems = res.errors.map((e) => e.message);
    await this.#opts.onLoaded?.(res.config, res);
    // An editor filled from another's read has no `load` of its own to end.
    this.loading = false;
    return true;
  }

  /** What `edits.ts` holds for this file, now that `saved` is what the file
   * says. A restore goes straight on screen. A moved edit waits in `moved` for
   * the reader to choose.
   *
   * `readAtCommit` stays the commit just read, not the one in the record. The
   * record's is what `hold` is given, so the next save after a restore carries
   * the commit its edit was typed at, but the comparison just made is the
   * stronger check and it is per file: a restore proved the file has not moved,
   * and a reader who brings a moved edit back has seen what it writes over.
   * Sending the older commit would refuse both for a save somebody made to a
   * different file on this branch. */
  private adoptHeld(): void {
    const verdict = verdictFor(this.file, this.saved);
    this.moved = null;
    if (verdict.kind === 'restore') {
      this.#opts.set(this.#parse(verdict.held.value));
    } else if (verdict.kind === 'moved') {
      this.moved = {
        value: verdict.held.value,
        rows: summarizeChanges(verdict.fresh, verdict.held.value),
      };
    }
  }

  /** Brings the moved edit back over the file. */
  restoreHeld(): void {
    if (!this.moved) return;
    this.#opts.set(this.#parse(this.moved.value));
    this.moved = null;
  }

  /** Leaves the file as it is. Clean again, so the effect forgets the record. */
  dropHeld(): void {
    this.moved = null;
  }

  discard(): void {
    this.#opts.set(this.#parse(this.saved));
    this.conflict = null;
    this.#opts.validation?.clear();
    this.#opts.onDiscard?.();
  }

  /** True when it landed. A branch that moved is not an error message: it is
   * the one refusal the writer can still act on, so it goes to `conflict`.
   * `value` is what is sent, where it is not the value on screen as it stands. */
  async save(value: T = this.#opts.get()): Promise<boolean> {
    this.saving = true;
    this.conflict = null;
    // Captured before the await: an edit made while this request is in flight must still
    // read as unsaved, not be folded into the baseline this request never sent.
    const submitted = this.#text(value);
    try {
      const request = () => this.#opts.put(value, this.readAtCommit);
      const res = await (this.#opts.validation ? this.#opts.validation.save(request) : request());
      if (res.ok) {
        this.saved = submitted;
        this.readAtCommit = res.commit ?? null;
        return true;
      }
      if (res.conflict) {
        this.conflict = res.conflict;
        errors.shown(res.errors);
      }
      return false;
    } catch (e) {
      reported(e);
      return false;
    } finally {
      this.saving = false;
    }
  }

  /** Backs the "Edit JSON" page of a file that is a list. */
  saveFromJson<U>(this: SectionEditor<U[]>, text: string): Promise<JsonSaveResult> {
    return saveArrayFromJson<U>(
      text,
      this.#opts.what ?? this.file,
      (items) => this.#opts.put(items, this.readAtCommit),
      (items, res) => {
        this.#opts.set(items);
        this.saved = this.text;
        this.readAtCommit = res.commit ?? null;
      },
    );
  }
}
