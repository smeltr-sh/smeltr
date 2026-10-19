/**
 * Unsaved edits, held for the tab's life rather than a component's.
 *
 * The router is one `{#if}` chain on `route.page` (`App.svelte`), so moving
 * between pages destroys the component that was open and mounts another. An
 * edit kept in that component's own state dies with it, although nothing was
 * unloaded and the tab is still there. This holds it instead, the way
 * `scope.ts` holds the last worker-group read for the same reason.
 *
 * A record is one file's worth of unsaved work: what was typed, what the file
 * held when the typing started, and the commit that reading was at. The last
 * of those is what keeps the leader's refusal real. The base commit `api.ts`
 * sends is the commit of the last adopting read, which is an editor's own read
 * for as long as that editor stays on screen; an edit that outlives its read
 * has to carry its own, or the read that would notice the branch moved is the
 * same read that adopts the new head.
 *
 * The module knows nothing about what a value holds. A page hands over strings
 * it already makes -- each keeps a serialized `saved` baseline for its own
 * dirty check -- so a value is never a live reference into a page's
 * state, which is what stops a later edit mutating the snapshot it is compared
 * against.
 *
 * One file, one record. A page that writes two files holds two, so a save that
 * lands one and refuses the other keeps the half that is still unsaved.
 */

import { BRANCH_STARTED_EVENT, currentBranch, currentScope, MAIN_BRANCH } from './api';
import { SESSION_EVENT } from './session';

/** One file's unsaved work. */
export interface Held {
  /** Which file: a section's name, or `pipelines/<id>`. */
  file: string;
  /** What a reader calls the file, where `file` holds an id nobody should
   * have to read: a pipeline's name. Absent on a section, whose key is
   * already its name. */
  label?: string;
  /** The branch it was typed on. A record on any other branch is dropped. */
  branch: string;
  /** The group whose bundle it belongs to. */
  scope: string;
  /** What was typed, serialized so the page can put it back. */
  value: string;
  /** The form `value` is compared by, where the page's own dirty check reads
   * another form than the one it restores from. The pipeline editor is the
   * case: it restores its own model of the steps and compares their
   * `stepToDto` form. Absent where the two are the same thing, which is every
   * page whose value is already what it compares. */
  signature?: string;
  /** What the file held when the typing started, in the same form as
   * `signature` -- the page's own baseline, normalized its own way. */
  baseline: string;
  /** The commit `baseline` was read at, and what a save of `value` must be
   * judged against. `null` where the read carried no commit, which sends no
   * header and skips the check, the way a first write on a fresh branch does. */
  readAtCommit: string | null;
}

/** What a page should do about a record when its own read lands. */
export type Verdict =
  /** Nothing is held, or what is held is already what the file says. */
  | { kind: 'none' }
  /** The file did not move. Put the edit back and say nothing. */
  | { kind: 'restore'; held: Held }
  /** The file moved under the edit. Say so and wait: `user-stories.md`, W9. */
  | { kind: 'moved'; held: Held; fresh: string };

/** Raised when what is held changes -- a file gains a record or loses one, not
 * every keystroke into one. The shell draws its list from this, the way it
 * draws the group's color from `SCOPE_EVENT`. */
export const EDITS_EVENT = 'smeltr:edits';

const records = new Map<string, Held>();

/** Runs `change`, and says so only if what the shell lists came out
 * different: the files held, and what each is called. A page holds on every
 * keystroke, and the shell's list does not change because a value did. */
function changing(change: () => void): void {
  const listed = () =>
    [...records].map(([key, r]) => `${key}\u0001${r.label ?? ''}`).join('\u0000');
  const before = listed();
  change();
  if (listed() === before) return;
  if (typeof window !== 'undefined') window.dispatchEvent(new CustomEvent(EDITS_EVENT));
}

function keyOf(branch: string, scope: string, file: string): string {
  return `${branch}::${scope}::${file}`;
}

/** Drops every record made on a branch this tab is no longer on.
 *
 * Branch and scope both decide which file a save writes, so both are in the
 * key. They differ in whether a record under one can be got back to. A scope
 * moves without a reload -- the shell mounts the page afresh instead -- so a
 * record in another group is still reachable. A branch is not: the chip is
 * seeded once, and every way of moving the branch in view reloads, which takes
 * the record with it, but a first write starting a branch, which carries what
 * was held on `main` along (`carry`). A record that cannot be reached must not
 * be counted or listed, so it is not kept.
 */
function prune(): void {
  changing(() => {
    const branch = currentBranch();
    for (const [key, record] of records) {
      if (record.branch !== branch) records.delete(key);
    }
  });
}

/** Moves what was held on `main` to `to`, a branch a first write just started from it. The branch forks from what
 * those edits were read against, so each baseline and commit still holds; a `main` that moved since is refused on
 * save, as it would have been there. */
export function carry(to: string): void {
  changing(() => {
    for (const [key, record] of [...records]) {
      if (record.branch !== MAIN_BRANCH) continue;
      records.delete(key);
      records.set(keyOf(to, record.scope, record.file), { ...record, branch: to });
    }
  });
}

/** Holds `file`'s unsaved work, replacing whatever was held for it.
 *
 * An edit equal to its baseline is not unsaved work, so it drops the record
 * instead of keeping one that would count toward the leave warning and list a
 * row with nothing in it. */
export function hold(
  file: string,
  edit: {
    value: string;
    signature?: string;
    baseline: string;
    readAtCommit: string | null;
    label?: string;
  },
): void {
  prune();
  const [branch, scope] = [currentBranch(), currentScope()];
  const key = keyOf(branch, scope, file);
  changing(() => {
    if ((edit.signature ?? edit.value) === edit.baseline) {
      records.delete(key);
      return;
    }
    records.set(key, { file, branch, scope, ...edit });
  });
}

/** What is held for `file` in the branch and group in hand, if anything. */
export function heldFor(file: string): Held | undefined {
  prune();
  return records.get(keyOf(currentBranch(), currentScope(), file));
}

/** Forgets `file`'s record: its save landed, or the reader discarded it. */
export function drop(file: string): void {
  changing(() => records.delete(keyOf(currentBranch(), currentScope(), file)));
}

/** Forgets every record in group `scope`, which is gone: nothing could save them. */
export function dropScope(scope: string): void {
  changing(() => {
    for (const [key, record] of records) if (record.scope === scope) records.delete(key);
  });
}

/** Forgets every record, on every branch and in every group.
 *
 * The session ended. Nothing held is worth carrying into the next one: it is
 * one person's unsaved work, and the next person to sign in on this machine
 * would be handed it as though it were the file. */
export function dropAll(): void {
  changing(() => records.clear());
}

/** What to do about `file`'s record now that the page's own read has landed.
 *
 * `fresh` is what the file says, serialized the way that page serializes its
 * own baseline -- its own normalization, not the raw read, or a page that
 * rewrites what it loads reports a move that never happened. */
export function verdictFor(file: string, fresh: string): Verdict {
  const record = heldFor(file);
  if (!record) return { kind: 'none' };
  // The edit is already what the file says. Whatever the baseline says about
  // where it came from, there is nothing left to put back.
  if ((record.signature ?? record.value) === fresh) {
    drop(file);
    return { kind: 'none' };
  }
  if (record.baseline === fresh) return { kind: 'restore', held: record };
  return { kind: 'moved', held: record, fresh };
}

/** What to call `record`'s file to a reader: its label, or for a pipeline
 * held without one, the word alone rather than its id. */
export function heldLabel(record: Held): string {
  if (record.label) return record.label;
  return record.file.startsWith('pipelines/') ? 'pipeline' : record.file;
}

/** Whether anything is held, which is what the leave warning asks. */
export function anyHeld(): boolean {
  prune();
  return records.size > 0;
}

/** Everything held on the branch in hand, for the shell's own list of it. */
export function listHeld(): Held[] {
  prune();
  return [...records.values()];
}

// The exits memory cannot survive: the document itself going away. Registered
// once, for everything held, rather than by each page for its own -- held work
// outlives the component that made it, so a page's own handler would be gone
// exactly when it is needed. `confirmContextSwitch` asks its question by
// dispatching one of these and reading `defaultPrevented`, so this is also what
// answers for a branch switch, a scope switch and signing out.
//
// Guarded because this module is imported by tests that have no window, the
// same way `api.ts` guards its own storage reads.
if (typeof window !== 'undefined') {
  window.addEventListener('beforeunload', (e: BeforeUnloadEvent) => {
    if (!anyHeld()) return;
    // The wording is the browser's; every one shows its own fixed sentence and
    // ignores anything set here.
    e.preventDefault();
    e.returnValue = '';
  });
  // Every way a session ends reaches `signedOut()`, and each raises this: the
  // menu item, any call answered 401, the idle report answered 401, and a
  // password change. None of them reloads, so without this the records outlive
  // the person who made them.
  window.addEventListener(SESSION_EVENT, () => dropAll());
  // A first write starts a branch without a reload, so what was typed on `main` beside it is still in reach.
  window.addEventListener(BRANCH_STARTED_EVENT, (e) =>
    carry((e as CustomEvent<{ to: string }>).detail.to),
  );
}
