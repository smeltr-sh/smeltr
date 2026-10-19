// A change's comment threads, folded from its timeline: the comment that
// starts each one, its replies, and whether it is resolved.

import type { CommentAnchor, ConfigSection, TimelineEvent } from './api';
import type { ObjectChange, StepChange } from './objectChanges';

export interface Thread {
  /** The id of the comment that started it. */
  id: string;
  root: TimelineEvent;
  /** Oldest first. */
  replies: TimelineEvent[];
  resolved: boolean;
  /** The latest resolve or reopen, which decides `resolved`. */
  settledBy?: TimelineEvent;
  outdated: boolean;
  anchor?: CommentAnchor;
}

/** Every thread, by the id of the comment that starts it. A comment with an
 * id and no `replyTo` starts one; a reply, resolve or reopen naming no known
 * thread is left out here and shown on its own by `displayList`. */
export function threads(events: TimelineEvent[]): Map<string, Thread> {
  const out = new Map<string, Thread>();
  for (const e of events) {
    if (e.kind === 'comment' && e.id && !e.replyTo) {
      out.set(e.id, {
        id: e.id,
        root: e,
        replies: [],
        resolved: false,
        outdated: e.outdated === true,
        anchor: e.anchor,
      });
    }
  }
  for (const e of events) {
    const thread = e.replyTo ? out.get(e.replyTo) : undefined;
    if (!thread) continue;
    if (e.kind === 'comment') {
      thread.replies.push(e);
    } else if (e.kind === 'resolved' || e.kind === 'unresolved') {
      thread.resolved = e.kind === 'resolved';
      thread.settledBy = e;
    }
  }
  return out;
}

/** What the conversation shows, in timeline order: a thread in the place of
 * the comment that started it, with its replies and resolutions folded into
 * it, and every other event as itself. */
export type DisplayItem =
  { kind: 'event'; event: TimelineEvent } | { kind: 'thread'; thread: Thread };

export function displayList(events: TimelineEvent[], all: Map<string, Thread>): DisplayItem[] {
  const out: DisplayItem[] = [];
  for (const e of events) {
    const thread = e.id && !e.replyTo ? all.get(e.id) : undefined;
    if (thread && thread.root === e) {
      out.push({ kind: 'thread', thread });
    } else if (!(e.replyTo && all.has(e.replyTo))) {
      out.push({ kind: 'event', event: e });
    }
  }
  return out;
}

/** The threads on one object: on the object itself when `step` is absent,
 * on that step of it otherwise. */
export function threadsOnObject(
  list: Iterable<Thread>,
  scope: string,
  section: ConfigSection,
  objectId: string,
  step?: string,
): Thread[] {
  return [...list].filter(
    (t) =>
      t.anchor?.kind === 'object' &&
      t.anchor.scope === scope &&
      t.anchor.section === section &&
      t.anchor.objectId === objectId &&
      t.anchor.step === step,
  );
}

/** The threads on line `line` of `path`, or on any line of it when `line` is absent. */
export function threadsOnLine(list: Iterable<Thread>, path: string, line?: number): Thread[] {
  return [...list].filter(
    (t) =>
      t.anchor?.kind === 'line' &&
      t.anchor.path === path &&
      (line === undefined || t.anchor.line === line),
  );
}

/** A step anywhere in `steps`, its branches included. */
export function findStep(steps: StepChange[], id: string): StepChange | undefined {
  for (const s of steps) {
    if (s.id === id) return s;
    const inner = findStep(s.then ?? [], id) ?? findStep(s.else ?? [], id);
    if (inner) return inner;
  }
  return undefined;
}

function objectOf(objects: ObjectChange[], anchor: CommentAnchor & { kind: 'object' }) {
  return objects.find(
    (c) => c.scope === anchor.scope && c.section === anchor.section && c.id === anchor.objectId,
  );
}

/** The threads on an object or step that `objects` does not show, so a
 * view of them can still list them. */
export function elsewhere(list: Iterable<Thread>, objects: ObjectChange[]): Thread[] {
  return [...list].filter((t) => {
    if (t.anchor?.kind !== 'object') return false;
    const object = objectOf(objects, t.anchor);
    if (!object) return true;
    return t.anchor.step !== undefined && !findStep(object.steps ?? [], t.anchor.step);
  });
}

/** An anchor said for a reader: what it names, where in it, and the line's
 * own text when the anchor is a line the diff shows. */
export interface AnchorLabel {
  title: string;
  detail?: string;
  code?: string;
}

function position(n: number): string {
  return String(n).padStart(2, '0');
}

/** `objects` names an object by its label; an object it does not hold is
 * named by its id. `lines` is each file's new-side lines by number. */
export function anchorLabel(
  anchor: CommentAnchor,
  objects: ObjectChange[],
  lines?: Map<string, Map<number, string>>,
): AnchorLabel {
  if (anchor.kind === 'line') {
    const code = lines?.get(anchor.path)?.get(anchor.line);
    return { title: anchor.path, detail: `line ${anchor.line}`, code };
  }
  const object = objectOf(objects, anchor);
  const title = object?.label ?? anchor.objectId;
  if (anchor.step === undefined) return { title };
  const step = object && findStep(object.steps ?? [], anchor.step);
  if (!step) return { title, detail: `step ${anchor.step}` };
  const at = step.position === undefined ? 'removed step' : `step ${position(step.position)}`;
  return { title, detail: step.summary ? `${at} · ${step.summary}` : `${at} · ${step.type}` };
}
