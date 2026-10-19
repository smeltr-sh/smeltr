import { describe, expect, it } from 'vitest';
import type { CommentAnchor, TimelineEvent } from './api';
import type { ObjectChange } from './objectChanges';
import {
  anchorLabel,
  displayList,
  elsewhere,
  threads,
  threadsOnLine,
  threadsOnObject,
} from './threads';

const onStep: CommentAnchor = {
  kind: 'object',
  scope: 'network',
  section: 'pipelines',
  objectId: 'x7f3',
  step: 's4',
};
const onLine: CommentAnchor = { kind: 'line', path: 'network/pipelines.json', line: 28 };

let t = 0;
function ev(kind: TimelineEvent['kind'], extra: Partial<TimelineEvent> = {}): TimelineEvent {
  return { kind, time: ++t, author: 'Marcus Webb', ...extra };
}

const pipeline: ObjectChange = {
  scope: 'network',
  section: 'pipelines',
  noun: 'pipeline',
  id: 'x7f3',
  label: 'panos-traffic',
  status: 'changed',
  fields: [],
  steps: [
    { status: 'same', id: 's2', type: 'csv', summary: 'message', position: 2 },
    {
      status: 'added',
      id: 's4',
      type: 'code',
      summary: 'if risk >= 5 { event.risk = "high" }',
      position: 4,
    },
  ],
};

describe('threads', () => {
  const root = ev('comment', { id: 'c1', anchor: onStep, outdated: true, text: 'Rating 4' });
  const reply = ev('comment', { id: 'c2', replyTo: 'c1', author: 'Priya Raman' });
  const events = [
    ev('opened'),
    root,
    reply,
    ev('resolved', { id: 'r1', replyTo: 'c1' }),
    ev('unresolved', { id: 'r2', replyTo: 'c1' }),
    ev('resolved', { id: 'r3', replyTo: 'c1', author: 'Priya Raman' }),
    ev('comment', { id: 'c3', text: 'plain' }),
  ];
  const all = threads(events);

  it('folds replies and the latest resolution into the thread they name', () => {
    const thread = all.get('c1');
    expect(thread?.replies).toEqual([reply]);
    expect(thread?.resolved).toBe(true);
    expect(thread?.settledBy?.author).toBe('Priya Raman');
    expect(thread?.outdated).toBe(true);
    expect(thread?.anchor).toEqual(onStep);
  });

  it('starts a thread at every comment that replies to none', () => {
    expect([...all.keys()]).toEqual(['c1', 'c3']);
  });

  it('shows a thread in its root comment’s place and folds the rest into it', () => {
    const items = displayList(events, all);
    expect(
      items.map((i) => (i.kind === 'thread' ? `thread ${i.thread.id}` : i.event.kind)),
    ).toEqual(['opened', 'thread c1', 'thread c3']);
  });

  it('keeps a reply to no known thread as an event of its own', () => {
    const orphan = ev('comment', { id: 'c9', replyTo: 'gone' });
    const items = displayList([orphan], threads([orphan]));
    expect(items).toEqual([{ kind: 'event', event: orphan }]);
  });
});

describe('finding threads', () => {
  const all = threads([
    ev('comment', { id: 'a', anchor: onStep }),
    ev('comment', { id: 'b', anchor: { ...onStep, step: undefined } }),
    ev('comment', { id: 'c', anchor: onLine }),
    ev('comment', { id: 'd', anchor: { ...onStep, objectId: 'gone' } }),
    ev('comment', { id: 'e', anchor: { ...onStep, step: 'gone' } }),
  ]).values();
  const list = [...all];

  it('tells a step’s threads from its object’s own', () => {
    expect(threadsOnObject(list, 'network', 'pipelines', 'x7f3', 's4').map((t) => t.id)).toEqual([
      'a',
    ]);
    expect(threadsOnObject(list, 'network', 'pipelines', 'x7f3').map((t) => t.id)).toEqual(['b']);
  });

  it('finds a line’s threads, and a file’s', () => {
    expect(threadsOnLine(list, onLine.path, 28).map((t) => t.id)).toEqual(['c']);
    expect(threadsOnLine(list, onLine.path, 27)).toEqual([]);
    expect(threadsOnLine(list, onLine.path).map((t) => t.id)).toEqual(['c']);
  });

  it('lists threads whose object or step the change no longer shows', () => {
    expect(elsewhere(list, [pipeline]).map((t) => t.id)).toEqual(['d', 'e']);
  });
});

describe('anchorLabel', () => {
  it('names an object by its label and a step by its place and summary', () => {
    expect(anchorLabel(onStep, [pipeline])).toEqual({
      title: 'panos-traffic',
      detail: 'step 04 · if risk >= 5 { event.risk = "high" }',
    });
  });

  it('falls back to the id of an object it cannot find', () => {
    expect(anchorLabel({ ...onStep, step: undefined }, [])).toEqual({ title: 'x7f3' });
  });

  it('gives a line its text when the diff shows it', () => {
    const lines = new Map([[onLine.path, new Map([[28, '"code": "x"']])]]);
    expect(anchorLabel(onLine, [], lines)).toEqual({
      title: onLine.path,
      detail: 'line 28',
      code: '"code": "x"',
    });
  });
});
