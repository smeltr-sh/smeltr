// A change's conversation: its timeline, read for display.

import type { TimelineEvent } from './api';

/** Up to two letters naming a person in a circle: the first letters of the
 * first and last word of the name. */
export function initials(name: string): string {
  const words = name.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return '?';
  // By code point, so a name starting outside the BMP keeps its whole first character.
  const first = [...words[0]][0];
  const last = words.length > 1 ? [...words[words.length - 1]][0] : '';
  return (first + last).toUpperCase();
}

export interface Conversation {
  /** The event that opened the change, shown as the card that heads the
   * conversation. Absent when the timeline holds none. */
  opened?: TimelineEvent;
  /** Every other event, in the order the timeline gave them. */
  rest: TimelineEvent[];
}

/** The first `opened` event heads the conversation; the rest follow it. */
export function conversation(events: TimelineEvent[]): Conversation {
  const at = events.findIndex((e) => e.kind === 'opened');
  if (at < 0) return { rest: events };
  return { opened: events[at], rest: events.filter((_, i) => i !== at) };
}

/** What a one-line event says its author did. A comment, a review verdict and
 * the opening are cards, not lines, and have none. A resolve or reopen is
 * told by its thread's card, and is a line only when its thread is not shown. */
export function eventVerb(kind: TimelineEvent['kind']): string {
  switch (kind) {
    case 'renamed':
      return 'renamed this change to';
    case 'reopened':
      return 'reopened this change';
    case 'closed':
      return 'closed this change';
    case 'saved':
      return 'saved';
    case 'resolved':
      return 'resolved a thread';
    case 'unresolved':
      return 'reopened a thread';
    case 'opened':
    case 'comment':
    case 'approved':
    case 'changes-requested':
      return '';
    default:
      return kind satisfies never;
  }
}
