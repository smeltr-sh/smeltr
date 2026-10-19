import { describe, expect, it } from 'vitest';
import type { TimelineEvent } from './api';
import { conversation, eventVerb, initials } from './conversation';

describe('initials', () => {
  it('takes the first and last word', () => {
    expect(initials('Priya Raman')).toBe('PR');
    expect(initials('Anna Maria de la Cruz')).toBe('AC');
  });

  it('takes one letter from a single word', () => {
    expect(initials('jo')).toBe('J');
  });

  it('keeps a character outside the BMP whole', () => {
    expect(initials('😀 Smith')).toBe('😀S');
  });

  it('marks a missing name rather than drawing an empty circle', () => {
    expect(initials('  ')).toBe('?');
  });
});

describe('conversation', () => {
  const ev = (kind: TimelineEvent['kind'], time: number): TimelineEvent => ({
    kind,
    time,
    author: 'Priya Raman',
  });

  it('heads with the opening and keeps the rest in order', () => {
    const events = [ev('opened', 1), ev('saved', 2), ev('comment', 3)];
    const c = conversation(events);
    expect(c.opened).toBe(events[0]);
    expect(c.rest.map((e) => e.time)).toEqual([2, 3]);
  });

  it('keeps every event when none opened the change', () => {
    const events = [ev('saved', 2), ev('comment', 3)];
    expect(conversation(events)).toEqual({ rest: events });
  });

  it('heads with the first opening only', () => {
    const events = [ev('opened', 1), ev('closed', 2), ev('opened', 3)];
    expect(conversation(events).rest.map((e) => e.time)).toEqual([2, 3]);
  });
});

describe('eventVerb', () => {
  it('says what a one-line event did', () => {
    expect(eventVerb('saved')).toBe('saved');
    expect(eventVerb('closed')).toBe('closed this change');
    expect(eventVerb('comment')).toBe('');
  });
});
